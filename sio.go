package main

import (
	"encoding/json"
	"github.com/bitly/go-simplejson"
	log "github.com/golang/glog"
	"github.com/googollee/go-engine.io"
	"io/ioutil"
	"net/http"
)

type SIOServer struct {
	server *engineio.Server
}

func (s *SIOServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Info(req.Header.Get("Origin"))
	if req.Header.Get("Origin") != "" {
		w.Header().Set("Access-Control-Allow-Origin", req.Header.Get("Origin"))
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", `Origin, No-Cache, X-Requested-With, If-Modified-Since, Pragma,
		Last-Modified, Cache-Control, Expires, Content-Type`)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	} else {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	}
	s.server.ServeHTTP(w, req)
}

func StartSocketIO(socket_io_address string) {
	server, err := engineio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			conn, err := server.Accept()
			if err != nil {
				log.Info("accept connect fail")
			}
			handlerEngineIOClient(conn)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/engine.io/", &SIOServer{server})
	log.Infof("EngineIO Serving at %s...", socket_io_address)
	HTTPService(socket_io_address, mux)

}

func handlerEngineIOClient(conn engineio.Conn) {
	client := NewClient(conn)
	client.Run()
}

func SendEngineIOMessage(conn engineio.Conn, msg *Message) {

	w, err := conn.NextWriter(engineio.MessageText)
	if err != nil {
		log.Info("get next writer fail")
		return
	}

	d, err := ToJson(msg)
	if err != nil {
		log.Info("json encode error")
		return
	}

	n, err := w.Write(d)
	if err != nil || n != len(d) {
		log.Info("engine io write error")
		return
	}

	w.Close()
}

func ToJson(msg *Message) ([]byte, error) {
	data := msg.ToMap()
	log.Info(data)
	return json.Marshal(data)
}

func ReadEngineIOMessage(conn engineio.Conn) *Message {
	t, r, err := conn.NextReader()
	if err != nil {
		return nil
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil
	}
	r.Close()
	if t == engineio.MessageText {
		log.Info(string(b))
		return ReadMessage(b)
	} else {
		log.Info("receive binary data")
		return nil
	}

}

func ReadMessage(b []byte) *Message {
	input, err := simplejson.NewJson(b)

	if err != nil {
		log.Info("json decode fail")
		return nil
	}

	cmd, err := input.Get("cmd").Int()
	if err != nil {
		log.Info("json decode cmd fail")
		return nil
	}
	seq, err := input.Get("seq").Int()
	if err != nil {
		log.Info("json decode seq fail")
		return nil
	}

	msg := new(Message)
	msg.cmd = cmd
	msg.seq = seq

	if msg.FromJson(input) {
		return msg
	}
	return nil
}
