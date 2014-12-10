package main

import (
	"encoding/json"
	"github.com/bitly/go-simplejson"
	log "github.com/golang/glog"
	"github.com/googollee/go-engine.io"
	"io/ioutil"
	"net/http"
	"time"
)

const PING_INTERVAL = 25
const PING_TIMEOUT = 60

func StartSocketIO(socket_io_address string) {
	server, err := engineio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}
	server.SetPingInterval(time.Second * PING_INTERVAL)
	server.SetPingTimeout(time.Second * PING_TIMEOUT)

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
	mux.Handle("/engine.io/", server)
	mux.Handle("/", http.FileServer(http.Dir("./asset")))
	log.Infof("EngineIO Serving at %s...", socket_io_address)
	log.Fatal(http.ListenAndServe(socket_io_address, mux))

}

func handlerEngineIOClient(conn engineio.Conn) {
	client := NewClient(conn)
	client.Run()
}

func SendEngineIOMessage(conn engineio.Conn, msg *Message) {
	data := msg.ToMap()
	log.Info(data)
	w, err := conn.NextWriter(engineio.MessageText)
	if err != nil {
		log.Info("get next writer fail")
		return
	}

	d, err := json.Marshal(data)

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
		return readMessage(b)
	} else {
		log.Info("receive binary data")
		return nil
	}

}

func readMessage(b []byte) *Message {
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

	msg.FromJson(input)
	return msg
}
