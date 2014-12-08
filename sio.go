package main

import (
	log "github.com/golang/glog"
	"net/http"

	"github.com/googollee/go-socket.io"
)

func StartSocketIO(socket_io_address string) {
	server, err := socketio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}
	server.On("connection", func(so socketio.Socket) {
		log.Info("on connection")

		client := NewClient(nil, so)
		go client.Write()
		client.Read()
	})
	server.On("error", func(so socketio.Socket, err error) {
		log.Error("error:", err)
	})
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", server)
	mux.Handle("/", http.FileServer(http.Dir("./asset")))
	log.Infof("SocketIO Serving at %s...", socket_io_address)
	log.Fatal(http.ListenAndServe(socket_io_address, mux))
}

func SendSocketIOMessage(so socketio.Socket, msg *Message) {
	data := msg.ToMap()
	log.Info(data)
	err := so.Emit("chat", data)
	if err != nil {
		log.Info("sock write error")
		return
	}
}
