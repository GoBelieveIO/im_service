/**
 * Copyright (c) 2014-2015, GoBelieve     
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import (
	log "github.com/golang/glog"
	"github.com/gorilla/websocket"
	"net/http"
	"bytes"
)


func ReadBinaryMesage(b []byte) *Message {
	reader := bytes.NewReader(b)
	return ReceiveClientMessage(reader)
}

func CheckOrigin(r *http.Request) bool {
	// allow all connections by default
	return true
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:CheckOrigin,
}

func ServeWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("upgrade err:", err)
		return
	}
	conn.SetReadLimit(64*1024)
	conn.SetPongHandler(func(string) error {
		log.Info("brower websocket pong...")
		return nil
	})
	log.Info("new websocket connection, remote address:", conn.RemoteAddr());
	client := NewClient(conn)
	client.Run()
}


func StartWSSServer(tls_address string, cert_file string, key_file string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ServeWebsocket)

	if tls_address != "" && cert_file != "" && key_file != "" {
		log.Infof("websocket Serving TLS at %s...", tls_address)
		err := http.ListenAndServeTLS(tls_address, cert_file, key_file, mux)
		if err != nil {
			log.Fatalf("listen err:%s", err)
		}
	}
}

func StartWSServer(address string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ServeWebsocket)
	err := http.ListenAndServe(address, mux)
	if err != nil {
		log.Fatalf("listen err:%s", err)
	}
}


func ReadWebsocketMessage(conn *websocket.Conn) *Message {
	messageType, p, err := conn.ReadMessage()
	if err != nil {
		log.Info("read websocket err:", err)
		return nil
	}
	if messageType == websocket.BinaryMessage {
		return ReadBinaryMesage(p)
	} else {
		log.Error("invalid websocket message type:", messageType)
		return nil
	}
}

func SendWebsocketBinaryMessage(conn *websocket.Conn, msg *Message) error {
	w, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		log.Info("get next writer fail")
		return err
	}
	err = SendMessage(w, msg)
	if err != nil {
		return err
	}
	err = w.Close()
	return err
}
