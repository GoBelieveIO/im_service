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
	"github.com/googollee/go-engine.io"
	"io/ioutil"
	"net/http"
	"bytes"
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

func StartSocketIO(address string, tls_address string, 
	cert_file string, key_file string) {
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
	log.Infof("EngineIO Serving at %s...", address)

	if tls_address != "" && cert_file != "" && key_file != "" {
		log.Infof("EngineIO Serving TLS at %s...", tls_address)
		err = http.ListenAndServeTLS(tls_address, cert_file, key_file, mux)
		if err != nil {
			log.Fatalf("listen err:%s", err)
		}
	}
	err = http.ListenAndServe(address, mux)
	if err != nil {
		log.Fatalf("listen err:%s", err)
	}
}

func handlerEngineIOClient(conn engineio.Conn) {
	client := NewClient(conn)
	client.Run()
}

func SendEngineIOBinaryMessage(conn engineio.Conn, msg *Message) {
	w, err := conn.NextWriter(engineio.MessageBinary)
	if err != nil {
		log.Info("get next writer fail")
		return
	}
	log.Info("message version:", msg.version)
	err = SendMessage(w, msg)
	if err != nil {
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
		return nil
	} else {
		return ReadBinaryMesage(b)
	}
}

func ReadBinaryMesage(b []byte) *Message {
	reader := bytes.NewReader(b)
	return ReceiveMessage(reader)
}

