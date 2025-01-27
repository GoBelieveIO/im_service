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
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"

	"github.com/GoBelieveIO/im_service/server"
)

type Listener struct {
	server_summary *server.ServerSummary
	low_memory     *int32
	server         *server.Server
}

func handle_client(conn server.Conn, listener *Listener) {
	low := atomic.LoadInt32(listener.low_memory)
	if low != 0 {
		log.Warning("low memory, drop new connection")
		return
	}
	client := server.NewClient(conn, listener.server_summary, listener.server)
	client.Run()
}

func handle_ws_client(conn *websocket.Conn, listener *Listener) {
	handle_client(&server.WSConn{Conn: conn}, listener)
}

func handle_tcp_client(conn net.Conn, listener *Listener) {
	handle_client(&server.NetConn{Conn: conn}, listener)
}

func ListenClient(port int, listener *Listener) {
	listen_addr := fmt.Sprintf("0.0.0.0:%d", port)
	listen, err := net.Listen("tcp", listen_addr)
	if err != nil {
		log.Errorf("listen err:%s", err)
		return
	}
	tcp_listener, ok := listen.(*net.TCPListener)
	if !ok {
		log.Error("listen err")
		return
	}

	for {
		conn, err := tcp_listener.AcceptTCP()
		if err != nil {
			log.Errorf("accept err:%s", err)
			return
		}
		log.Infoln("handle new connection, remote address:", conn.RemoteAddr())
		handle_tcp_client(conn, listener)
	}
}

func ListenSSL(port int, cert_file, key_file string, listener *Listener) {
	cert, err := tls.LoadX509KeyPair(cert_file, key_file)
	if err != nil {
		log.Fatal("load cert err:", err)
		return
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	addr := fmt.Sprintf(":%d", port)
	listen, err := tls.Listen("tcp", addr, config)
	if err != nil {
		log.Fatal("ssl listen err:", err)
	}

	log.Infof("ssl listen...")
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal("ssl accept err:", err)
		}
		log.Infoln("handle new ssl connection,  remote address:", conn.RemoteAddr())
		handle_tcp_client(conn, listener)
	}
}
