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

package router

import (
	"net"

	log "github.com/sirupsen/logrus"

	"github.com/GoBelieveIO/im_service/protocol"
)

type ClientObserver interface {
	onClientMessage(client *Client, msg *protocol.Message)
	onClientClose(client *Client)
}

type Client struct {
	wt chan *protocol.Message

	conn      *net.TCPConn
	app_route *AppRoute

	observer ClientObserver
}

func NewClient(conn *net.TCPConn, observer ClientObserver) *Client {
	client := new(Client)
	client.conn = conn
	client.wt = make(chan *protocol.Message, 10)
	client.app_route = NewAppRoute()
	client.observer = observer
	return client
}

func (client *Client) ContainAppUserID(id *RouteUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainUserID(id.uid)
}

func (client *Client) IsAppUserOnline(id *RouteUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.IsUserOnline(id.uid)
}

func (client *Client) ContainAppRoomID(id *RouteRoomID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainRoomID(id.room_id)
}

func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			client.observer.onClientClose(client)
			client.wt <- nil
			break
		}
		client.observer.onClientMessage(client, msg)
	}
}

func (client *Client) Write() {
	seq := 0
	for {
		msg := <-client.wt
		if msg == nil {
			client.close()
			log.Infof("client socket closed")
			break
		}
		seq++
		msg.Seq = seq
		client.send(msg)
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}

func (client *Client) read() *protocol.Message {
	return protocol.ReceiveMessage(client.conn)
}

func (client *Client) send(msg *protocol.Message) {
	protocol.SendMessage(client.conn, msg)
}

func (client *Client) close() {
	client.conn.Close()
}
