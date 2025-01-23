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
	"sync/atomic"
	"time"

	"container/list"

	log "github.com/sirupsen/logrus"
)

type ClientObserver interface {
	onClientMessage(*Client, *Message)
	onClientClose(*Client)
}

type Client struct {
	Connection //必须放在结构体首部
	observer   ClientObserver
}

func NewClient(conn Conn, server_summary *ServerSummary, observer ClientObserver) *Client {
	client := new(Client)

	//初始化Connection
	client.conn = conn // conn is net.Conn or engineio.Conn
	client.wt = make(chan *Message, 300)
	//'10'对于用户拥有非常多的超级群，读线程还是有可能会阻塞
	client.pwt = make(chan []*Message, 10)

	client.lwt = make(chan int, 1) //only need 1
	client.messages = list.New()
	client.server_summary = server_summary
	client.observer = observer

	atomic.AddInt64(&server_summary.nconnections, 1)

	return client
}

func (client *Client) Read() {
	for {
		tc := atomic.LoadInt32(&client.tc)
		if tc > 0 {
			log.Infof("quit read goroutine, client:%d write goroutine blocked", client.uid)
			client.observer.onClientClose(client)
			break
		}

		t1 := time.Now().Unix()
		msg := client.read()
		t2 := time.Now().Unix()
		if t2-t1 > 6*60 {
			log.Infof("client:%d socket read timeout:%d %d", client.uid, t1, t2)
		}
		if msg == nil {
			client.observer.onClientClose(client)
			break
		}

		client.observer.onClientMessage(client, msg)
		t3 := time.Now().Unix()
		if t3-t2 > 2 {
			log.Infof("client:%d handle message is too slow:%d %d", client.uid, t2, t3)
		}
	}
}

// 发送等待队列中的消息
func (client *Client) SendMessages() {
	var messages *list.List
	client.mutex.Lock()
	if client.messages.Len() == 0 {
		client.mutex.Unlock()
		return
	}
	messages = client.messages
	client.messages = list.New()
	client.mutex.Unlock()

	e := messages.Front()
	for e != nil {
		msg := e.Value.(*Message)
		if msg.cmd == MSG_RT || msg.cmd == MSG_IM ||
			msg.cmd == MSG_GROUP_IM || msg.cmd == MSG_ROOM_IM {
			atomic.AddInt64(&client.server_summary.out_message_count, 1)
		}

		if msg.meta != nil {
			meta_msg := &Message{cmd: MSG_METADATA, version: client.version, body: msg.meta}
			client.send(meta_msg)
		}
		client.send(msg)
		e = e.Next()
	}
}

func (client *Client) Write() {
	running := true

	//发送在线消息
	for running {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			if msg.cmd == MSG_RT || msg.cmd == MSG_IM ||
				msg.cmd == MSG_GROUP_IM || msg.cmd == MSG_ROOM_IM {
				atomic.AddInt64(&client.server_summary.out_message_count, 1)
			}

			if msg.meta != nil {
				meta_msg := &Message{cmd: MSG_METADATA, version: client.version, body: msg.meta}
				client.send(meta_msg)
			}
			client.send(msg)
		case messages := <-client.pwt:
			for _, msg := range messages {
				if msg.cmd == MSG_RT || msg.cmd == MSG_IM ||
					msg.cmd == MSG_GROUP_IM || msg.cmd == MSG_ROOM_IM {
					atomic.AddInt64(&client.server_summary.out_message_count, 1)
				}

				if msg.meta != nil {
					meta_msg := &Message{cmd: MSG_METADATA, version: client.version, body: msg.meta}
					client.send(meta_msg)
				}
				client.send(msg)
			}
		case <-client.lwt:
			client.SendMessages()
			break
		}
	}

	//等待200ms,避免发送者阻塞
	t := time.After(200 * time.Millisecond)
	running = true
	for running {
		select {
		case <-t:
			running = false
		case <-client.wt:
			log.Warning("msg is dropped")
		}
	}

	log.Info("write goroutine exit")
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}
