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
import "net"
import log "github.com/sirupsen/logrus"


type Push struct {
	queue_name string
	content []byte
}

type Client struct {
	wt     chan *Message
	
	pwt     chan *Push
	
	conn   *net.TCPConn
	app_route *AppRoute
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn
	client.pwt = make(chan *Push, 10000)
	client.wt = make(chan *Message, 10)
	client.app_route = NewAppRoute()
	return client
}

func (client *Client) ContainAppUserID(id *AppUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainUserID(id.uid)
}

func (client *Client) IsAppUserOnline(id *AppUserID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.IsUserOnline(id.uid)
}

func (client *Client) ContainAppRoomID(id *AppRoomID) bool {
	route := client.app_route.FindRoute(id.appid)
	if route == nil {
		return false
	}

	return route.ContainRoomID(id.room_id)
}

func (client *Client) Read() {
	AddClient(client)
	for {
		msg := client.read()
		if msg == nil {
			RemoveClient(client)
			client.pwt <- nil
			client.wt <- nil
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_SUBSCRIBE:
		client.HandleSubscribe(msg.body.(*SubscribeMessage))
	case MSG_UNSUBSCRIBE:
		client.HandleUnsubscribe(msg.body.(*AppUserID))
	case MSG_PUBLISH:
		client.HandlePublish(msg.body.(*AppMessage))
	case MSG_PUBLISH_GROUP:
		client.HandlePublishGroup(msg.body.(*AppMessage))
	case MSG_PUSH:
		client.HandlePush(msg.body.(*BatchPushMessage))
	case MSG_SUBSCRIBE_ROOM:
		client.HandleSubscribeRoom(msg.body.(*AppRoomID))
	case MSG_UNSUBSCRIBE_ROOM:
		client.HandleUnsubscribeRoom(msg.body.(*AppRoomID))
	case MSG_PUBLISH_ROOM:
		client.HandlePublishRoom(msg.body.(*AppMessage))
	default:
		log.Warning("unknown message cmd:", msg.cmd)
	}
}

func (client *Client) HandleSubscribe(id *SubscribeMessage) {
	log.Infof("subscribe appid:%d uid:%d online:%d", id.appid, id.uid, id.online)
	route := client.app_route.FindOrAddRoute(id.appid)
	on := id.online != 0
	route.AddUserID(id.uid, on)
}

func (client *Client) HandleUnsubscribe(id *AppUserID) {
	log.Infof("unsubscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveUserID(id.uid)
}


func (client *Client) HandlePublishGroup(amsg *AppMessage) {
	log.Infof("publish message appid:%d group id:%d msgid:%d cmd:%s", amsg.appid, amsg.receiver, amsg.msgid, Command(amsg.msg.cmd))

	//群发给所有接入服务器
	s := GetClientSet()

	msg := &Message{cmd:MSG_PUBLISH_GROUP, body:amsg}
	for c := range(s) {
		//不发送给自身
		if client == c {
			continue
		}
		c.wt <- msg
	}
}

func (client *Client) HandlePush(pmsg *BatchPushMessage) {
	log.Infof("push message appid:%d cmd:%s", pmsg.appid, Command(pmsg.msg.cmd))

	off_members := make([]int64, 0)	
	
	for _, uid := range(pmsg.receivers) {
		if !IsUserOnline(pmsg.appid, uid) {
			off_members = append(off_members, uid)
		}
	}

	cmd := pmsg.msg.cmd
	if len(off_members) > 0 {
		if cmd == MSG_GROUP_IM {
			client.PublishGroupMessage(pmsg.appid, off_members, pmsg.msg.body.(*IMMessage))
		} else if cmd == MSG_IM {
			//assert len(off_members) == 1
			client.PublishPeerMessage(pmsg.appid, pmsg.msg.body.(*IMMessage))
		} else if cmd == MSG_CUSTOMER || 
			cmd == MSG_CUSTOMER_SUPPORT {
			//assert len(off_members) == 1
			receiver := off_members[0]
			client.PublishCustomerMessage(pmsg.appid, receiver, 
				pmsg.msg.body.(*CustomerMessage), pmsg.msg.cmd)
		} else if cmd == MSG_SYSTEM {
			//assert len(off_members) == 1
			receiver := off_members[0]
			sys := pmsg.msg.body.(*SystemMessage)
			if config.is_push_system {
				client.PublishSystemMessage(pmsg.appid, receiver, sys.notification)
			}
		}
	}
}

func (client *Client) HandlePublish(amsg *AppMessage) {
	log.Infof("publish message appid:%d uid:%d msgid:%d cmd:%s", amsg.appid, amsg.receiver, amsg.msgid, Command(amsg.msg.cmd))

	receiver := &AppUserID{appid:amsg.appid, uid:amsg.receiver}
	s := FindClientSet(receiver)
	msg := &Message{cmd:MSG_PUBLISH, body:amsg}
	for c := range(s) {
		//不发送给自身
		if client == c {
			continue
		}
		c.wt <- msg
	}
}

func (client *Client) HandleSubscribeRoom(id *AppRoomID) {
	log.Infof("subscribe appid:%d room id:%d", id.appid, id.room_id)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.AddRoomID(id.room_id)
}

func (client *Client) HandleUnsubscribeRoom(id *AppRoomID) {
	log.Infof("unsubscribe appid:%d room id:%d", id.appid, id.room_id)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveRoomID(id.room_id)
}

func (client *Client) HandlePublishRoom(amsg *AppMessage) {
	log.Infof("publish room message appid:%d room id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
	receiver := &AppRoomID{appid:amsg.appid, room_id:amsg.receiver}
	s := FindRoomClientSet(receiver)

	msg := &Message{cmd:MSG_PUBLISH_ROOM, body:amsg}
	for c := range(s) {
		//不发送给自身
		if client == c {
			continue
		}
		log.Info("publish room message")
		c.wt <- msg
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
		msg.seq = seq
		client.send(msg)
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
	go client.Push()
}


func (client *Client) read() *Message {
	return ReceiveMessage(client.conn)
}

func (client *Client) send(msg *Message) {
	SendMessage(client.conn, msg)
}

func (client *Client) close() {
	client.conn.Close()
}
