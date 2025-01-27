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
	"sync"

	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
	srv "github.com/GoBelieveIO/im_service/server"
)

type Server struct {
	clients       ClientSet
	mutex         sync.Mutex
	push_service  *srv.PushService
	push_disabled bool
}

func NewServer(redis_pool *redis.Pool, push_disabled bool) *Server {
	s := &Server{}
	s.clients = NewClientSet()
	s.push_service = srv.NewPushService(redis_pool)
	s.push_disabled = push_disabled
	return s
}

func (server *Server) RunPushService() {
	server.push_service.Run()
}

func (server *Server) onClientClose(client *Client) {
	server.RemoveClient(client)
}

func (server *Server) onClientMessage(client *Client, msg *Message) {
	log.Info("msg cmd:", Command(msg.Cmd))
	switch msg.Cmd {
	case MSG_SUBSCRIBE:
		server.HandleSubscribe(client, msg.Body.(*SubscribeMessage))
	case MSG_UNSUBSCRIBE:
		server.HandleUnsubscribe(client, msg.Body.(*RouteUserID))
	case MSG_PUBLISH:
		server.HandlePublish(client, msg.Body.(*RouteMessage))
	case MSG_PUBLISH_GROUP:
		server.HandlePublishGroup(client, msg.Body.(*RouteMessage))
	case MSG_PUSH:
		server.HandlePush(client, msg.Body.(*BatchPushMessage))
	case MSG_SUBSCRIBE_ROOM:
		server.HandleSubscribeRoom(client, msg.Body.(*RouteRoomID))
	case MSG_UNSUBSCRIBE_ROOM:
		server.HandleUnsubscribeRoom(client, msg.Body.(*RouteRoomID))
	case MSG_PUBLISH_ROOM:
		server.HandlePublishRoom(client, msg.Body.(*RouteMessage))
	default:
		log.Warning("unknown message cmd:", msg.Cmd)
	}
}

func (server *Server) HandleSubscribe(client *Client, id *SubscribeMessage) {
	log.Infof("subscribe appid:%d uid:%d online:%d", id.appid, id.uid, id.online)
	route := client.app_route.FindOrAddRoute(id.appid)
	on := id.online != 0
	route.AddUserID(id.uid, on)
}

func (server *Server) HandleUnsubscribe(client *Client, id *RouteUserID) {
	log.Infof("unsubscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveUserID(id.uid)
}

func (server *Server) HandlePublishGroup(client *Client, amsg *RouteMessage) {
	log.Infof("publish message appid:%d group id:%d msgid:%d", amsg.appid, amsg.receiver, amsg.msgid)

	//群发给所有接入服务器
	s := server.GetClientSet()

	msg := &Message{Cmd: MSG_PUBLISH_GROUP, Body: amsg}
	for c := range s {
		//不发送给自身
		if client == c {
			continue
		}
		c.wt <- msg
	}
}

func (server *Server) HandlePush(client *Client, pmsg *BatchPushMessage) {
	if server.push_disabled {
		return
	}

	log.Infof("push message appid:%d cmd:%s", pmsg.appid, Command(pmsg.msg.Cmd))

	off_members := make([]int64, 0)

	for _, uid := range pmsg.receivers {
		if !server.IsUserOnline(pmsg.appid, uid) {
			off_members = append(off_members, uid)
		}
	}

	cmd := pmsg.msg.Cmd
	if len(off_members) > 0 {
		if cmd == MSG_GROUP_IM {
			server.push_service.PublishGroupMessage(pmsg.appid, off_members, pmsg.msg.Body.(*srv.IMMessage))
		} else if cmd == MSG_IM {
			//assert len(off_members) == 1
			server.push_service.PublishPeerMessage(pmsg.appid, pmsg.msg.Body.(*srv.IMMessage))
		} else if cmd == MSG_SYSTEM {
			//assert len(off_members) == 1
			receiver := off_members[0]
			sys := pmsg.msg.Body.(*srv.SystemMessage)
			server.push_service.PublishSystemMessage(pmsg.appid, receiver, sys)
		} else if cmd == MSG_CUSTOMER_V2 {
			server.push_service.PublishCustomerMessageV2(pmsg.appid, pmsg.msg.Body.(*srv.CustomerMessageV2))
		}
	}
}

func (server *Server) HandlePublish(client *Client, amsg *RouteMessage) {
	log.Infof("publish message appid:%d uid:%d msgid:%d", amsg.appid, amsg.receiver, amsg.msgid)

	receiver := &RouteUserID{appid: amsg.appid, uid: amsg.receiver}
	s := server.FindClientSet(receiver)
	msg := &Message{Cmd: MSG_PUBLISH, Body: amsg}
	for c := range s {
		//不发送给自身
		if client == c {
			continue
		}
		c.wt <- msg
	}
}

func (server *Server) HandleSubscribeRoom(client *Client, id *RouteRoomID) {
	log.Infof("subscribe appid:%d room id:%d", id.appid, id.room_id)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.AddRoomID(id.room_id)
}

func (server *Server) HandleUnsubscribeRoom(client *Client, id *RouteRoomID) {
	log.Infof("unsubscribe appid:%d room id:%d", id.appid, id.room_id)
	route := client.app_route.FindOrAddRoute(id.appid)
	route.RemoveRoomID(id.room_id)
}

func (server *Server) HandlePublishRoom(client *Client, amsg *RouteMessage) {
	log.Infof("publish room message appid:%d room id:%d", amsg.appid, amsg.receiver)
	receiver := &RouteRoomID{appid: amsg.appid, room_id: amsg.receiver}
	s := server.FindRoomClientSet(receiver)

	msg := &Message{Cmd: MSG_PUBLISH_ROOM, Body: amsg}
	for c := range s {
		//不发送给自身
		if client == c {
			continue
		}
		log.Info("publish room message")
		c.wt <- msg
	}
}

func (server *Server) AddClient(client *Client) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	server.clients.Add(client)
}

func (server *Server) RemoveClient(client *Client) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	server.clients.Remove(client)
}

// clone clients
func (server *Server) GetClientSet() ClientSet {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	s := NewClientSet()

	for c := range server.clients {
		s.Add(c)
	}
	return s
}

func (server *Server) FindClientSet(id *RouteUserID) ClientSet {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	s := NewClientSet()

	for c := range server.clients {
		if c.ContainAppUserID(id) {
			s.Add(c)
		}
	}
	return s
}

func (server *Server) FindRoomClientSet(id *RouteRoomID) ClientSet {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	s := NewClientSet()

	for c := range server.clients {
		if c.ContainAppRoomID(id) {
			s.Add(c)
		}
	}
	return s
}

func (server *Server) IsUserOnline(appid, uid int64) bool {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	id := &RouteUserID{appid: appid, uid: uid}

	for c := range server.clients {
		if c.IsAppUserOnline(id) {
			return true
		}
	}
	return false
}
