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

package server

import (
	"sync/atomic"
	"time"

	"github.com/GoBelieveIO/im_service/storage"
	"github.com/bitly/go-simplejson"
	"github.com/gomodule/redigo/redis"
	"github.com/importcjj/sensitive"

	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
)

type Auth interface {
	LoadUserAccessToken(token string) (int64, int64, error)
}

type MessageHandler func(*Client, *Message)

type Server struct {
	handlers map[int]MessageHandler

	filter         *sensitive.Filter
	redis_pool     *redis.Pool
	app            *App
	app_route      *AppRoute
	server_summary *ServerSummary
	rpc_storage    *RPCStorage

	friend_permission bool //验证好友关系
	enable_blacklist  bool //验证是否在对方的黑名单中
	kefu_appid        int64

	relationship_pool *RelationshipPool
	sync_c            chan *storage.SyncHistory

	group_manager *GroupManager
	group_sync_c  chan *storage.SyncGroupHistory

	auth Auth
}

func NewServer(
	group_manager *GroupManager,
	filter *sensitive.Filter,
	redis_pool *redis.Pool,
	server_summary *ServerSummary,
	relationship_pool *RelationshipPool,
	auth Auth,
	rpc_storage *RPCStorage,
	sync_c chan *storage.SyncHistory,
	group_sync_c chan *storage.SyncGroupHistory,
	app_route *AppRoute,
	app *App,
	enable_blacklist bool,
	friend_permission bool,
	kefu_appid int64) *Server {
	s := &Server{}
	log.Info("tttttttttttttttttttttttt")
	s.handlers = make(map[int]MessageHandler)
	s.handlers[MSG_AUTH_TOKEN] = s.HandleAuthToken
	s.handlers[MSG_PING] = s.HandlePing
	s.handlers[MSG_ACK] = s.HandleACK

	s.handlers[MSG_IM] = s.HandleIMMessage
	s.handlers[MSG_RT] = s.HandleRTMessage
	s.handlers[MSG_UNREAD_COUNT] = s.HandleUnreadCount
	s.handlers[MSG_SYNC] = s.HandleSync
	s.handlers[MSG_SYNC_KEY] = s.HandleSyncKey

	s.handlers[MSG_ENTER_ROOM] = s.HandleEnterRoom
	s.handlers[MSG_LEAVE_ROOM] = s.HandleLeaveRoom
	s.handlers[MSG_ROOM_IM] = s.HandleRoomIM

	s.handlers[MSG_GROUP_IM] = s.HandleGroupIMMessage
	s.handlers[MSG_SYNC_GROUP] = s.HandleGroupSync
	s.handlers[MSG_GROUP_SYNC_KEY] = s.HandleGroupSyncKey

	s.handlers[MSG_CUSTOMER_V2] = s.HandleCustomerMessageV2

	s.group_manager = group_manager
	s.filter = filter
	s.redis_pool = redis_pool
	s.server_summary = server_summary
	s.relationship_pool = relationship_pool
	s.auth = auth
	s.rpc_storage = rpc_storage
	s.sync_c = sync_c
	s.group_sync_c = group_sync_c
	s.app_route = app_route
	s.app = app
	s.enable_blacklist = enable_blacklist
	s.friend_permission = friend_permission
	s.kefu_appid = kefu_appid

	return s
}

func (server *Server) onClientMessage(client *Client, msg *Message) {
	if h, ok := server.handlers[msg.Cmd]; ok {
		h(client, msg)
	} else {
		log.Warning("Can't find message handler, cmd:", msg.Cmd)
	}
}

func (server *Server) onClientClose(client *Client) {
	atomic.AddInt64(&server.server_summary.nconnections, -1)
	if client.uid > 0 {
		atomic.AddInt64(&server.server_summary.nclients, -1)
	}
	atomic.StoreInt32(&client.closed, 1)

	server.RemoveClient(client)

	//quit when write goroutine received
	client.wt <- nil

	server.Logout(client)
}

func (server *Server) RemoveClient(client *Client) {
	if client.uid == 0 {
		return
	}
	route := server.app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return
	}
	_, is_delete := route.RemoveClient(client)

	if is_delete {
		atomic.AddInt64(&server.server_summary.clientset_count, -1)
	}
	if client.room_id > 0 {
		route.RemoveRoomClient(client.room_id, client)
	}
}

func (server *Server) AuthToken(client *Client, token string) (int64, int64, int, bool, error) {
	appid, uid, err := server.auth.LoadUserAccessToken(token)

	if err != nil {
		return 0, 0, 0, false, err
	}

	forbidden, notification_on, err := GetUserPreferences(server.redis_pool, appid, uid)
	if err != nil {
		return 0, 0, 0, false, err
	}

	return appid, uid, forbidden, notification_on, nil
}

func (server *Server) AddClient(client *Client) {
	route := server.app_route.FindOrAddRoute(client.appid)
	is_new := route.AddClient(client)

	atomic.AddInt64(&server.server_summary.nclients, 1)
	if is_new {
		atomic.AddInt64(&server.server_summary.clientset_count, 1)
	}
}

// 发送超级群消息
func (server *Server) SendGroupMessage(client *Client, group *Group, msg *Message) {
	appid := client.appid
	sender := &Sender{appid: client.appid, uid: client.uid, deviceID: client.device_ID}
	server.app.SendGroupMessage(appid, group, msg, sender)
}

func (server *Server) SendMessage(client *Client, uid int64, msg *Message) bool {
	appid := client.appid
	sender := &Sender{appid: client.appid, uid: client.uid, deviceID: client.device_ID}
	server.app.SendMessage(appid, uid, msg, sender)
	return true
}

func (server *Server) SendAppMessage(client *Client, appid int64, uid int64, msg *Message) bool {
	sender := &Sender{appid: client.appid, uid: client.uid, deviceID: client.device_ID}
	server.app.SendMessage(appid, uid, msg, sender)
	return true
}
func (server *Server) SendRoomMessage(client *Client, room_id int64, msg *Message) {
	appid := client.appid
	sender := &Sender{appid: client.appid, uid: client.uid, deviceID: client.device_ID}
	server.app.SendRoomMessage(appid, room_id, msg, sender)
}

func (server *Server) Logout(client *Client) {
	if client.uid > 0 {
		channel := server.app.GetChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid, client.online)

		for _, c := range server.app.group_route_channels {
			if c == channel {
				continue
			}

			c.Unsubscribe(client.appid, client.uid, client.online)
		}
	}

	if client.room_id > 0 {
		channel := server.app.GetRoomChannel(client.room_id)
		channel.UnsubscribeRoom(client.appid, client.room_id)
		route := server.app_route.FindOrAddRoute(client.appid)
		route.RemoveRoomClient(client.room_id, client.Client())
	}
}

func (server *Server) Login(client *Client) {
	channel := server.app.GetChannel(client.uid)

	channel.Subscribe(client.appid, client.uid, client.online)

	for _, c := range server.app.group_route_channels {
		if c == channel {
			continue
		}

		c.Subscribe(client.appid, client.uid, client.online)
	}

	SetUserUnreadCount(server.redis_pool, client.appid, client.uid, 0)
}

func (server *Server) HandleAuthToken(client *Client, m *Message) {
	login, version := m.Body.(*AuthenticationToken), m.Version

	if client.uid > 0 {
		log.Info("repeat login")
		return
	}

	var err error
	appid, uid, fb, on, err := server.AuthToken(client, login.token)
	if err != nil {
		log.Infof("auth token:%s err:%s", login.token, err)
		msg := &Message{Cmd: MSG_AUTH_STATUS, Version: version, Body: &AuthenticationStatus{1}}
		client.EnqueueMessage(msg)
		return
	}
	if uid == 0 {
		log.Info("auth token uid==0")
		msg := &Message{Cmd: MSG_AUTH_STATUS, Version: version, Body: &AuthenticationStatus{1}}
		client.EnqueueMessage(msg)
		return
	}

	if login.platform_id != PLATFORM_WEB && len(login.device_id) > 0 {
		client.device_ID, err = GetDeviceID(server.redis_pool, login.device_id, int(login.platform_id))
		if err != nil {
			log.Info("auth token uid==0")
			msg := &Message{Cmd: MSG_AUTH_STATUS, Version: version, Body: &AuthenticationStatus{1}}
			client.EnqueueMessage(msg)
			return
		}
	}

	is_mobile := login.platform_id == PLATFORM_IOS || login.platform_id == PLATFORM_ANDROID
	online := true
	if on && !is_mobile {
		online = false
	}

	client.appid = appid
	client.uid = uid
	client.forbidden = int32(fb)
	client.notification_on = on
	client.online = online
	client.version = version
	client.device_id = login.device_id
	client.platform_id = login.platform_id
	client.tm = time.Now()
	log.Infof("auth token:%s appid:%d uid:%d device id:%s:%d forbidden:%d notification on:%t online:%t",
		login.token, client.appid, client.uid, client.device_id,
		client.device_ID, client.forbidden, client.notification_on, client.online)

	msg := &Message{Cmd: MSG_AUTH_STATUS, Version: version, Body: &AuthenticationStatus{0}}
	client.EnqueueMessage(msg)

	server.AddClient(client)

	server.Login(client)
}

func (server *Server) HandlePing(client *Client, _ *Message) {
	m := &Message{Cmd: MSG_PONG}
	client.EnqueueMessage(m)
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
}

func (server *Server) HandleACK(client *Client, msg *Message) {
	ack := msg.Body.(*MessageACK)
	log.Info("ack:", ack.seq)
}

// 过滤敏感词
func FilterDirtyWord(filter *sensitive.Filter, msg *IMMessage) {
	if filter == nil {
		log.Info("filter is null")
		return
	}

	obj, err := simplejson.NewJson([]byte(msg.content))
	if err != nil {
		log.Info("filter dirty word, can't decode json")
		return
	}

	text, err := obj.Get("text").String()
	if err != nil {
		log.Info("filter dirty word, can't get text")
		return
	}

	if exist, _ := filter.FindIn(text); exist {
		t := filter.RemoveNoise(text)
		replacedText := filter.Replace(t, '*')

		obj.Set("text", replacedText)
		c, err := obj.Encode()
		if err != nil {
			log.Errorf("json encode err:%s", err)
			return
		}
		msg.content = string(c)
		log.Infof("filter dirty word, replace text %s with %s", text, replacedText)
	}
}

func SyncKeyService(redis_pool *redis.Pool,
	sync_c chan *storage.SyncHistory,
	group_sync_c chan *storage.SyncGroupHistory) {
	for {
		select {
		case s := <-sync_c:
			origin := GetSyncKey(redis_pool, s.AppID, s.Uid)
			if s.LastMsgID > origin {
				log.Infof("save sync key:%d %d %d", s.AppID, s.Uid, s.LastMsgID)
				SaveSyncKey(redis_pool, s.AppID, s.Uid, s.LastMsgID)
			}
		case s := <-group_sync_c:
			origin := GetGroupSyncKey(redis_pool, s.AppID, s.Uid, s.GroupID)
			if s.LastMsgID > origin {
				log.Infof("save group sync key:%d %d %d %d",
					s.AppID, s.Uid, s.GroupID, s.LastMsgID)
				SaveGroupSyncKey(redis_pool, s.AppID, s.Uid, s.GroupID, s.LastMsgID)
			}
		}
	}
}
