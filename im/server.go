package main

import (
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type MessageHandler func(*Client, *Message)

type Server struct {
	handlers map[int]MessageHandler
}

func NewServer() *Server {
	s := &Server{}

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
	return s
}

func (server *Server) onClientMessage(client *Client, msg *Message) {
	if h, ok := server.handlers[msg.cmd]; ok {
		h(client, msg)
	} else {
		log.Warning("Can't find message handler, cmd:", msg.cmd)
	}
}

func (server *Server) onClientClose(client *Client) {
	atomic.AddInt64(&client.server_summary.nconnections, -1)
	if client.uid > 0 {
		atomic.AddInt64(&client.server_summary.nclients, -1)
	}
	atomic.StoreInt32(&client.closed, 1)

	client.RemoveClient()

	//quit when write goroutine received
	client.wt <- nil

	client.RoomClient.Logout()
	client.PeerClient.Logout()
}

func (server *Server) HandleAuthToken(client *Client, m *Message) {
	login, version := m.body.(*AuthenticationToken), m.version

	if client.uid > 0 {
		log.Info("repeat login")
		return
	}

	var err error
	appid, uid, fb, on, err := client.AuthToken(login.token)
	if err != nil {
		log.Infof("auth token:%s err:%s", login.token, err)
		msg := &Message{cmd: MSG_AUTH_STATUS, version: version, body: &AuthenticationStatus{1}}
		client.EnqueueMessage(msg)
		return
	}
	if uid == 0 {
		log.Info("auth token uid==0")
		msg := &Message{cmd: MSG_AUTH_STATUS, version: version, body: &AuthenticationStatus{1}}
		client.EnqueueMessage(msg)
		return
	}

	if login.platform_id != PLATFORM_WEB && len(login.device_id) > 0 {
		client.device_ID, err = GetDeviceID(client.redis_pool, login.device_id, int(login.platform_id))
		if err != nil {
			log.Info("auth token uid==0")
			msg := &Message{cmd: MSG_AUTH_STATUS, version: version, body: &AuthenticationStatus{1}}
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

	msg := &Message{cmd: MSG_AUTH_STATUS, version: version, body: &AuthenticationStatus{0}}
	client.EnqueueMessage(msg)

	client.AddClient()

	client.PeerClient.Login()
}

func (server *Server) HandlePing(client *Client, _ *Message) {
	m := &Message{cmd: MSG_PONG}
	client.EnqueueMessage(m)
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
}

func (server *Server) HandleACK(client *Client, msg *Message) {
	ack := msg.body.(*MessageACK)
	log.Info("ack:", ack.seq)
}
