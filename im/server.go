package main

import (
	"sync/atomic"
	"time"

	"crypto/tls"
	"fmt"
	"net"

	"github.com/GoBelieveIO/im_service/storage"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/importcjj/sensitive"

	log "github.com/sirupsen/logrus"
)

type MessageHandler func(*Client, *Message)

type Server struct {
	handlers map[int]MessageHandler

	filter         *sensitive.Filter
	redis_pool     *redis.Pool
	app            *App
	app_route      *AppRoute
	server_summary *ServerSummary
	rpc_storage    *RPCStorage
	config         *Config

	relationship_pool *RelationshipPool
	sync_c            chan *storage.SyncHistory

	group_manager *GroupManager
	group_sync_c  chan *storage.SyncGroupHistory

	auth Auth
}

type Listener struct {
	group_manager     *GroupManager
	redis_pool        *redis.Pool
	filter            *sensitive.Filter
	server_summary    *ServerSummary
	relationship_pool *RelationshipPool
	auth              Auth
	rpc_storage       *RPCStorage
	group_sync_c      chan *storage.SyncGroupHistory
	sync_c            chan *storage.SyncHistory
	low_memory        *int32
	app_route         *AppRoute
	app               *App
	config            *Config
	server            *Server
}

func handle_client(conn Conn, listener *Listener) {
	low := atomic.LoadInt32(listener.low_memory)
	if low != 0 {
		log.Warning("low memory, drop new connection")
		return
	}
	client := NewClient(conn, listener.server_summary, listener.server)
	client.Run()
}

func handle_ws_client(conn *websocket.Conn, listener *Listener) {
	handle_client(&WSConn{Conn: conn}, listener)
}

func handle_tcp_client(conn net.Conn, listener *Listener) {
	handle_client(&NetConn{Conn: conn}, listener)
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
	config *Config) *Server {
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
	s.config = config

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
	login, version := m.body.(*AuthenticationToken), m.version

	if client.uid > 0 {
		log.Info("repeat login")
		return
	}

	var err error
	appid, uid, fb, on, err := server.AuthToken(client, login.token)
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
		client.device_ID, err = GetDeviceID(server.redis_pool, login.device_id, int(login.platform_id))
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

	server.AddClient(client)

	server.Login(client)
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
