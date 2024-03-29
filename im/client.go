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
	"net"
	"sync/atomic"
	"time"

	"container/list"
	"crypto/tls"
	"fmt"

	"github.com/GoBelieveIO/im_service/storage"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	"github.com/importcjj/sensitive"
	log "github.com/sirupsen/logrus"
)

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
}

type Client struct {
	Connection //必须放在结构体首部
	*PeerClient
	*GroupClient
	*RoomClient
	*CustomerClient
	auth Auth
}

func NewClient(conn Conn,
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
	config *Config) *Client {
	client := new(Client)

	//初始化Connection
	client.conn = conn // conn is net.Conn or engineio.Conn
	client.wt = make(chan *Message, 300)
	//'10'对于用户拥有非常多的超级群，读线程还是有可能会阻塞
	client.pwt = make(chan []*Message, 10)

	client.lwt = make(chan int, 1) //only need 1
	client.messages = list.New()
	client.filter = filter
	client.redis_pool = redis_pool
	client.server_summary = server_summary
	client.rpc_storage = rpc_storage
	client.auth = auth
	client.config = config

	atomic.AddInt64(&server_summary.nconnections, 1)

	client.PeerClient = &PeerClient{&client.Connection, relationship_pool, sync_c}
	client.GroupClient = &GroupClient{&client.Connection, group_manager, group_sync_c}
	client.RoomClient = &RoomClient{Connection: &client.Connection}
	client.CustomerClient = NewCustomerClient(&client.Connection)
	return client
}

func handle_client(conn Conn, listener *Listener) {
	low := atomic.LoadInt32(listener.low_memory)
	if low != 0 {
		log.Warning("low memory, drop new connection")
		return
	}
	client := NewClient(conn, listener.group_manager,
		listener.filter, listener.redis_pool,
		listener.server_summary, listener.relationship_pool,
		listener.auth, listener.rpc_storage, listener.sync_c,
		listener.group_sync_c, listener.app_route,
		listener.app, listener.config)
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

func (client *Client) Read() {
	for {
		tc := atomic.LoadInt32(&client.tc)
		if tc > 0 {
			log.Infof("quit read goroutine, client:%d write goroutine blocked", client.uid)
			client.HandleClientClosed()
			break
		}

		t1 := time.Now().Unix()
		msg := client.read()
		t2 := time.Now().Unix()
		if t2-t1 > 6*60 {
			log.Infof("client:%d socket read timeout:%d %d", client.uid, t1, t2)
		}
		if msg == nil {
			client.HandleClientClosed()
			break
		}

		client.HandleMessage(msg)
		t3 := time.Now().Unix()
		if t3-t2 > 2 {
			log.Infof("client:%d handle message is too slow:%d %d", client.uid, t2, t3)
		}
	}
}

func (client *Client) RemoveClient() {
	if client.uid == 0 {
		return
	}
	route := client.app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return
	}
	_, is_delete := route.RemoveClient(client)

	if is_delete {
		atomic.AddInt64(&client.server_summary.clientset_count, -1)
	}
	if client.room_id > 0 {
		route.RemoveRoomClient(client.room_id, client)
	}
}

func (client *Client) HandleClientClosed() {
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

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_AUTH_TOKEN:
		client.HandleAuthToken(msg.body.(*AuthenticationToken), msg.version)
	case MSG_ACK:
		client.HandleACK(msg.body.(*MessageACK))
	case MSG_PING:
		client.HandlePing()
	}

	client.PeerClient.HandleMessage(msg)
	client.GroupClient.HandleMessage(msg)
	client.RoomClient.HandleMessage(msg)
	client.CustomerClient.HandleMessage(msg)
}

func (client *Client) AuthToken(token string) (int64, int64, int, bool, error) {
	appid, uid, err := client.auth.LoadUserAccessToken(token)

	if err != nil {
		return 0, 0, 0, false, err
	}

	forbidden, notification_on, err := GetUserPreferences(client.redis_pool, appid, uid)
	if err != nil {
		return 0, 0, 0, false, err
	}

	return appid, uid, forbidden, notification_on, nil
}

func (client *Client) HandleAuthToken(login *AuthenticationToken, version int) {
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

func (client *Client) AddClient() {
	route := client.app_route.FindOrAddRoute(client.appid)
	is_new := route.AddClient(client)

	atomic.AddInt64(&client.server_summary.nclients, 1)
	if is_new {
		atomic.AddInt64(&client.server_summary.clientset_count, 1)
	}
}

func (client *Client) HandlePing() {
	m := &Message{cmd: MSG_PONG}
	client.EnqueueMessage(m)
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
}

func (client *Client) HandleACK(ack *MessageACK) {
	log.Info("ack:", ack.seq)
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
