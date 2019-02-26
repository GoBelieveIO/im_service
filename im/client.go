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
import "time"
import "sync/atomic"
import log "github.com/golang/glog"
import "container/list"

type Client struct {
	Connection//必须放在结构体首部
	*PeerClient
	*GroupClient
	*RoomClient
	*CustomerClient
	public_ip int32
}

func NewClient(conn interface{}) *Client {
	client := new(Client)

	//初始化Connection
	client.conn = conn // conn is net.Conn or engineio.Conn

	if net_conn, ok := conn.(net.Conn); ok {
		addr := net_conn.LocalAddr()
		if taddr, ok := addr.(*net.TCPAddr); ok {
			ip4 := taddr.IP.To4()
			client.public_ip = int32(ip4[0]) << 24 | int32(ip4[1]) << 16 | int32(ip4[2]) << 8 | int32(ip4[3])
		}
	}

	client.wt = make(chan *Message, 300)
	client.lwt = make(chan int, 1)//only need 1
	//'10'对于用户拥有非常多的超级群，读线程还是有可能会阻塞
	client.pwt = make(chan []*Message, 10)
	client.messages = list.New()
	
	atomic.AddInt64(&server_summary.nconnections, 1)

	client.PeerClient = &PeerClient{&client.Connection}
	client.GroupClient = &GroupClient{&client.Connection}	
	client.RoomClient = &RoomClient{Connection:&client.Connection}
	client.CustomerClient = NewCustomerClient(&client.Connection)
	return client
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
		if t2 - t1 > 6*60 {
			log.Infof("client:%d socket read timeout:%d %d", client.uid, t1, t2)
		}
		if msg == nil {
			client.HandleClientClosed()
			break
		}

		client.HandleMessage(msg)
		t3 := time.Now().Unix()
		if t3 - t2 > 2 {
			log.Infof("client:%d handle message is too slow:%d %d", client.uid, t2, t3)
		}
	}
}

func (client *Client) RemoveClient() {
	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return
	}
	route.RemoveClient(client)

	if client.room_id > 0 {
		route.RemoveRoomClient(client.room_id, client)
	}
}

func (client *Client) HandleClientClosed() {
	atomic.AddInt64(&server_summary.nconnections, -1)
	if client.uid > 0 {
		atomic.AddInt64(&server_summary.nclients, -1)
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
	appid, uid, forbidden, notification_on, err := LoadUserAccessToken(token)

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
		msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
		client.EnqueueMessage(msg)
		return
	}
	if  uid == 0 {
		log.Info("auth token uid==0")
		msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
		client.EnqueueMessage(msg)
		return
	}

	if login.platform_id != PLATFORM_WEB && len(login.device_id) > 0{
		client.device_ID, err = GetDeviceID(login.device_id, int(login.platform_id))
		if err != nil {
			log.Info("auth token uid==0")
			msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
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

	msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{0, client.public_ip}}
	client.EnqueueMessage(msg)

	client.AddClient()

	client.PeerClient.Login()

	CountDAU(client.appid, client.uid)
	atomic.AddInt64(&server_summary.nclients, 1)
}

func (client *Client) AddClient() {
	route := app_route.FindOrAddRoute(client.appid)
	route.AddClient(client)
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

//发送等待队列中的消息
func (client *Client) SendMessages(seq int) int {
	var messages *list.List
	client.mutex.Lock()
	if (client.messages.Len() == 0) {
		client.mutex.Unlock()		
		return seq
	}
	messages = client.messages
	client.messages = list.New()
	client.mutex.Unlock()

	e := messages.Front();	
	for e != nil {
		msg := e.Value.(*Message)
		if msg.cmd == MSG_RT || msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
			atomic.AddInt64(&server_summary.out_message_count, 1)
		}
		seq++
		//以当前客户端所用版本号发送消息
		vmsg := &Message{msg.cmd, seq, client.version, msg.flag, msg.body}
		client.send(vmsg)
		
		e = e.Next()
	}
	return seq
}

func (client *Client) Write() {
	seq := 0
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
			if msg.cmd == MSG_RT || msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			seq++

			//以当前客户端所用版本号发送消息
			vmsg := &Message{msg.cmd, seq, client.version, msg.flag, msg.body}
			client.send(vmsg)
		case messages := <- client.pwt:
			for _, msg := range(messages) {
				if msg.cmd == MSG_RT || msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
					atomic.AddInt64(&server_summary.out_message_count, 1)
				}
				seq++

				//以当前客户端所用版本号发送消息
				vmsg := &Message{msg.cmd, seq, client.version, msg.flag, msg.body}
				client.send(vmsg)
			}
		case <- client.lwt:
			seq = client.SendMessages(seq)
			break

		}
	}

	//等待200ms,避免发送者阻塞
	t := time.After(200 * time.Millisecond)
	running = true
	for running {
		select {
		case <- t:
			running = false
		case <- client.wt:
			log.Warning("msg is dropped")
		}
	}

	log.Info("write goroutine exit")
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}
