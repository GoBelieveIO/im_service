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

type Client struct {
	Connection//必须放在结构体首部
	*IMClient
	*RoomClient
	*VOIPClient
	*CSClient
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

	client.wt = make(chan *Message, 10)
	client.ewt = make(chan *EMessage, 10)
	client.owt = make(chan *EMessage, 10)

	client.unacks = make(map[int]int64)
	client.unackMessages = make(map[int]*EMessage)
	atomic.AddInt64(&server_summary.nconnections, 1)

	client.IMClient = &IMClient{&client.Connection}
	client.RoomClient = &RoomClient{Connection:&client.Connection}
	client.VOIPClient = &VOIPClient{Connection:&client.Connection}
	client.CSClient = NewCSClient(&client.Connection)
	return client
}

func (client *Client) Read() {
	c := make(chan int)
	ticker := time.NewTicker(60*6*time.Second)
	ts := time.Now().Unix()

	go func () {
		for {
			select {
			case _, ok := <-c:
				if !ok {
					log.Info("check socket routine exit")
					return
				}
			case _, ok := <-ticker.C:
				if !ok {
					return
				}
				now := time.Now().Unix()
				if now - ts > 60*6 {
					log.Error("client:%d read is blocked:%d %d", client.uid, ts, now)
				}
			}
		}
	}()
	for {
		ts = time.Now().Unix()
		msg := client.read()
		if msg == nil {
			ticker.Stop()
			close(c)
			client.HandleRemoveClient()
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) HandleRemoveClient() {
	client.wt <- nil
	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return
	}
	route.RemoveClient(client)

	client.RoomClient.Logout(route)
	client.IMClient.Logout()
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MSG_AUTH_TOKEN:
		client.HandleAuthToken(msg.body.(*AuthenticationToken), msg.version)
	case MSG_HEARTBEAT:
		// nothing to do
	case MSG_PING:
		client.HandlePing()
	}

	client.IMClient.HandleMessage(msg)
	client.RoomClient.HandleMessage(msg)
	client.VOIPClient.HandleMessage(msg)
	client.CSClient.HandleMessage(msg)
}


func (client *Client) SendLoginPoint() {
	point := &LoginPoint{}
	point.up_timestamp = int32(client.tm.Unix())
	point.platform_id = client.platform_id
	point.device_id = client.device_id
	msg := &Message{cmd:MSG_LOGIN_POINT, body:point}
	client.SendMessage(client.uid, msg)
}

func (client *Client) AuthToken(token string) (int64, int64, error) {
	appid, uid, _, err := LoadUserAccessToken(token)
	return appid, uid, err
}


func (client *Client) HandleAuthToken(login *AuthenticationToken, version int) {
	if client.uid > 0 {
		log.Info("repeat login")
		return
	}

	var err error
	client.appid, client.uid, err = client.AuthToken(login.token)
	if err != nil {
		log.Infof("auth token:%s err:%s", login.token, err)
		msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
		client.wt <- msg
		return
	}
	if  client.uid == 0 {
		log.Info("auth token uid==0")
		msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
		client.wt <- msg
		return
	}

	if login.platform_id != PLATFORM_WEB && len(login.device_id) > 0{
		client.device_ID, err = GetDeviceID(login.device_id, int(login.platform_id))
		if err != nil {
			log.Info("auth token uid==0")
			msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{1, 0}}
			client.wt <- msg
			return
		}
	}

	client.version = version
	client.device_id = login.device_id
	client.platform_id = login.platform_id
	client.tm = time.Now()
	log.Infof("auth token:%s appid:%d uid:%d device id:%s:%d", 
		login.token, client.appid, client.uid, client.device_id, client.device_ID)

	msg := &Message{cmd: MSG_AUTH_STATUS, version:version, body: &AuthenticationStatus{0, client.public_ip}}
	client.wt <- msg

	client.SendLoginPoint()
	client.AddClient()

	client.IMClient.Login()
	
	close(client.owt)
	log.Infof("offline loaded:%d", client.uid)

	CountDAU(client.appid, client.uid)
	atomic.AddInt64(&server_summary.nclients, 1)
}

func (client *Client) AddClient() {
	route := app_route.FindOrAddRoute(client.appid)
	route.AddClient(client)
}


func (client *Client) HandlePing() {
	m := &Message{cmd: MSG_PONG}
	client.wt <- m
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
}


func (client *Client) Write() {
	seq := 0
	running := true
	loaded := false

	//发送离线消息
	for running && !loaded {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				atomic.AddInt64(&server_summary.nconnections, -1)
				if client.uid > 0 {
					atomic.AddInt64(&server_summary.nclients, -1)
				}
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			if msg.cmd == MSG_RT {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			seq++

			//以当前客户端所用版本号发送消息
			vmsg := &Message{msg.cmd, seq, client.version, msg.body}
			client.send(vmsg)
		case emsg, ok := <- client.owt:
			if !ok {
				//离线消息读取完毕
				loaded = true
				break
			}
			seq++

			emsg.msg.seq = seq
			client.AddUnAckMessage(emsg)

			//以当前客户端所用版本号发送消息
			msg := &Message{emsg.msg.cmd, seq, client.version, emsg.msg.body}
			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			client.send(msg)
		}
	}
	
	//发送在线消息
	for running {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				atomic.AddInt64(&server_summary.nconnections, -1)
				if client.uid > 0 {
					atomic.AddInt64(&server_summary.nclients, -1)
				}
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			if msg.cmd == MSG_RT {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			seq++

			//以当前客户端所用版本号发送消息
			vmsg := &Message{msg.cmd, seq, client.version, msg.body}
			client.send(vmsg)
		case emsg := <- client.ewt:
			seq++

			emsg.msg.seq = seq
			client.AddUnAckMessage(emsg)

			//以当前客户端所用版本号发送消息
			msg := &Message{cmd:emsg.msg.cmd, seq:seq, version:client.version, body:emsg.msg.body}
			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			client.send(msg)
		}
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}
