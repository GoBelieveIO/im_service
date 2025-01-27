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
	"bytes"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/GoBelieveIO/im_service/protocol"
	. "github.com/GoBelieveIO/im_service/protocol"
	"github.com/GoBelieveIO/im_service/server"
)

type Subscriber struct {
	uids     map[int64]int
	room_ids map[int64]int
}

func NewSubscriber() *Subscriber {
	s := new(Subscriber)
	s.uids = make(map[int64]int)
	s.room_ids = make(map[int64]int)
	return s
}

type Channel struct {
	addr string
	wt   chan *Message

	mutex       sync.Mutex
	subscribers map[int64]*Subscriber

	dispatch       func(appid, uid int64, msg *Message)
	dispatch_group func(appid, group_id int64, msg *Message)
	dispatch_room  func(appid, room_id int64, msg *Message)
}

func NewChannel(addr string, f func(appid, uid int64, msg *Message),
	f2 func(appid, group_id int64, msg *Message), f3 func(appid, room_id int64, msg *Message)) *Channel {
	channel := new(Channel)
	channel.subscribers = make(map[int64]*Subscriber)
	channel.dispatch = f
	channel.dispatch_group = f2
	channel.dispatch_room = f3
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

func (channel *Channel) Address() string {
	return channel.addr
}

// 返回添加前的计数
func (channel *Channel) AddSubscribe(appid, uid int64, online bool) (int, int) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在时count==0
	count := subscriber.uids[uid]

	//低16位表示总数量 高16位表示online的数量
	c1 := count & 0xffff
	c2 := count >> 16 & 0xffff

	if online {
		c2 += 1
	}
	c1 += 1
	subscriber.uids[uid] = c2<<16 | c1
	return count & 0xffff, count >> 16 & 0xffff
}

// 返回删除前的计数
func (channel *Channel) RemoveSubscribe(appid, uid int64, online bool) (int, int) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0, 0
	}

	count, ok := subscriber.uids[uid]
	//低16位表示总数量 高16位表示online的数量
	c1 := count & 0xffff
	c2 := count >> 16 & 0xffff
	if ok {
		if online {
			c2 -= 1
			//assert c2 >= 0
			if c2 < 0 {
				log.Warning("online count < 0")
			}
		}
		c1 -= 1
		if c1 > 0 {
			subscriber.uids[uid] = c2<<16 | c1
		} else {
			delete(subscriber.uids, uid)
		}
	}
	return count & 0xffff, count >> 16 & 0xffff
}

func (channel *Channel) GetAllSubscriber() map[int64]*Subscriber {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make(map[int64]*Subscriber)
	for appid, s := range channel.subscribers {
		sub := NewSubscriber()
		for uid, c := range s.uids {
			sub.uids[uid] = c
		}

		subs[appid] = sub
	}
	return subs
}

// online表示用户不再接受推送通知(apns, gcm)
func (channel *Channel) Subscribe(appid int64, uid int64, online bool) {
	count, online_count := channel.AddSubscribe(appid, uid, online)
	log.Info("sub count:", count, online_count)
	if count == 0 {
		//新用户上线
		on := 0
		if online {
			on = 1
		}
		id := &SubscribeMessage{appid: appid, uid: uid, online: int8(on)}
		msg := &Message{Cmd: MSG_SUBSCRIBE, Body: id}
		channel.wt <- msg
	} else if online_count == 0 && online {
		//手机端上线
		id := &SubscribeMessage{appid: appid, uid: uid, online: 1}
		msg := &Message{Cmd: MSG_SUBSCRIBE, Body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Unsubscribe(appid int64, uid int64, online bool) {
	count, online_count := channel.RemoveSubscribe(appid, uid, online)
	log.Info("unsub count:", count, online_count)
	if count == 1 {
		//用户断开全部连接
		id := &RouteUserID{appid: appid, uid: uid}
		msg := &Message{Cmd: MSG_UNSUBSCRIBE, Body: id}
		channel.wt <- msg
	} else if count > 1 && online_count == 1 && online {
		//手机端断开连接,pc/web端还未断开连接
		id := &SubscribeMessage{appid: appid, uid: uid, online: 0}
		msg := &Message{Cmd: MSG_SUBSCRIBE, Body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) PublishMessage(appid int64, uid int64, msg *Message) {
	now := time.Now().UnixNano()

	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, msg)
	msg_buf := mbuffer.Bytes()

	amsg := &RouteMessage{appid: appid, receiver: uid, timestamp: now, msg: msg_buf}
	if msg.Meta != nil {
		meta := msg.Meta.(*server.Metadata)
		amsg.msgid = meta.SyncKey()
		amsg.prev_msgid = meta.PrevSyncKey()
	}
	channel.Publish(amsg)
}

func (channel *Channel) PublishGroupMessage(appid int64, group_id int64, msg *Message) {
	now := time.Now().UnixNano()

	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, msg)
	msg_buf := mbuffer.Bytes()

	amsg := &RouteMessage{appid: appid, receiver: group_id, timestamp: now, msg: msg_buf}
	if msg.Meta != nil {
		meta := msg.Meta.(*server.Metadata)
		amsg.msgid = meta.SyncKey()
		amsg.prev_msgid = meta.PrevSyncKey()
	}
	channel.PublishGroup(amsg)
}

func (channel *Channel) PublishRoomMessage(appid int64, room_id int64, m *Message) {
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, m)
	msg_buf := mbuffer.Bytes()
	amsg := &RouteMessage{appid: appid, receiver: room_id, msg: msg_buf}
	channel.PublishRoom(amsg)
}

func (channel *Channel) Publish(amsg *RouteMessage) {
	msg := &Message{Cmd: MSG_PUBLISH, Body: amsg}
	channel.wt <- msg
}

func (channel *Channel) PublishGroup(amsg *RouteMessage) {
	msg := &Message{Cmd: MSG_PUBLISH_GROUP, Body: amsg}
	channel.wt <- msg
}

func (channel *Channel) Push(appid int64, receivers []int64, msg *Message) {
	p := &BatchPushMessage{appid: appid, receivers: receivers, msg: msg}
	m := &Message{Cmd: MSG_PUSH, Body: p}
	channel.wt <- m
}

// 返回添加前的计数
func (channel *Channel) AddSubscribeRoom(appid, room_id int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在count==0
	count := subscriber.room_ids[room_id]
	subscriber.room_ids[room_id] = count + 1
	return count
}

// 返回删除前的计数
func (channel *Channel) RemoveSubscribeRoom(appid, room_id int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0
	}

	count, ok := subscriber.room_ids[room_id]
	if ok {
		if count > 1 {
			subscriber.room_ids[room_id] = count - 1
		} else {
			delete(subscriber.room_ids, room_id)
		}
	}
	return count
}

func (channel *Channel) GetAllRoomSubscriber() []*RouteRoomID {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make([]*RouteRoomID, 0, 100)
	for appid, s := range channel.subscribers {
		for room_id := range s.room_ids {
			id := &RouteRoomID{appid: appid, room_id: room_id}
			subs = append(subs, id)
		}
	}
	return subs
}

func (channel *Channel) SubscribeRoom(appid int64, room_id int64) {
	count := channel.AddSubscribeRoom(appid, room_id)
	log.Info("sub room count:", count)
	if count == 0 {
		id := &RouteRoomID{appid: appid, room_id: room_id}
		msg := &Message{Cmd: MSG_SUBSCRIBE_ROOM, Body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) UnsubscribeRoom(appid int64, room_id int64) {
	count := channel.RemoveSubscribeRoom(appid, room_id)
	log.Info("unsub room count:", count)
	if count == 1 {
		id := &RouteRoomID{appid: appid, room_id: room_id}
		msg := &Message{Cmd: MSG_UNSUBSCRIBE_ROOM, Body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) PublishRoom(amsg *RouteMessage) {
	msg := &Message{Cmd: MSG_PUBLISH_ROOM, Body: amsg}
	channel.wt <- msg
}

func (channel *Channel) ReSubscribe(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllSubscriber()
	for appid, sub := range subs {
		for uid, count := range sub.uids {
			//低16位表示总数量 高16位表示online的数量
			c2 := count >> 16 & 0xffff
			on := 0
			if c2 > 0 {
				on = 1
			}

			id := &SubscribeMessage{appid: appid, uid: uid, online: int8(on)}
			msg := &Message{Cmd: MSG_SUBSCRIBE, Body: id}

			seq = seq + 1
			msg.Seq = seq
			SendMessage(conn, msg)
		}
	}
	return seq
}

func (channel *Channel) ReSubscribeRoom(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllRoomSubscriber()
	for _, id := range subs {
		msg := &Message{Cmd: MSG_SUBSCRIBE_ROOM, Body: id}
		seq = seq + 1
		msg.Seq = seq
		SendMessage(conn, msg)
	}
	return seq
}

func (channel *Channel) DispatchMessage(amsg *RouteMessage) {
	now := time.Now().UnixNano()
	d := now - amsg.timestamp

	mbuffer := bytes.NewBuffer(amsg.msg)
	msg := protocol.ReceiveMessage(mbuffer)
	if msg == nil {
		log.Warning("can't dispatch message")
		return
	}

	log.Infof("dispatch app message:%s %d %d", protocol.Command(msg.Cmd), msg.Flag, d)
	if d > int64(time.Second) {
		log.Warning("dispatch app message slow...")
	}

	if amsg.msgid > 0 {
		if (msg.Flag & protocol.MESSAGE_FLAG_PUSH) == 0 {
			log.Fatal("invalid message flag", msg.Flag)
		}
		meta := server.NewMetadata(amsg.msgid, amsg.prev_msgid)
		msg.Meta = meta
	}
	channel.dispatch(amsg.appid, amsg.receiver, msg)
}

func (channel *Channel) DispatchRoomMessage(amsg *RouteMessage) {
	mbuffer := bytes.NewBuffer(amsg.msg)
	msg := protocol.ReceiveMessage(mbuffer)
	if msg == nil {
		log.Warning("can't dispatch room message")
		return
	}

	log.Info("dispatch room message", protocol.Command(msg.Cmd))

	room_id := amsg.receiver
	channel.dispatch_room(amsg.appid, room_id, msg)
}

func (channel *Channel) DispatchGroupMessage(amsg *RouteMessage) {
	now := time.Now().UnixNano()
	d := now - amsg.timestamp
	mbuffer := bytes.NewBuffer(amsg.msg)
	msg := protocol.ReceiveMessage(mbuffer)
	if msg == nil {
		log.Warning("can't dispatch room message")
		return
	}
	log.Infof("dispatch group message:%s %d %d", protocol.Command(msg.Cmd), msg.Flag, d)
	if d > int64(time.Second) {
		log.Warning("dispatch group message slow...")
	}

	if amsg.msgid > 0 {
		if (msg.Flag & protocol.MESSAGE_FLAG_PUSH) == 0 {
			log.Fatal("invalid message flag", msg.Flag)
		}
		if (msg.Flag & protocol.MESSAGE_FLAG_SUPER_GROUP) == 0 {
			log.Fatal("invalid message flag", msg.Flag)
		}

		meta := server.NewMetadata(amsg.msgid, amsg.prev_msgid)
		msg.Meta = meta
	}

	channel.dispatch_group(amsg.appid, amsg.receiver, msg)
}

func (channel *Channel) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	closed_ch := make(chan bool)
	seq := 0
	seq = channel.ReSubscribe(conn, seq)
	seq = channel.ReSubscribeRoom(conn, seq)

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closed_ch)
				return
			}
			log.Info("channel recv message:", Command(msg.Cmd))
			if msg.Cmd == MSG_PUBLISH {
				amsg := msg.Body.(*RouteMessage)
				if channel.dispatch != nil {
					channel.DispatchMessage(amsg)
				}
			} else if msg.Cmd == MSG_PUBLISH_ROOM {
				amsg := msg.Body.(*RouteMessage)
				if channel.dispatch_room != nil {
					channel.DispatchRoomMessage(amsg)
				}
			} else if msg.Cmd == MSG_PUBLISH_GROUP {
				amsg := msg.Body.(*RouteMessage)
				if channel.dispatch_group != nil {
					channel.DispatchGroupMessage(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.Cmd)
			}
		}
	}()

	for {
		select {
		case <-closed_ch:
			log.Info("channel closed")
			return
		case msg := <-channel.wt:
			seq = seq + 1
			msg.Seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := SendMessage(conn, msg)
			if err != nil {
				log.Info("channel send message:", err)
			}
		}
	}
}

func (channel *Channel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", channel.addr)
		if err != nil {
			log.Info("connect route server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("channel connected")
		nsleep = 100
		channel.RunOnce(tconn)
	}
}

func (channel *Channel) Start() {
	go channel.Run()
}
