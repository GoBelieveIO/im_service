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
import "sync"
import log "github.com/golang/glog"


type StorageChannel struct {
	addr            string
	mutex           sync.Mutex
	dispatch_group  func(*AppMessage)
	subscribers     map[AppGroupMemberID]int32
	wt              chan *Message
}

func NewStorageChannel(addr string, f func(*AppMessage)) *StorageChannel {
	channel := new(StorageChannel)
	channel.subscribers = make(map[AppGroupMemberID]int32)
	channel.dispatch_group = f
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

func (sc *StorageChannel) SubscribeGroup(appid int64, gid int64, uid int64, limit int32) {
	sc.mutex.Lock()
	id := AppGroupMemberID{appid:appid, gid:gid, uid:uid, limit:0}
	if _, ok := sc.subscribers[id]; ok {
		sc.mutex.Unlock()
		return
	}
	sc.subscribers[id] = limit
	sc.mutex.Unlock()

	id.limit = limit
	msg := &Message{cmd: MSG_SUBSCRIBE_GROUP, body: &id}
	sc.wt <- msg
}

func (sc *StorageChannel) UnSubscribeGroup(appid int64, gid int64, uid int64) {
	sc.mutex.Lock()
	id := AppGroupMemberID{appid:appid, gid:gid, uid:uid, limit:0}
	if _, ok := sc.subscribers[id]; !ok {
		sc.mutex.Unlock()
		return
	}
	delete(sc.subscribers, id)
	sc.mutex.Unlock()

	msg := &Message{cmd: MSG_UNSUBSCRIBE_GROUP, body: &id}
	sc.wt <- msg
}

func (sc *StorageChannel) GetAllSubscriber() []*AppGroupMemberID {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	subs := make([]*AppGroupMemberID, 0, 100)
	for id, limit := range sc.subscribers {
		s := &AppGroupMemberID{appid:id.appid, gid:id.gid, uid:id.uid, limit:limit}
		subs = append(subs, s)
	}
	return subs
}


func (sc *StorageChannel) ReSubscribe(conn *net.TCPConn, seq int) int {
	subs := sc.GetAllSubscriber()
	for _, id := range(subs) {
		msg := &Message{cmd: MSG_SUBSCRIBE_GROUP, body: id}
		seq = seq + 1
		msg.seq = seq
		SendMessage(conn, msg)
	}
	return seq
}

func (sc *StorageChannel) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	closed_ch := make(chan bool)
	seq := 0
	seq = sc.ReSubscribe(conn, seq)

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closed_ch)
				return
			}
			log.Info("stroage channel recv message:", Command(msg.cmd))
			if msg.cmd == MSG_PUBLISH_GROUP {
				amsg := msg.body.(*AppMessage)
				if sc.dispatch_group != nil {
					sc.dispatch_group(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.cmd)
			}
		}
	}()

	for {
		select {
		case _ = <-closed_ch:
			log.Info("storage channel closed")
			return
		case msg := <-sc.wt:
			seq = seq + 1
			msg.seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := SendMessage(conn, msg)
			if err != nil {
				log.Info("channel send message:", err)
			}
		}
	}
}

func (sc *StorageChannel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", sc.addr)
		if err != nil {
			log.Info("connect server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("storage channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("storage channel connected")
		nsleep = 100
		sc.RunOnce(tconn)
	}
}


func (sc *StorageChannel) Start() {
	go sc.Run()
}
