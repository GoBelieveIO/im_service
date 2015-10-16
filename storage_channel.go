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
	app_subs        map[int64]*ApplicationSubscriber
	wt              chan *Message
}

func NewStorageChannel(addr string, f func(*AppMessage)) *StorageChannel {
	channel := new(StorageChannel)
	channel.app_subs = make(map[int64]*ApplicationSubscriber)
	channel.dispatch_group = f
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

func (sc *StorageChannel) SubscribeGroup(appid int64, gid int64, uid int64) {
	sc.mutex.Lock()
	if _, ok := sc.app_subs[appid]; !ok {
		sc.app_subs[appid] = &ApplicationSubscriber{appid:appid, subs:make(map[int64]*GroupSubscriber)}
	}
	as := sc.app_subs[appid]
	if _, ok := as.subs[uid]; !ok {
		sub := &GroupSubscriber{groups:NewIntSet()}
		as.subs[uid] = sub
	}
	sub := as.subs[uid]

	if sub.groups.IsMember(gid) {
		sc.mutex.Unlock()
		log.Infof("appid:%d gid:%d uid:%d is subscriber\n", appid, gid, uid)
		return
	}

	sub.groups.Add(gid)
	sc.mutex.Unlock()

	log.Infof("subscribe group appid:%d gid:%d uid:%d\n", appid, gid, uid)
	
	id := AppGroupMemberID{appid:appid, gid:gid, uid:uid}
	msg := &Message{cmd: MSG_SUBSCRIBE_GROUP, body: &id}
	sc.wt <- msg
}

func (sc *StorageChannel) UnSubscribeGroup(appid int64, gid int64, uid int64) {
	sc.mutex.Lock()

	if _, ok := sc.app_subs[appid]; !ok {
		sc.mutex.Unlock()
		log.Infof("appid:%d gid:%d uid:%d is't subscriber\n", appid, gid, uid)
		return
	}
	
	as := sc.app_subs[appid]
	if _, ok := as.subs[uid]; !ok {
		sc.mutex.Unlock()
		log.Infof("appid:%d gid:%d uid:%d is't subscriber\n", appid, gid, uid)
		return
	}
	
	sub := as.subs[uid]

	if !sub.groups.IsMember(gid) {
		sc.mutex.Unlock()
		log.Infof("appid:%d gid:%d uid:%d is't subscriber\n", appid, gid, uid)
		return
	}

	sub.groups.Remove(gid)

	if len(sub.groups) == 0 {
		delete(as.subs, uid)
	}

	sc.mutex.Unlock()

	log.Infof("unsubscribe group appid:%d gid:%d uid:%d\n", appid, gid, uid)
	id := AppGroupMemberID{appid:appid, gid:gid, uid:uid}
	msg := &Message{cmd: MSG_UNSUBSCRIBE_GROUP, body: &id}
	sc.wt <- msg
}

func (sc *StorageChannel) GetAllSubscriber() []*AppGroupMemberID {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	subs := make([]*AppGroupMemberID, 0, 100)

	for appid, as := range sc.app_subs {
		for uid, sub := range as.subs {
			for gid := range sub.groups {
				id := &AppGroupMemberID{appid:appid, gid:gid, uid:uid}
				subs = append(subs, id)
			}
		}
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
