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
import "sync"
import "time"
import log "github.com/sirupsen/logrus"

type SyncClient struct {
	conn *net.TCPConn
	ewt  chan *Message
}

func NewSyncClient(conn *net.TCPConn) *SyncClient {
	c := new(SyncClient)
	c.conn = conn
	c.ewt = make(chan *Message, 10)
	return c
}

func (client *SyncClient) RunLoop() {
	seq := 0
	msg := ReceiveMessage(client.conn)
	if msg == nil {
		return
	}
	if msg.cmd != MSG_STORAGE_SYNC_BEGIN {
		return
	}

	cursor := msg.body.(*SyncCursor)
	log.Info("cursor msgid:", cursor.msgid)
	c := storage.LoadSyncMessagesInBackground(cursor.msgid)

	for batch := range c {
		msg := &Message{cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, body: batch}
		seq = seq + 1
		msg.seq = seq
		SendMessage(client.conn, msg)
	}

	master.AddClient(client)
	defer master.RemoveClient(client)

	for {
		msg := <-client.ewt
		if msg == nil {
			log.Warning("chan closed")
			break
		}

		seq = seq + 1
		msg.seq = seq
		err := SendMessage(client.conn, msg)
		if err != nil {
			break
		}
	}
}

func (client *SyncClient) Run() {
	go client.RunLoop()
}

type Master struct {
	ewt chan *EMessage

	mutex   sync.Mutex
	clients map[*SyncClient]struct{}
}

func NewMaster() *Master {
	master := new(Master)
	master.clients = make(map[*SyncClient]struct{})
	master.ewt = make(chan *EMessage, 10)
	return master
}

func (master *Master) AddClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.clients[client] = struct{}{}
}

func (master *Master) RemoveClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	delete(master.clients, client)
}

func (master *Master) CloneClientSet() map[*SyncClient]struct{} {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	clone := make(map[*SyncClient]struct{})
	for k, v := range master.clients {
		clone[k] = v
	}
	return clone
}

func (master *Master) SendBatch(cache []*EMessage) {
	if len(cache) == 0 {
		return
	}

	batch := &MessageBatch{msgs: make([]*Message, 0, 1000)}
	batch.first_id = cache[0].msgid
	for _, em := range cache {
		batch.last_id = em.msgid
		batch.msgs = append(batch.msgs, em.msg)
	}
	m := &Message{cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, body: batch}
	clients := master.CloneClientSet()
	for c := range clients {
		c.ewt <- m
	}
}

func (master *Master) Run() {
	cache := make([]*EMessage, 0, 1000)
	var first_ts time.Time
	for {
		t := 60 * time.Second
		tf := time.After(t)
		if len(cache) > 0 {
			ts := first_ts.Add(time.Second * 1)
			now := time.Now()

			if ts.After(now) {
				t = ts.Sub(now)
			} else {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		}
		select {
		case emsg := <-master.ewt:
			cache = append(cache, emsg)
			if len(cache) == 1 {
				first_ts = time.Now()
			}
			if len(cache) >= 1000 {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		case <-tf:
			if len(cache) > 0 {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		}
	}
}

func (master *Master) Start() {
	go master.Run()
}

type Slaver struct {
	addr string
}

func NewSlaver(addr string) *Slaver {
	s := new(Slaver)
	s.addr = addr
	return s
}

func (slaver *Slaver) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	seq := 0

	msgid := storage.NextMessageID()
	cursor := &SyncCursor{msgid}
	log.Info("cursor msgid:", msgid)

	msg := &Message{cmd: MSG_STORAGE_SYNC_BEGIN, body: cursor}
	seq += 1
	msg.seq = seq
	SendMessage(conn, msg)

	for {
		msg := ReceiveStorageSyncMessage(conn)
		if msg == nil {
			return
		}

		if msg.cmd == MSG_STORAGE_SYNC_MESSAGE {
			emsg := msg.body.(*EMessage)
			storage.SaveSyncMessage(emsg)
		} else if msg.cmd == MSG_STORAGE_SYNC_MESSAGE_BATCH {
			mb := msg.body.(*MessageBatch)
			storage.SaveSyncMessageBatch(mb)
		} else {
			log.Error("unknown message cmd:", Command(msg.cmd))
		}
	}
}

func (slaver *Slaver) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", slaver.addr)
		if err != nil {
			log.Info("connect master server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("slaver sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("slaver connected with master")
		nsleep = 100
		slaver.RunOnce(tconn)
	}
}

func (slaver *Slaver) Start() {
	go slaver.Run()
}
