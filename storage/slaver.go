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

package storage

import (
	"net"
	"time"

	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
)

type Slaver struct {
	addr    string
	storage *Storage
}

func NewSlaver(addr string, storage *Storage) *Slaver {
	s := new(Slaver)
	s.addr = addr
	s.storage = storage
	return s
}

func (slaver *Slaver) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	seq := 0

	msgid := slaver.storage.NextMessageID()
	cursor := &SyncCursor{msgid}
	log.Info("cursor msgid:", msgid)

	msg := &Message{Cmd: MSG_STORAGE_SYNC_BEGIN, Body: cursor}
	seq += 1
	msg.Seq = seq
	SendMessage(conn, msg)

	for {
		msg := ReceiveStorageSyncMessage(conn)
		if msg == nil {
			return
		}

		if msg.Cmd == MSG_STORAGE_SYNC_MESSAGE {
			emsg := msg.Body.(*EMessage)
			slaver.storage.SaveSyncMessage(emsg)
		} else if msg.Cmd == MSG_STORAGE_SYNC_MESSAGE_BATCH {
			mb := msg.Body.(*MessageBatch)
			slaver.storage.SaveSyncMessageBatch(mb)
		} else {
			log.Error("unknown message cmd:", Command(msg.Cmd))
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
