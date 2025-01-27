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

	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
)

type SyncClient struct {
	conn    *net.TCPConn
	ewt     chan *Message
	storage *Storage
	master  *Master
}

func NewSyncClient(conn *net.TCPConn, storage *Storage, master *Master) *SyncClient {
	c := new(SyncClient)
	c.conn = conn
	c.ewt = make(chan *Message, 10)
	c.storage = storage
	c.master = master
	return c
}

func (client *SyncClient) RunLoop() {
	seq := 0
	msg := ReceiveMessage(client.conn)
	if msg == nil {
		return
	}
	if msg.Cmd != MSG_STORAGE_SYNC_BEGIN {
		return
	}

	cursor := msg.Body.(*SyncCursor)
	log.Info("cursor msgid:", cursor.msgid)
	c := client.storage.LoadSyncMessagesInBackground(cursor.msgid)

	for batch := range c {
		msg := &Message{Cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, Body: batch}
		seq = seq + 1
		msg.Seq = seq
		SendMessage(client.conn, msg)
	}

	client.master.AddClient(client)
	defer client.master.RemoveClient(client)

	for {
		msg := <-client.ewt
		if msg == nil {
			log.Warning("chan closed")
			break
		}

		seq = seq + 1
		msg.Seq = seq
		err := SendMessage(client.conn, msg)
		if err != nil {
			break
		}
	}
}

func (client *SyncClient) Run() {
	go client.RunLoop()
}
