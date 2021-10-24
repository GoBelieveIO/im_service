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

import "time"
import log "github.com/sirupsen/logrus"


type CustomerClient struct {
	*Connection
}

func NewCustomerClient(conn *Connection) *CustomerClient {
	c := &CustomerClient{Connection:conn}
	return c
}

func (client *CustomerClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_CUSTOMER_V2:
		client.HandleCustomerMessageV2(msg)
	}
}

func (client *CustomerClient) HandleCustomerMessageV2(message *Message) {
	msg := message.body.(*CustomerMessageV2)
	seq := message.seq

	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid || msg.sender_appid != client.appid {
		log.Warningf("customer message sender:%d %d client:%d %d",
			msg.sender_appid, msg.sender, client.appid, client.uid)
		return
	}

	//限制在客服app和普通app之间
	if msg.sender_appid != config.kefu_appid && msg.receiver_appid != config.kefu_appid {
		log.Warningf("invalid appid, customer message sender:%d %d receiver:%d %d",
			msg.sender_appid, msg.sender, msg.receiver_appid, msg.receiver)
		return
	}

	log.Infof("customer message v2 sender:%d %d receiver:%d %d",
		msg.sender_appid, msg.sender, msg.receiver_appid, msg.receiver)

	msg.timestamp = int32(time.Now().Unix())

	m := &Message{cmd:MSG_CUSTOMER_V2, version:DEFAULT_VERSION, body:msg}
	
	msgid, prev_msgid, err := rpc_storage.SaveMessage(msg.receiver_appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	msgid2, prev_msgid2, err := rpc_storage.SaveMessage(msg.sender_appid, msg.sender, client.device_ID, m)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	PushMessage(msg.receiver_appid, msg.receiver, m)

	meta := &Metadata{sync_key:msgid, prev_sync_key:prev_msgid}
	m1 := &Message{cmd:MSG_CUSTOMER_V2, version:DEFAULT_VERSION, flag:message.flag|MESSAGE_FLAG_PUSH, body:msg, meta:meta}
	SendAppMessage(msg.receiver_appid, msg.receiver, m1)

	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(msg.receiver_appid, msg.receiver, notify)	

	//发送给自己的其它登录点
	meta = &Metadata{sync_key:msgid2, prev_sync_key:prev_msgid2}
	m2 := &Message{cmd:MSG_CUSTOMER_V2, version:DEFAULT_VERSION, flag:message.flag|MESSAGE_FLAG_PUSH, body:msg, meta:meta}
	client.SendMessage(client.uid, m2)

	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)

	meta = &Metadata{sync_key:msgid2, prev_sync_key:prev_msgid2}	
	ack := &Message{cmd: MSG_ACK, body: &MessageACK{seq:int32(seq)}, meta:meta}
	client.EnqueueMessage(ack)
}

