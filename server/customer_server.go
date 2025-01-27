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

package server

import (
	"time"

	. "github.com/GoBelieveIO/im_service/protocol"
	log "github.com/sirupsen/logrus"
)

func (server *Server) HandleCustomerMessageV2(client *Client, message *Message) {
	msg := message.Body.(*CustomerMessageV2)
	seq := message.Seq

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
	if msg.sender_appid != server.kefu_appid && msg.receiver_appid != server.kefu_appid {
		log.Warningf("invalid appid, customer message sender:%d %d receiver:%d %d",
			msg.sender_appid, msg.sender, msg.receiver_appid, msg.receiver)
		return
	}

	log.Infof("customer message v2 sender:%d %d receiver:%d %d",
		msg.sender_appid, msg.sender, msg.receiver_appid, msg.receiver)

	msg.timestamp = int32(time.Now().Unix())

	m := &Message{Cmd: MSG_CUSTOMER_V2, Version: DEFAULT_VERSION, Body: msg}

	msgid, prev_msgid, err := server.rpc_storage.SaveMessage(msg.receiver_appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	msgid2, prev_msgid2, err := server.rpc_storage.SaveMessage(msg.sender_appid, msg.sender, client.device_ID, m)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	server.app.PushMessage(msg.receiver_appid, msg.receiver, m)

	meta := &Metadata{sync_key: msgid, prev_sync_key: prev_msgid}
	m1 := &Message{Cmd: MSG_CUSTOMER_V2, Version: DEFAULT_VERSION, Flag: message.Flag | MESSAGE_FLAG_PUSH, Body: msg, Meta: meta}
	server.SendAppMessage(client, msg.receiver_appid, msg.receiver, m1)

	notify := &Message{Cmd: MSG_SYNC_NOTIFY, Body: &SyncKey{msgid}}
	server.SendAppMessage(client, msg.receiver_appid, msg.receiver, notify)

	//发送给自己的其它登录点
	meta = &Metadata{sync_key: msgid2, prev_sync_key: prev_msgid2}
	m2 := &Message{Cmd: MSG_CUSTOMER_V2, Version: DEFAULT_VERSION, Flag: message.Flag | MESSAGE_FLAG_PUSH, Body: msg, Meta: meta}
	server.SendMessage(client, client.uid, m2)

	notify = &Message{Cmd: MSG_SYNC_NOTIFY, Body: &SyncKey{msgid2}}
	server.SendMessage(client, client.uid, notify)

	meta = &Metadata{sync_key: msgid2, prev_sync_key: prev_msgid2}
	ack := &Message{Cmd: MSG_ACK, Body: &MessageACK{seq: int32(seq)}, Meta: meta}
	client.EnqueueMessage(ack)
}
