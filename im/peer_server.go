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

import (
	"sync/atomic"
	"time"

	"github.com/GoBelieveIO/im_service/storage"
	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
)

func (server *Server) HandleSync(client *Client, msg *Message) {
	sync_key := msg.Body.(*SyncKey)

	if client.uid == 0 {
		return
	}
	last_id := sync_key.sync_key

	if last_id == 0 {
		last_id = GetSyncKey(server.redis_pool, client.appid, client.uid)
	}

	log.Infof("syncing message:%d %d %d %d", client.appid, client.uid, client.device_ID, last_id)

	ph, err := server.rpc_storage.SyncMessage(client.appid, client.uid, client.device_ID, last_id)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}
	messages := ph.Messages
	msgs := make([]*Message, 0, len(messages)+2)

	sk := &SyncKey{last_id}
	msgs = append(msgs, &Message{Cmd: MSG_SYNC_BEGIN, Body: sk})

	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.MsgID, Command(msg.Cmd))
		m := &Message{Cmd: int(msg.Cmd), Version: DEFAULT_VERSION}
		m.FromData(msg.Raw)
		sk.sync_key = msg.MsgID
		if client.isSender(m, msg.DeviceID) {
			m.Flag |= MESSAGE_FLAG_SELF
		}
		msgs = append(msgs, m)
	}

	if ph.LastMsgID < last_id && ph.LastMsgID > 0 {
		sk.sync_key = ph.LastMsgID
		log.Warningf("client last id:%d server last id:%d", last_id, ph.LastMsgID)
	}

	msgs = append(msgs, &Message{Cmd: MSG_SYNC_END, Body: sk})

	client.EnqueueMessages(msgs)

	if ph.HasMore {
		notify := &Message{Cmd: MSG_SYNC_NOTIFY, Body: &SyncKey{ph.LastMsgID + 1}}
		client.EnqueueMessage(notify)
	}
}

func (server *Server) HandleSyncKey(client *Client, msg *Message) {
	sync_key := msg.Body.(*SyncKey)

	if client.uid == 0 {
		return
	}

	last_id := sync_key.sync_key
	log.Infof("sync key:%d %d %d %d", client.appid, client.uid, client.device_ID, last_id)
	if last_id > 0 {
		s := &storage.SyncHistory{
			AppID:     client.appid,
			Uid:       client.uid,
			LastMsgID: last_id,
		}
		server.sync_c <- s
	}
}

func (server *Server) HandleIMMessage(client *Client, message *Message) {
	msg := message.Body.(*IMMessage)
	seq := message.Seq
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("im message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}

	var rs Relationship = NoneRelationship
	if server.friend_permission || server.enable_blacklist {
		rs = server.relationship_pool.GetRelationship(client.appid, client.uid, msg.receiver)
	}
	if server.friend_permission {
		if !rs.IsMyFriend() {
			ack := &Message{Cmd: MSG_ACK, Version: client.version, Body: &MessageACK{seq: int32(seq), status: ACK_NOT_MY_FRIEND}}
			client.EnqueueMessage(ack)
			log.Infof("relationship%d-%d:%d invalid, can't send message", msg.sender, msg.receiver, rs)
			return
		}

		if !rs.IsYourFriend() {
			ack := &Message{Cmd: MSG_ACK, Version: client.version, Body: &MessageACK{seq: int32(seq), status: ACK_NOT_YOUR_FRIEND}}
			client.EnqueueMessage(ack)
			log.Infof("relationship%d-%d:%d invalid, can't send message", msg.sender, msg.receiver, rs)
			return
		}
	}
	if server.enable_blacklist {
		if rs.IsInYourBlacklist() {
			ack := &Message{Cmd: MSG_ACK, Version: client.version, Body: &MessageACK{seq: int32(seq), status: ACK_IN_YOUR_BLACKLIST}}
			client.EnqueueMessage(ack)
			log.Infof("relationship%d-%d:%d invalid, can't send message", msg.sender, msg.receiver, rs)
			return
		}
	}

	if message.Flag&MESSAGE_FLAG_TEXT != 0 {
		FilterDirtyWord(server.filter, msg)
	}
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{Cmd: MSG_IM, Version: DEFAULT_VERSION, Body: msg}

	msgid, prev_msgid, err := server.rpc_storage.SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Errorf("save peer message:%d %d err:%v", msg.sender, msg.receiver, err)
		return
	}

	//保存到自己的消息队列，这样用户的其它登陆点也能接受到自己发出的消息
	msgid2, prev_msgid2, err := server.rpc_storage.SaveMessage(client.appid, msg.sender, client.device_ID, m)
	if err != nil {
		log.Errorf("save peer message:%d %d err:%v", msg.sender, msg.receiver, err)
		return
	}

	//推送外部通知
	server.app.PushMessage(client.appid, msg.receiver, m)

	meta := &Metadata{sync_key: msgid, prev_sync_key: prev_msgid}
	m1 := &Message{Cmd: MSG_IM, Version: DEFAULT_VERSION, Flag: message.Flag | MESSAGE_FLAG_PUSH, Body: msg, Meta: meta}
	server.SendMessage(client, msg.receiver, m1)
	notify := &Message{Cmd: MSG_SYNC_NOTIFY, Body: &SyncKey{msgid}}
	server.SendMessage(client, msg.receiver, notify)

	//发送给自己的其它登录点
	meta = &Metadata{sync_key: msgid2, prev_sync_key: prev_msgid2}
	m2 := &Message{Cmd: MSG_IM, Version: DEFAULT_VERSION, Flag: message.Flag | MESSAGE_FLAG_PUSH, Body: msg, Meta: meta}
	server.SendMessage(client, client.uid, m2)
	notify = &Message{Cmd: MSG_SYNC_NOTIFY, Body: &SyncKey{msgid}}
	server.SendMessage(client, client.uid, notify)

	meta = &Metadata{sync_key: msgid2, prev_sync_key: prev_msgid2}
	ack := &Message{Cmd: MSG_ACK, Body: &MessageACK{seq: int32(seq)}, Meta: meta}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send peer message ack error")
	}

	atomic.AddInt64(&server.server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (server *Server) HandleUnreadCount(client *Client, msg *Message) {
	u := msg.Body.(*MessageUnreadCount)
	SetUserUnreadCount(server.redis_pool, client.appid, client.uid, u.count)
}

func (server *Server) HandleRTMessage(client *Client, msg *Message) {
	rt := msg.Body.(*RTMessage)
	if rt.sender != client.uid {
		log.Warningf("rt message sender:%d client uid:%d\n", rt.sender, client.uid)
		return
	}

	m := &Message{Cmd: MSG_RT, Body: rt}
	server.SendMessage(client, rt.receiver, m)

	atomic.AddInt64(&server.server_summary.in_message_count, 1)
	log.Infof("realtime message sender:%d receiver:%d", rt.sender, rt.receiver)
}
