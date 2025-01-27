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
	"errors"
	"sync/atomic"
	"time"

	"github.com/GoBelieveIO/im_service/storage"
	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
)

func (server *Server) HandleSuperGroupMessage(client *Client, msg *IMMessage, group *Group) (int64, int64, error) {
	m := &Message{Cmd: MSG_GROUP_IM, Version: DEFAULT_VERSION, Body: msg}
	msgid, prev_msgid, err := server.rpc_storage.SaveGroupMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Errorf("save group message:%d %d err:%s", msg.sender, msg.receiver, err)
		return 0, 0, err
	}

	//推送外部通知
	server.app.PushGroupMessage(client.appid, group, m)

	m.Meta = &Metadata{sync_key: msgid, prev_sync_key: prev_msgid}
	m.Flag = MESSAGE_FLAG_PUSH | MESSAGE_FLAG_SUPER_GROUP
	server.SendGroupMessage(client, group, m)

	notify := &Message{Cmd: MSG_SYNC_GROUP_NOTIFY, Body: &GroupSyncKey{group_id: msg.receiver, sync_key: msgid}}
	server.SendGroupMessage(client, group, notify)

	return msgid, prev_msgid, nil
}

func (server *Server) HandleGroupMessage(client *Client, im *IMMessage, group *Group) (int64, int64, error) {
	gm := &PendingGroupMessage{}
	gm.appid = client.appid
	gm.sender = im.sender
	gm.device_ID = client.device_ID
	gm.gid = im.receiver
	gm.timestamp = im.timestamp

	members := group.Members()
	gm.members = make([]int64, len(members))
	i := 0
	for uid := range members {
		gm.members[i] = uid
		i += 1
	}

	gm.content = im.content
	deliver := server.app.GetGroupMessageDeliver(group.gid)
	m := &Message{Cmd: MSG_PENDING_GROUP_MESSAGE, Body: gm}

	c := make(chan *Metadata, 1)
	callback_id := deliver.SaveMessage(m, c)
	defer deliver.RemoveCallback(callback_id)
	select {
	case meta := <-c:
		return meta.sync_key, meta.prev_sync_key, nil
	case <-time.After(time.Second * 2):
		log.Errorf("save group message:%d %d timeout", im.sender, im.receiver)
		return 0, 0, errors.New("timeout")
	}
}

func (server *Server) HandleGroupIMMessage(client *Client, message *Message) {
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
	if message.Flag&MESSAGE_FLAG_TEXT != 0 {
		FilterDirtyWord(server.filter, msg)
	}

	msg.timestamp = int32(time.Now().Unix())

	loader := server.app.GetGroupLoader(msg.receiver)
	group := loader.LoadGroup(msg.receiver)
	if group == nil {
		ack := &Message{Cmd: MSG_ACK, Body: &MessageACK{seq: int32(seq), status: ACK_GROUP_NONEXIST}}
		client.EnqueueMessage(ack)
		log.Warning("can't find group:", msg.receiver)
		return
	}

	if !group.IsMember(msg.sender) {
		ack := &Message{Cmd: MSG_ACK, Body: &MessageACK{seq: int32(seq), status: ACK_NOT_GROUP_MEMBER}}
		client.EnqueueMessage(ack)
		log.Warningf("sender:%d is not group member", msg.sender)
		return
	}

	if group.GetMemberMute(msg.sender) {
		log.Warningf("sender:%d is mute in group", msg.sender)
		return
	}

	var meta *Metadata
	var flag int
	if group.super {
		msgid, prev_msgid, err := server.HandleSuperGroupMessage(client, msg, group)
		if err == nil {
			meta = &Metadata{sync_key: msgid, prev_sync_key: prev_msgid}
		}
		flag = MESSAGE_FLAG_SUPER_GROUP
	} else {
		msgid, prev_msgid, err := server.HandleGroupMessage(client, msg, group)
		if err == nil {
			meta = &Metadata{sync_key: msgid, prev_sync_key: prev_msgid}
		}
	}

	ack := &Message{Cmd: MSG_ACK, Flag: flag, Body: &MessageACK{seq: int32(seq)}, Meta: meta}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send group message ack error")
	}

	atomic.AddInt64(&server.server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d super:%v", msg.sender, msg.receiver, group.super)
	if meta != nil {
		log.Info("group message ack meta:", meta.sync_key, meta.prev_sync_key)
	}
}

func (server *Server) HandleGroupSync(client *Client, msg *Message) {
	group_sync_key := msg.Body.(*GroupSyncKey)

	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.group_id

	group := server.group_manager.FindGroup(group_id)
	if group == nil {
		log.Warning("can't find group:", group_id)
		return
	}

	if !group.IsMember(client.uid) {
		log.Warningf("sender:%d is not group member", client.uid)
		return
	}

	ts := group.GetMemberTimestamp(client.uid)

	last_id := group_sync_key.sync_key
	if last_id == 0 {
		last_id = GetGroupSyncKey(server.redis_pool, client.appid, client.uid, group_id)
	}

	log.Info("sync group message...", group_sync_key.sync_key, last_id)
	gh, err := server.rpc_storage.SyncGroupMessage(client.appid, client.uid, client.device_ID, group_sync_key.group_id, last_id, int32(ts))
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}
	messages := gh.Messages

	sk := &GroupSyncKey{sync_key: last_id, group_id: group_id}
	client.EnqueueMessage(&Message{Cmd: MSG_SYNC_GROUP_BEGIN, Body: sk})
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.MsgID, Command(msg.Cmd))
		m := &Message{Cmd: int(msg.Cmd), Version: DEFAULT_VERSION}
		m.FromData(msg.Raw)
		sk.sync_key = msg.MsgID
		if client.isSender(m, msg.DeviceID) {
			m.Flag |= MESSAGE_FLAG_SELF
		}
		client.EnqueueMessage(m)
	}

	if gh.LastMsgID < last_id && gh.LastMsgID > 0 {
		sk.sync_key = gh.LastMsgID
		log.Warningf("group:%d client last id:%d server last id:%d", group_id, last_id, gh.LastMsgID)
	}
	client.EnqueueMessage(&Message{Cmd: MSG_SYNC_GROUP_END, Body: sk})
}

func (server *Server) HandleGroupSyncKey(client *Client, msg *Message) {
	group_sync_key := msg.Body.(*GroupSyncKey)

	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.group_id
	last_id := group_sync_key.sync_key

	log.Info("group sync key:", group_sync_key.sync_key, last_id)
	if last_id > 0 {
		s := &storage.SyncGroupHistory{
			AppID:     client.appid,
			Uid:       client.uid,
			GroupID:   group_id,
			LastMsgID: last_id,
		}
		server.group_sync_c <- s
	}
}
