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
import "sync/atomic"
import "errors"
import log "github.com/sirupsen/logrus"

type GroupClient struct {
	*Connection
}

func (client *GroupClient) HandleSuperGroupMessage(msg *IMMessage, group *Group) (int64, int64, error) {
	m := &Message{cmd: MSG_GROUP_IM, version:DEFAULT_VERSION, body: msg}
	msgid, prev_msgid, err := SaveGroupMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Errorf("save group message:%d %d err:%s", msg.sender, msg.receiver, err)
		return 0, 0, err
	}
	
	//推送外部通知
	PushGroupMessage(client.appid, group, m)

	m.meta = &Metadata{sync_key:msgid, prev_sync_key:prev_msgid}
	m.flag = MESSAGE_FLAG_PUSH|MESSAGE_FLAG_SUPER_GROUP
	client.SendGroupMessage(group, m)

	notify := &Message{cmd:MSG_SYNC_GROUP_NOTIFY, body:&GroupSyncKey{group_id:msg.receiver, sync_key:msgid}}
	client.SendGroupMessage(group, notify)
	
	return msgid, prev_msgid, nil
}

func (client *GroupClient) HandleGroupMessage(im *IMMessage, group *Group) (int64, int64, error) {
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
	deliver := GetGroupMessageDeliver(group.gid)
	m := &Message{cmd:MSG_PENDING_GROUP_MESSAGE, body: gm}
	
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

func (client *GroupClient) HandleGroupIMMessage(message *Message) {
	msg := message.body.(*IMMessage)
	seq := message.seq		
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("im message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}
	if message.flag & MESSAGE_FLAG_TEXT != 0 {
		FilterDirtyWord(msg)
	}
	
	msg.timestamp = int32(time.Now().Unix())

	deliver := GetGroupMessageDeliver(msg.receiver)
	group := deliver.LoadGroup(msg.receiver)
	if group == nil {
		log.Warning("can't find group:", msg.receiver)
		return
	}

	if !group.IsMember(msg.sender) {
		ack := &Message{cmd: MSG_ACK, body: &MessageACK{seq:int32(seq), status:ACK_NOT_GROUP_MEMBER}}
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
		msgid, prev_msgid, err := client.HandleSuperGroupMessage(msg, group)
		if err == nil {
			meta = &Metadata{sync_key:msgid, prev_sync_key:prev_msgid}
		}
		flag = MESSAGE_FLAG_SUPER_GROUP
	} else {
		msgid, prev_msgid, err := client.HandleGroupMessage(msg, group)
		if err == nil {
			meta = &Metadata{sync_key:msgid, prev_sync_key:prev_msgid}
		}
	}
	
	ack := &Message{cmd: MSG_ACK, flag:flag, body: &MessageACK{seq:int32(seq)}, meta:meta}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send group message ack error")
	}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d super:%v", msg.sender, msg.receiver, group.super)
	if meta != nil {
		log.Info("group message ack meta:", meta.sync_key, meta.prev_sync_key)
	}	
}


func (client *GroupClient) HandleGroupSync(group_sync_key *GroupSyncKey) {
	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.group_id


	group := group_manager.FindGroup(group_id)
	if group == nil {
		log.Warning("can't find group:", group_id)
		return
	}

	if !group.IsMember(client.uid) {
		log.Warningf("sender:%d is not group member", client.uid)
		return
	}

	ts := group.GetMemberTimestamp(client.uid)
	
	rpc := GetGroupStorageRPCClient(group_id)

	last_id := group_sync_key.sync_key
	if last_id == 0 {
		last_id = GetGroupSyncKey(client.appid, client.uid, group_id)
	}

	s := &SyncGroupHistory{
		AppID:client.appid, 
		Uid:client.uid, 
		DeviceID:client.device_ID, 
		GroupID:group_sync_key.group_id, 
		LastMsgID:last_id,
		Timestamp:int32(ts),
	}

	log.Info("sync group message...", group_sync_key.sync_key, last_id)
	resp, err := rpc.Call("SyncGroupMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}

	gh := resp.(*GroupHistoryMessage)
	messages := gh.Messages
	
	sk := &GroupSyncKey{sync_key:last_id, group_id:group_id}
	client.EnqueueMessage(&Message{cmd:MSG_SYNC_GROUP_BEGIN, body:sk})
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.MsgID, Command(msg.Cmd))
		m := &Message{cmd:int(msg.Cmd), version:DEFAULT_VERSION}
		m.FromData(msg.Raw)
		sk.sync_key = msg.MsgID
		if client.isSender(m, msg.DeviceID) {
			m.flag |= MESSAGE_FLAG_SELF
		}
		client.EnqueueMessage(m)
	}

	if gh.LastMsgID < last_id && gh.LastMsgID > 0 {
		sk.sync_key = gh.LastMsgID
		log.Warningf("group:%d client last id:%d server last id:%d", group_id, last_id, gh.LastMsgID)		
	}
	client.EnqueueMessage(&Message{cmd:MSG_SYNC_GROUP_END, body:sk})
}


func (client *GroupClient) HandleGroupSyncKey(group_sync_key *GroupSyncKey) {
	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.group_id
	last_id := group_sync_key.sync_key

	log.Info("group sync key:", group_sync_key.sync_key, last_id)
	if last_id > 0 {
		s := &SyncGroupHistory{
			AppID:client.appid, 
			Uid:client.uid, 
			GroupID:group_id, 
			LastMsgID:last_id,
		}
		group_sync_c <- s
	}
}

func (client *GroupClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_GROUP_IM:
		client.HandleGroupIMMessage(msg)
	case MSG_SYNC_GROUP:
		client.HandleGroupSync(msg.body.(*GroupSyncKey))
	case MSG_GROUP_SYNC_KEY:
		client.HandleGroupSyncKey(msg.body.(*GroupSyncKey))
	}
}

