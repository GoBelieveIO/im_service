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
import log "github.com/golang/glog"

type IMClient struct {
	*Connection
}

func (client *IMClient) Login() {
	channel := GetChannel(client.uid)
	channel.Subscribe(client.appid, client.uid)

	for _, c := range group_route_channels {
		if c == channel {
			continue
		}

		c.Subscribe(client.appid, client.uid)
	}
	
	SetUserUnreadCount(client.appid, client.uid, 0)
}

func (client *IMClient) Logout() {
	if client.uid > 0 {
		channel := GetChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid)

		for _, c := range group_route_channels {
			if c == channel {
				continue
			}

			c.Unsubscribe(client.appid, client.uid)
		}		
	}
}


//自己是否是发送者
func (client *IMClient) isSender(msg *Message, device_id int64) bool {
	if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
		m := msg.body.(*IMMessage)
		if m.sender == client.uid && device_id == client.device_ID {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER {
		m := msg.body.(*CustomerMessage)
		if m.customer_appid == client.appid && 
			m.customer_id == client.uid && 
			device_id == client.device_ID {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER_SUPPORT {
		m := msg.body.(*CustomerMessage)
		if config.kefu_appid == client.appid && 
			m.seller_id == client.uid && 
			device_id == client.device_ID {
			return true
		}
	}
	return false
}


func (client *IMClient) HandleGroupSync(group_sync_key *GroupSyncKey) {
	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.group_id
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
	}

	log.Info("sync group message...", group_sync_key.sync_key, last_id)
	resp, err := rpc.Call("SyncGroupMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}

	messages := resp.([]*HistoryMessage)

	sk := &GroupSyncKey{sync_key:last_id, group_id:group_id}
	client.EnqueueMessage(&Message{cmd:MSG_SYNC_GROUP_BEGIN, body:sk})
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.MsgID, Command(msg.Cmd))
		m := &Message{cmd:int(msg.Cmd), version:DEFAULT_VERSION}
		m.FromData(msg.Raw)
		sk.sync_key = msg.MsgID

		//过滤掉所有自己在当前设备发出的消息
		if client.isSender(m, msg.DeviceID) {
			continue
		}

		client.EnqueueMessage(m)
	}

	client.EnqueueMessage(&Message{cmd:MSG_SYNC_GROUP_END, body:sk})
}


func (client *IMClient) HandleGroupSyncKey(group_sync_key *GroupSyncKey) {
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


func (client *IMClient) HandleSync(sync_key *SyncKey) {
	if client.uid == 0 {
		return
	}
	last_id := sync_key.sync_key

	if last_id == 0 {
		last_id = GetSyncKey(client.appid, client.uid)
	}

	rpc := GetStorageRPCClient(client.uid)

	s := &SyncHistory{
		AppID:client.appid, 
		Uid:client.uid, 
		DeviceID:client.device_ID, 
		LastMsgID:last_id,
	}

	log.Infof("syncing message:%d %d %d %d", client.appid, client.uid, client.device_ID, last_id)

	resp, err := rpc.Call("SyncMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}
	
	messages := resp.([]*HistoryMessage)

	sk := &SyncKey{last_id}
	client.EnqueueMessage(&Message{cmd:MSG_SYNC_BEGIN, body:sk})
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.MsgID, Command(msg.Cmd))
		m := &Message{cmd:int(msg.Cmd), version:DEFAULT_VERSION}
		m.FromData(msg.Raw)
		sk.sync_key = msg.MsgID

		//过滤掉所有自己在当前设备发出的消息
		if client.isSender(m, msg.DeviceID) {
			continue
		}
		
		client.EnqueueMessage(m)
	}

	client.EnqueueMessage(&Message{cmd:MSG_SYNC_END, body:sk})
}

func (client *IMClient) HandleSyncKey(sync_key *SyncKey) {
	if client.uid == 0 {
		return
	}

	last_id := sync_key.sync_key
	log.Infof("sync key:%d %d %d %d", client.appid, client.uid, client.device_ID, last_id)
	if last_id > 0 {
		s := &SyncHistory{
			AppID:client.appid, 
			Uid:client.uid, 
			LastMsgID:last_id,
		}
		sync_c <- s
	}
}

func (client *IMClient) HandleIMMessage(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("im message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_IM, version:DEFAULT_VERSION, body: msg}

	msgid, err := SaveMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Errorf("save peer message:%d %d err:", msg.sender, msg.receiver, err)
		return
	}

	//保存到自己的消息队列，这样用户的其它登陆点也能接受到自己发出的消息
	msgid2, err := SaveMessage(client.appid, msg.sender, client.device_ID, m)
	if err != nil {
		log.Errorf("save peer message:%d %d err:", msg.sender, msg.receiver, err)
		return
	}

	//推送外部通知
	PushMessage(client.appid, msg.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	client.SendMessage(msg.receiver, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)
	

	ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send peer message ack error")
	}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (client *IMClient) HandleSuperGroupMessage(msg *IMMessage) {
	m := &Message{cmd: MSG_GROUP_IM, version:DEFAULT_VERSION, body: msg}
	msgid, err := SaveGroupMessage(client.appid, msg.receiver, client.device_ID, m)
	if err != nil {
		log.Errorf("save group message:%d %d err:%s", msg.sender, msg.receiver, err)
		return
	}
	
	//推送外部通知
	PushGroupMessage(client.appid, msg.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_GROUP_NOTIFY, body:&GroupSyncKey{group_id:msg.receiver, sync_key:msgid}}
	client.SendGroupMessage(msg.receiver, notify)
}

func (client *IMClient) HandleGroupMessage(im *IMMessage, group *Group) {
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
	
	m := &Message{cmd:MSG_PENDING_GROUP_MESSAGE, body: gm}
	group_message_deliver.SaveMessage(m)
}

func (client *IMClient) HandleGroupIMMessage(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("im message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}
	
	msg.timestamp = int32(time.Now().Unix())

	group := group_manager.FindGroup(msg.receiver)
	if group == nil {
		log.Warning("can't find group:", msg.receiver)
		return
	}

	if !group.IsMember(msg.sender) {
		log.Warningf("sender:%d is not group member", msg.sender)
		return
	}
	if group.super {
		client.HandleSuperGroupMessage(msg)
	} else {
		client.HandleGroupMessage(msg, group)
	}
	ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send group message ack error")
	}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d", msg.sender, msg.receiver)
}

func (client *IMClient) HandleInputing(inputing *MessageInputing) {
	msg := &Message{cmd: MSG_INPUTING, body: inputing}
	client.SendMessage(inputing.receiver, msg)
	log.Infof("inputting sender:%d receiver:%d", inputing.sender, inputing.receiver)
}

func (client *IMClient) HandleUnreadCount(u *MessageUnreadCount) {
	SetUserUnreadCount(client.appid, client.uid, u.count)
}

func (client *IMClient) HandleRTMessage(msg *Message) {
	rt := msg.body.(*RTMessage)
	if rt.sender != client.uid {
		log.Warningf("rt message sender:%d client uid:%d\n", rt.sender, client.uid)
		return
	}
	
	m := &Message{cmd:MSG_RT, body:rt}
	client.SendMessage(rt.receiver, m)

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("realtime message sender:%d receiver:%d", rt.sender, rt.receiver)
}


func (client *IMClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_IM:
		client.HandleIMMessage(msg.body.(*IMMessage), msg.seq)
	case MSG_GROUP_IM:
		client.HandleGroupIMMessage(msg.body.(*IMMessage), msg.seq)
	case MSG_INPUTING:
		client.HandleInputing(msg.body.(*MessageInputing))
	case MSG_RT:
		client.HandleRTMessage(msg)
	case MSG_UNREAD_COUNT:
		client.HandleUnreadCount(msg.body.(*MessageUnreadCount))
	case MSG_SYNC:
		client.HandleSync(msg.body.(*SyncKey))
	case MSG_SYNC_KEY:
		client.HandleSyncKey(msg.body.(*SyncKey))
	case MSG_SYNC_GROUP:
		client.HandleGroupSync(msg.body.(*GroupSyncKey))
	case MSG_GROUP_SYNC_KEY:
		client.HandleGroupSyncKey(msg.body.(*GroupSyncKey))
	}
}


