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

type PeerClient struct {
	*Connection
}

func (client *PeerClient) Login() {
	channel := GetChannel(client.uid)
	
	channel.Subscribe(client.appid, client.uid, client.online)

	for _, c := range group_route_channels {
		if c == channel {
			continue
		}

		c.Subscribe(client.appid, client.uid, client.online)
	}
	
	SetUserUnreadCount(client.appid, client.uid, 0)
}

func (client *PeerClient) Logout() {
	if client.uid > 0 {
		channel := GetChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid, client.online)

		for _, c := range group_route_channels {
			if c == channel {
				continue
			}

			c.Unsubscribe(client.appid, client.uid, client.online)
		}		
	}
}

func (client *PeerClient) HandleSync(sync_key *SyncKey) {
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
	client.sync_count += 1
	
	ph := resp.(*PeerHistoryMessage)
	messages := ph.Messages

	msgs := make([]*Message, 0, len(messages) + 2)
	
	sk := &SyncKey{last_id}
	msgs = append(msgs, &Message{cmd:MSG_SYNC_BEGIN, body:sk})
	
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.MsgID, Command(msg.Cmd))
		m := &Message{cmd:int(msg.Cmd), version:DEFAULT_VERSION}
		m.FromData(msg.Raw)
		sk.sync_key = msg.MsgID

		//连接成功后的首次同步，自己发送的消息也下发给客户端
		//过滤掉所有自己在当前设备发出的消息
		if client.sync_count > 1 && client.isSender(m, msg.DeviceID) {
			continue
		}
		msgs = append(msgs, m)
	}


	if ph.LastMsgID < last_id && ph.LastMsgID > 0 {
		sk.sync_key = ph.LastMsgID
		log.Warningf("client last id:%d server last id:%d", last_id, ph.LastMsgID)
	}

	msgs = append(msgs, &Message{cmd:MSG_SYNC_END, body:sk})

	client.EnqueueMessages(msgs)
}

func (client *PeerClient) HandleSyncKey(sync_key *SyncKey) {
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

func (client *PeerClient) HandleIMMessage(message *Message) {
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


func (client *PeerClient) HandleUnreadCount(u *MessageUnreadCount) {
	SetUserUnreadCount(client.appid, client.uid, u.count)
}

func (client *PeerClient) HandleRTMessage(msg *Message) {
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


func (client *PeerClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_IM:
		client.HandleIMMessage(msg)
	case MSG_RT:
		client.HandleRTMessage(msg)
	case MSG_UNREAD_COUNT:
		client.HandleUnreadCount(msg.body.(*MessageUnreadCount))
	case MSG_SYNC:
		client.HandleSync(msg.body.(*SyncKey))
	case MSG_SYNC_KEY:
		client.HandleSyncKey(msg.body.(*SyncKey))
	}
}


