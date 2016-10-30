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
	client.SubscribeGroup()
	channel := GetUserStorageChannel(client.uid)
	channel.Subscribe(client.appid, client.uid)
	channel = GetChannel(client.uid)
	channel.Subscribe(client.appid, client.uid)

	client.LoadOffline()
	client.LoadGroupOffline()

	SetUserUnreadCount(client.appid, client.uid, 0)
}

func (client *IMClient) Logout() {
	if client.uid > 0 {
		channel := GetChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid)
		channel = GetUserStorageChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid)
		client.UnsubscribeGroup()
	}
}

func (client *IMClient) LoadGroupOfflineMessage(gid int64) ([]*EMessage, error) {
	storage_pool := GetGroupStorageConnPool(gid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return nil, err
	}
	defer storage_pool.Release(storage)

	return storage.LoadGroupOfflineMessage(client.appid, gid, client.uid, client.device_ID)
}

func (client *IMClient) LoadGroupOffline() {
	if client.device_ID == 0 {
		return
	}

	groups := group_manager.FindUserGroups(client.appid, client.uid)
	for _, group := range groups {
		if !group.super {
			continue
		}
		messages, err := client.LoadGroupOfflineMessage(group.gid)
		if err != nil {
			log.Errorf("load group offline message err:%d %s", group.gid, err)
			continue
		}

		for _, emsg := range messages {
			client.EnqueueOfflineMessage(emsg)
		}
	}
}

func (client *IMClient) LoadOffline() {
	if client.device_ID == 0 {
		return
	}
	storage_pool := GetStorageConnPool(client.uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	messages, err := storage.LoadOfflineMessage(client.appid, client.uid, client.device_ID)
	if err != nil {
		log.Errorf("load offline message err:%d %s", client.uid, err)
		return
	}
	for _, emsg := range messages {
		client.EnqueueOfflineMessage(emsg)
	}
}

func (client *IMClient) SubscribeGroup() {
	group_center.SubscribeGroup(client.appid, client.uid)
}

func (client *IMClient) UnsubscribeGroup() {
	group_center.UnsubscribeGroup(client.appid, client.uid)
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
	SaveMessage(client.appid, msg.sender, client.device_ID, m)

	ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send peer message ack error")
	}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (client *IMClient) HandleGroupIMMessage(msg *IMMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_GROUP_IM, version:DEFAULT_VERSION, body: msg}

	group := group_manager.FindGroup(msg.receiver)
	if group == nil {
		log.Warning("can't find group:", msg.receiver)
		return
	}
	if group.super {
		_, err := SaveGroupMessage(client.appid, msg.receiver, client.device_ID, m)
		if err != nil {
			log.Errorf("save group message:%d %d err:%s", err, msg.sender, msg.receiver)
			return
		}
	} else {
		members := group.Members()
		for member := range members {
			_, err := SaveMessage(client.appid, member, client.device_ID, m)
			if err != nil {
				log.Errorf("save group member message:%d %d err:%s", err, msg.sender, msg.receiver)
				continue
			}
		}
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

func (client *IMClient) HandleACK(ack *MessageACK) {
	log.Info("ack:", ack)
	emsg := client.RemoveUnAckMessage(ack)
	if emsg == nil || emsg.msgid == 0 {
		return
	}

	msg := emsg.msg
	if msg != nil && msg.cmd == MSG_GROUP_IM {
		im := emsg.msg.body.(*IMMessage)
		group := group_manager.FindGroup(im.receiver)
		if group != nil && group.super {
			client.DequeueGroupMessage(emsg.msgid, im.receiver)
		} else {
			client.DequeueMessage(emsg.msgid)
		}
	} else {
		client.DequeueMessage(emsg.msgid)
	}

	if msg == nil {
		return
	}
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
	case MSG_ACK:
		client.HandleACK(msg.body.(*MessageACK))
	case MSG_INPUTING:
		client.HandleInputing(msg.body.(*MessageInputing))
	case MSG_RT:
		client.HandleRTMessage(msg)
	case MSG_UNREAD_COUNT:
		client.HandleUnreadCount(msg.body.(*MessageUnreadCount))
	}
}

func (client *IMClient) DequeueGroupMessage(msgid int64, gid int64) {
	storage_pool := GetGroupStorageConnPool(gid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	dq := &DQGroupMessage{msgid:msgid, appid:client.appid, receiver:client.uid, gid:gid, device_id:client.device_ID}
	err = storage.DequeueGroupMessage(dq)
	if err != nil {
		log.Error("dequeue message err:", err)
	}
}

func (client *IMClient) DequeueMessage(msgid int64) {
	storage_pool := GetStorageConnPool(client.uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	dq := &DQMessage{msgid:msgid, appid:client.appid, receiver:client.uid, device_id:client.device_ID}
	err = storage.DequeueMessage(dq)
	if err != nil {
		log.Error("dequeue message err:", err)
	}
}

