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
	"bytes"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

func GetChannel(uid int64) *Channel {
	if uid < 0 {
		uid = -uid
	}
	index := uid % int64(len(route_channels))
	return route_channels[index]
}

func GetGroupChannel(group_id int64) *Channel {
	if group_id < 0 {
		group_id = -group_id
	}
	index := group_id % int64(len(group_route_channels))
	return group_route_channels[index]
}

func GetRoomChannel(room_id int64) *Channel {
	if room_id < 0 {
		room_id = -room_id
	}
	index := room_id % int64(len(route_channels))
	return route_channels[index]
}

func GetGroupMessageDeliver(group_id int64) *GroupMessageDeliver {
	if group_id < 0 {
		group_id = -group_id
	}

	deliver_index := atomic.AddUint64(&current_deliver_index, 1)
	index := deliver_index % uint64(len(group_message_delivers))
	return group_message_delivers[index]
}

// 群消息通知(apns, gcm...)
func PushGroupMessage(appid int64, group *Group, m *Message) {
	channels := make(map[*Channel][]int64)
	members := group.Members()
	for member := range members {
		//不对自身推送
		if im, ok := m.body.(*IMMessage); ok {
			if im.sender == member {
				continue
			}
		}
		channel := GetChannel(member)
		if _, ok := channels[channel]; !ok {
			channels[channel] = []int64{member}
		} else {
			receivers := channels[channel]
			receivers = append(receivers, member)
			channels[channel] = receivers
		}
	}

	for channel, receivers := range channels {
		channel.Push(appid, receivers, m)
	}
}

// 离线消息推送
func PushMessage(appid int64, uid int64, m *Message) {
	channel := GetChannel(uid)
	channel.Push(appid, []int64{uid}, m)
}

func PublishMessage(appid int64, uid int64, msg *Message) {
	now := time.Now().UnixNano()

	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, msg)
	msg_buf := mbuffer.Bytes()

	amsg := &RouteMessage{appid: appid, receiver: uid, timestamp: now, msg: msg_buf}
	if msg.meta != nil {
		meta := msg.meta.(*Metadata)
		amsg.msgid = meta.sync_key
		amsg.prev_msgid = meta.prev_sync_key
	}
	channel := GetChannel(uid)
	channel.Publish(amsg)
}

func PublishGroupMessage(appid int64, group_id int64, msg *Message) {
	now := time.Now().UnixNano()

	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, msg)
	msg_buf := mbuffer.Bytes()

	amsg := &RouteMessage{appid: appid, receiver: group_id, timestamp: now, msg: msg_buf}
	if msg.meta != nil {
		meta := msg.meta.(*Metadata)
		amsg.msgid = meta.sync_key
		amsg.prev_msgid = meta.prev_sync_key
	}
	channel := GetGroupChannel(group_id)
	channel.PublishGroup(amsg)
}

func SendAppGroupMessage(app_route *AppRoute, appid int64, group *Group, msg *Message) {
	PublishGroupMessage(appid, group.gid, msg)
	DispatchMessageToGroup(app_route, msg, group, appid, nil)
}

func SendAppMessage(app_route *AppRoute, appid int64, uid int64, msg *Message) {
	PublishMessage(appid, uid, msg)
	DispatchMessageToPeer(app_route, msg, uid, appid, nil)
}

func DispatchAppMessage(app_route *AppRoute, amsg *RouteMessage) {
	now := time.Now().UnixNano()
	d := now - amsg.timestamp

	mbuffer := bytes.NewBuffer(amsg.msg)
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		log.Warning("can't dispatch message")
		return
	}

	log.Infof("dispatch app message:%s %d %d", Command(msg.cmd), msg.flag, d)
	if d > int64(time.Second) {
		log.Warning("dispatch app message slow...")
	}

	if amsg.msgid > 0 {
		if (msg.flag & MESSAGE_FLAG_PUSH) == 0 {
			log.Fatal("invalid message flag", msg.flag)
		}
		meta := &Metadata{sync_key: amsg.msgid, prev_sync_key: amsg.prev_msgid}
		msg.meta = meta
	}
	DispatchMessageToPeer(app_route, msg, amsg.receiver, amsg.appid, nil)
}

func DispatchRoomMessage(app_route *AppRoute, amsg *RouteMessage) {
	mbuffer := bytes.NewBuffer(amsg.msg)
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		log.Warning("can't dispatch room message")
		return
	}

	log.Info("dispatch room message", Command(msg.cmd))

	room_id := amsg.receiver
	DispatchMessageToRoom(app_route, msg, room_id, amsg.appid, nil)
}

func DispatchGroupMessage(amsg *RouteMessage) {
	now := time.Now().UnixNano()
	d := now - amsg.timestamp
	mbuffer := bytes.NewBuffer(amsg.msg)
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		log.Warning("can't dispatch room message")
		return
	}
	log.Infof("dispatch group message:%s %d %d", Command(msg.cmd), msg.flag, d)
	if d > int64(time.Second) {
		log.Warning("dispatch group message slow...")
	}

	if amsg.msgid > 0 {
		if (msg.flag & MESSAGE_FLAG_PUSH) == 0 {
			log.Fatal("invalid message flag", msg.flag)
		}
		if (msg.flag & MESSAGE_FLAG_SUPER_GROUP) == 0 {
			log.Fatal("invalid message flag", msg.flag)
		}

		meta := &Metadata{sync_key: amsg.msgid, prev_sync_key: amsg.prev_msgid}
		msg.meta = meta
	}

	deliver := GetGroupMessageDeliver(amsg.receiver)
	deliver.DispatchMessage(msg, amsg.receiver, amsg.appid)
}

func DispatchMessageToGroup(app_route *AppRoute, msg *Message, group *Group, appid int64, client *Client) bool {
	if group == nil {
		return false
	}

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", appid, group.gid, Command(msg.cmd))
		return false
	}

	members := group.Members()
	for member := range members {
		clients := route.FindClientSet(member)
		if len(clients) == 0 {
			continue
		}

		for c, _ := range clients {
			if c == client {
				continue
			}
			c.EnqueueNonBlockMessage(msg)
		}
	}

	return true
}

func DispatchMessageToPeer(app_route *AppRoute, msg *Message, uid int64, appid int64, client *Client) bool {
	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", appid, uid, Command(msg.cmd))
		return false
	}
	clients := route.FindClientSet(uid)
	if len(clients) == 0 {
		return false
	}

	for c, _ := range clients {
		if c == client {
			continue
		}
		c.EnqueueNonBlockMessage(msg)
	}
	return true
}

func DispatchMessageToRoom(app_route *AppRoute, msg *Message, room_id int64, appid int64, client *Client) bool {
	route := app_route.FindOrAddRoute(appid)
	clients := route.FindRoomClientSet(room_id)

	if len(clients) == 0 {
		return false
	}
	for c, _ := range clients {
		if c == client {
			continue
		}
		c.EnqueueNonBlockMessage(msg)
	}
	return true
}
