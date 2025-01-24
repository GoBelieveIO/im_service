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

type App struct {
	app_route *AppRoute

	// route server
	route_channels []*Channel
	// super group route server
	group_route_channels []*Channel

	current_deliver_index  uint64
	group_message_delivers []*GroupMessageDeliver

	group_loaders []*GroupLoader
}

func (app *App) GetChannel(uid int64) *Channel {
	if uid < 0 {
		uid = -uid
	}
	index := uid % int64(len(app.route_channels))
	return app.route_channels[index]
}

func (app *App) GetGroupChannel(group_id int64) *Channel {
	if group_id < 0 {
		group_id = -group_id
	}
	index := group_id % int64(len(app.group_route_channels))
	return app.group_route_channels[index]
}

func (app *App) GetRoomChannel(room_id int64) *Channel {
	if room_id < 0 {
		room_id = -room_id
	}
	index := room_id % int64(len(app.route_channels))
	return app.route_channels[index]
}

func (app *App) GetGroupLoader(group_id int64) *GroupLoader {
	if group_id < 0 {
		group_id = -group_id
	}

	index := uint64(group_id) % uint64(len(app.group_message_delivers))
	return app.group_loaders[index]
}

func (app *App) GetGroupMessageDeliver(group_id int64) *GroupMessageDeliver {
	deliver_index := atomic.AddUint64(&app.current_deliver_index, 1)
	index := deliver_index % uint64(len(app.group_message_delivers))
	return app.group_message_delivers[index]
}

// 群消息通知(apns, gcm...)
func (app *App) PushGroupMessage(appid int64, group *Group, m *Message) {
	channels := make(map[*Channel][]int64)
	members := group.Members()
	for member := range members {
		//不对自身推送
		if im, ok := m.body.(*IMMessage); ok {
			if im.sender == member {
				continue
			}
		}
		channel := app.GetChannel(member)
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
func (app *App) PushMessage(appid int64, uid int64, m *Message) {
	channel := app.GetChannel(uid)
	channel.Push(appid, []int64{uid}, m)
}

func (app *App) SendAnonymousGroupMessage(appid int64, group *Group, msg *Message) {
	app.SendGroupMessage(appid, group, msg, nil)
}

func (app *App) SendAnonymousMessage(appid int64, uid int64, msg *Message) {
	app.SendMessage(appid, uid, msg, nil)
}

func (app *App) SendAnonymousRoomMessage(appid int64, room_id int64, msg *Message) {
	app.SendRoomMessage(appid, room_id, msg, nil)
}

func (app *App) SendGroupMessage(appid int64, group *Group, msg *Message, sender *Sender) {
	app.PublishGroupMessage(appid, group.gid, msg)
	var sender_id int64
	var device_ID int64 = INVALID_DEVICE_ID
	if sender != nil {
		sender_id = sender.uid
		device_ID = sender.deviceID
	}

	app.app_route.sendGroupMessage(appid, sender_id, device_ID, group, msg)
}

func (app *App) SendMessage(appid int64, uid int64, msg *Message, sender *Sender) {
	app.PublishMessage(appid, uid, msg)
	var sender_appid int64
	var sender_id int64
	var device_ID int64 = INVALID_DEVICE_ID
	if sender != nil {
		sender_appid = sender.appid
		sender_id = sender.uid
		device_ID = sender.deviceID
	}

	app.app_route.sendPeerMessage(sender_appid, sender_id, device_ID, appid, uid, msg)
}

func (app *App) SendRoomMessage(appid int64, room_id int64, msg *Message, sender *Sender) {
	app.PublishRoomMessage(appid, room_id, msg)
	var sender_id int64
	var device_ID int64 = INVALID_DEVICE_ID
	if sender != nil {
		sender_id = sender.uid
		device_ID = sender.deviceID
	}
	app.app_route.sendRoomMessage(appid, sender_id, device_ID, room_id, msg)
}

func (app *App) PublishMessage(appid int64, uid int64, msg *Message) {
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
	channel := app.GetChannel(uid)
	channel.Publish(amsg)
}

func (app *App) PublishGroupMessage(appid int64, group_id int64, msg *Message) {
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
	channel := app.GetGroupChannel(group_id)
	channel.PublishGroup(amsg)
}

func (app *App) PublishRoomMessage(appid int64, room_id int64, m *Message) {
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, m)
	msg_buf := mbuffer.Bytes()
	amsg := &RouteMessage{appid: appid, receiver: room_id, msg: msg_buf}
	channel := app.GetRoomChannel(room_id)
	channel.PublishRoom(amsg)
}

func DispatchMessage(app_route *AppRoute, amsg *RouteMessage) {
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
	app_route.SendPeerMessage(amsg.appid, amsg.receiver, msg)
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
	app_route.SendRoomMessage(amsg.appid, room_id, msg)
}

func DispatchGroupMessage(app *App, amsg *RouteMessage) {
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

	loader := app.GetGroupLoader(amsg.receiver)
	loader.DispatchMessage(msg, amsg.receiver, amsg.appid)
}
