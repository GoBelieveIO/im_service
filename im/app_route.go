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
	"sync"
	"time"

	"github.com/GoBelieveIO/im_service/set"

	log "github.com/sirupsen/logrus"
)

const INVALID_DEVICE_ID = -1

type Sender struct {
	appid    int64
	uid      int64
	deviceID int64
}
type AppRoute struct {
	mutex sync.Mutex
	apps  map[int64]*Route

	// route server
	route_channels []*Channel
	// super group route server
	group_route_channels []*Channel
}

func NewAppRoute() *AppRoute {
	app_route := new(AppRoute)
	app_route.apps = make(map[int64]*Route)
	return app_route
}

func (app_route *AppRoute) FindOrAddRoute(appid int64) *Route {
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	if route, ok := app_route.apps[appid]; ok {
		return route
	}
	route := NewRoute(appid)
	app_route.apps[appid] = route
	return route
}

func (app_route *AppRoute) FindRoute(appid int64) *Route {
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	return app_route.apps[appid]
}

func (app_route *AppRoute) AddRoute(route *Route) {
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	app_route.apps[route.appid] = route
}

func (app_route *AppRoute) GetUsers() map[int64]set.IntSet {
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()

	r := make(map[int64]set.IntSet)
	for appid, route := range app_route.apps {
		uids := route.GetUserIDs()
		r[appid] = uids
	}
	return r
}

func (app_route *AppRoute) GetChannel(uid int64) *Channel {
	if uid < 0 {
		uid = -uid
	}
	index := uid % int64(len(app_route.route_channels))
	return app_route.route_channels[index]
}

func (app_route *AppRoute) GetGroupChannel(group_id int64) *Channel {
	if group_id < 0 {
		group_id = -group_id
	}
	index := group_id % int64(len(app_route.group_route_channels))
	return app_route.group_route_channels[index]
}

func (app_route *AppRoute) GetRoomChannel(room_id int64) *Channel {
	if room_id < 0 {
		room_id = -room_id
	}
	index := room_id % int64(len(app_route.route_channels))
	return app_route.route_channels[index]
}

// 群消息通知(apns, gcm...)
func (app_route *AppRoute) PushGroupMessage(appid int64, group *Group, m *Message) {
	channels := make(map[*Channel][]int64)
	members := group.Members()
	for member := range members {
		//不对自身推送
		if im, ok := m.body.(*IMMessage); ok {
			if im.sender == member {
				continue
			}
		}
		channel := app_route.GetChannel(member)
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
func (app_route *AppRoute) PushMessage(appid int64, uid int64, m *Message) {
	channel := app_route.GetChannel(uid)
	channel.Push(appid, []int64{uid}, m)
}

func (app_route *AppRoute) SendAnonymousGroupMessage(appid int64, group *Group, msg *Message) {
	app_route.SendGroupMessage(appid, group, msg, nil)
}

func (app_route *AppRoute) SendAnonymousMessage(appid int64, uid int64, msg *Message) {
	app_route.SendMessage(appid, uid, msg, nil)
}

func (app_route *AppRoute) SendAnonymousRoomMessage(appid int64, room_id int64, msg *Message) {
	app_route.SendRoomMessage(appid, room_id, msg, nil)
}

func (app_route *AppRoute) SendGroupMessage(appid int64, group *Group, msg *Message, sender *Sender) {
	app_route.PublishGroupMessage(appid, group.gid, msg)
	var sender_id int64
	var device_ID int64 = INVALID_DEVICE_ID
	if sender != nil {
		sender_id = sender.uid
		device_ID = sender.deviceID
	}

	app_route.dispatchMessageToGroup(appid, sender_id, device_ID, group, msg)
}

func (app_route *AppRoute) SendMessage(appid int64, uid int64, msg *Message, sender *Sender) {
	app_route.PublishMessage(appid, uid, msg)
	var sender_appid int64
	var sender_id int64
	var device_ID int64 = INVALID_DEVICE_ID
	if sender != nil {
		sender_appid = sender.appid
		sender_id = sender.uid
		device_ID = sender.deviceID
	}

	app_route.dispatchMessageToPeer(sender_appid, sender_id, device_ID, appid, uid, msg)
}

func (app_route *AppRoute) SendRoomMessage(appid int64, room_id int64, msg *Message, sender *Sender) {
	app_route.PublishRoomMessage(appid, room_id, msg)
	var sender_id int64
	var device_ID int64 = INVALID_DEVICE_ID
	if sender != nil {
		sender_id = sender.uid
		device_ID = sender.deviceID
	}
	app_route.dispatchMessageToRoom(appid, sender_id, device_ID, room_id, msg)
}

// func (app_route *AppRoute) SendAppMessage(appid int64, uid int64, msg *Message, sender *Sender) {
// 	app_route.PublishMessage(appid, uid, msg)
// 	app_route.dispatchAppMessageToPeer(sender_appid, sender, device_ID, appid, uid, msg)
// }

func (app_route *AppRoute) PublishMessage(appid int64, uid int64, msg *Message) {
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
	channel := app_route.GetChannel(uid)
	channel.Publish(amsg)
}

func (app_route *AppRoute) PublishGroupMessage(appid int64, group_id int64, msg *Message) {
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
	channel := app_route.GetGroupChannel(group_id)
	channel.PublishGroup(amsg)
}

func (app_route *AppRoute) PublishRoomMessage(appid int64, room_id int64, m *Message) {
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, m)
	msg_buf := mbuffer.Bytes()
	amsg := &RouteMessage{appid: appid, receiver: room_id, msg: msg_buf}
	channel := app_route.GetRoomChannel(room_id)
	channel.PublishRoom(amsg)
}

func (app_route *AppRoute) DispatchMessageToGroup(appid int64, group *Group, msg *Message) bool {
	return app_route.dispatchMessageToGroup(appid, 0, INVALID_DEVICE_ID, group, msg)
}

func (app_route *AppRoute) dispatchMessageToGroup(appid int64, sender int64, device_ID int64, group *Group, msg *Message) bool {
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
			//不再发送给自己
			if c.device_ID == device_ID && sender == c.uid {
				continue
			}
			c.EnqueueNonBlockMessage(msg)
		}
	}

	return true
}

func (app_route *AppRoute) dispatchMessageToPeer(sender_appid int64, sender int64, device_ID int64, appid int64, uid int64, msg *Message) bool {
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
		//不再发送给自己
		if c.device_ID == device_ID && sender == c.uid && sender_appid == c.appid {
			continue
		}
		c.EnqueueNonBlockMessage(msg)
	}
	return true
}

func (app_route *AppRoute) DispatchMessageToPeer(appid int64, uid int64, msg *Message) bool {
	return app_route.dispatchMessageToPeer(appid, 0, INVALID_DEVICE_ID, appid, uid, msg)
}

func (app_route *AppRoute) dispatchMessageToRoom(appid int64, sender int64, device_ID int64, room_id int64, msg *Message) bool {
	route := app_route.FindOrAddRoute(appid)
	clients := route.FindRoomClientSet(room_id)

	if len(clients) == 0 {
		return false
	}
	for c, _ := range clients {
		//不再发送给自己
		if c.device_ID == device_ID && sender == c.uid {
			continue
		}
		c.EnqueueNonBlockMessage(msg)
	}
	return true
}

func (app_route *AppRoute) DispatchMessageToRoom(appid int64, room_id int64, msg *Message) bool {
	return app_route.dispatchMessageToRoom(appid, 0, INVALID_DEVICE_ID, room_id, msg)
}
