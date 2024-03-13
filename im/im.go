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

func GetGroupLoader(group_id int64) *GroupLoader {
	if group_id < 0 {
		group_id = -group_id
	}

	index := uint64(group_id) % uint64(len(group_message_delivers))
	return group_loaders[index]
}

func GetGroupMessageDeliver(group_id int64) *GroupMessageDeliver {
	deliver_index := atomic.AddUint64(&current_deliver_index, 1)
	index := deliver_index % uint64(len(group_message_delivers))
	return group_message_delivers[index]
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
	app_route.DispatchMessageToPeer(amsg.appid, amsg.receiver, msg)
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
	app_route.DispatchMessageToRoom(amsg.appid, room_id, msg)
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

	loader := GetGroupLoader(amsg.receiver)
	loader.DispatchMessage(msg, amsg.receiver, amsg.appid)
}
