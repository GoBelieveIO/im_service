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

package server

import (
	"sync"

	"github.com/GoBelieveIO/im_service/set"

	log "github.com/sirupsen/logrus"

	"github.com/GoBelieveIO/im_service/protocol"
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

func (app_route *AppRoute) SendGroupMessage(appid int64, group *Group, msg *protocol.Message) bool {
	return app_route.sendGroupMessage(appid, 0, INVALID_DEVICE_ID, group, msg)
}

func (app_route *AppRoute) sendGroupMessage(appid int64, sender int64, device_ID int64, group *Group, msg *protocol.Message) bool {
	if group == nil {
		return false
	}

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", appid, group.gid, protocol.Command(msg.Cmd))
		return false
	}

	members := group.Members()
	for member := range members {
		clients := route.FindClientSet(member)
		if len(clients) == 0 {
			continue
		}

		for c := range clients {
			//不再发送给自己
			if c.device_ID == device_ID && sender == c.uid {
				continue
			}
			c.EnqueueNonBlockMessage(msg)
		}
	}

	return true
}

func (app_route *AppRoute) sendPeerMessage(sender_appid int64, sender int64, device_ID int64, appid int64, uid int64, msg *protocol.Message) bool {
	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", appid, uid, protocol.Command(msg.Cmd))
		return false
	}
	clients := route.FindClientSet(uid)
	if len(clients) == 0 {
		return false
	}

	for c := range clients {
		//不再发送给自己
		if c.device_ID == device_ID && sender == c.uid && sender_appid == c.appid {
			continue
		}
		c.EnqueueNonBlockMessage(msg)
	}
	return true
}

func (app_route *AppRoute) SendPeerMessage(appid int64, uid int64, msg *protocol.Message) bool {
	return app_route.sendPeerMessage(appid, 0, INVALID_DEVICE_ID, appid, uid, msg)
}

func (app_route *AppRoute) sendRoomMessage(appid int64, sender int64, device_ID int64, room_id int64, msg *protocol.Message) bool {
	route := app_route.FindOrAddRoute(appid)
	clients := route.FindRoomClientSet(room_id)

	if len(clients) == 0 {
		return false
	}
	for c := range clients {
		//不再发送给自己
		if c.device_ID == device_ID && sender == c.uid {
			continue
		}
		c.EnqueueNonBlockMessage(msg)
	}
	return true
}

func (app_route *AppRoute) SendRoomMessage(appid int64, room_id int64, msg *protocol.Message) bool {
	return app_route.sendRoomMessage(appid, 0, INVALID_DEVICE_ID, room_id, msg)
}
