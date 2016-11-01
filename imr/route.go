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
import "sync"

type Route struct {
	appid     int64
	mutex     sync.Mutex
	uids      IntSet
	room_ids  IntSet
}

func NewRoute(appid int64) *Route {
	r := new(Route)
	r.appid = appid
	r.uids = NewIntSet()
	r.room_ids = NewIntSet()
	return r
}


func (route *Route) IsIntersect(s IntSet) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	
	for uid := range(route.uids) {
		if s.IsMember(uid) {
			return true
		}
	}
	return false
}

func (route *Route) ContainUserID(uid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	
	return route.uids.IsMember(uid)
}

func (route *Route) AddUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids.Add(uid)
}

func (route *Route) RemoveUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids.Remove(uid)
}

func (route *Route) ContainRoomID(room_id int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	
	return route.room_ids.IsMember(room_id)
}

func (route *Route) AddRoomID(room_id int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.room_ids.Add(room_id)
}

func (route *Route) RemoveRoomID(room_id int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.room_ids.Remove(room_id)
}
