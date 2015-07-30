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
import log "github.com/golang/glog"

type GroupSubscriber struct {
	ref_count int
	groups    IntSet
}

type ApplicationSubscriber struct {
	appid     int64
	subs      map[int64]*GroupSubscriber
}

type GroupCenter struct {
	mutex     sync.Mutex
	apps      map[int64]*ApplicationSubscriber
}

func NewGroupCenter() *GroupCenter {
	gc := &GroupCenter{}
	gc.apps = make(map[int64]*ApplicationSubscriber)
	return gc
}

func (gc *GroupCenter) SubscribeGroup(appid int64, uid int64) {
	gc.mutex.Lock()

	ids := make([]*AppGroupMemberID, 0)
	if _, ok := gc.apps[appid]; !ok {
		gc.apps[appid] = &ApplicationSubscriber{appid:appid, subs:make(map[int64]*GroupSubscriber)}
	}
	app := gc.apps[appid]
	if sub, ok := app.subs[uid]; ok {
		sub.ref_count++
	} else {
		sub = &GroupSubscriber{ref_count:1, groups:NewIntSet()}
		groups := group_manager.FindUserGroups(appid, uid)
		for _, group := range groups {
			if !group.super {
				continue
			}
			sub.groups.Add(group.gid)

			id := &AppGroupMemberID{appid:appid, gid:group.gid, uid:uid}
			ids = append(ids, id)
		}
		app.subs[uid] = sub
		log.Infof("subscribe group appid:%d uid:%d\n", appid, uid)
	}

	gc.mutex.Unlock()

	for _, id := range ids {
		log.Infof("subscribe group appid:%d gid:%d uid:%d\n", id.appid, id.gid, id.uid)
		sc := GetGroupStorageChannel(id.gid)
		sc.SubscribeGroup(id.appid, id.gid, id.uid)
	}
}

func (gc *GroupCenter) UnsubscribeGroup(appid int64, uid int64) {
	gc.mutex.Lock()
	if _, ok := gc.apps[appid]; !ok {
		gc.mutex.Unlock()
		return
	}

	ids := make([]*AppGroupMemberID, 0)
	app := gc.apps[appid]
	if sub, ok := app.subs[uid]; ok {
		sub.ref_count--
		if sub.ref_count == 0 {
			delete(app.subs, uid)
			for gid := range sub.groups {
				id := &AppGroupMemberID{appid:appid, gid:gid, uid:uid}
				ids = append(ids, id)
			}
			log.Infof("unsubscribe group appid:%d uid:%d\n", appid, uid)
		}
	}
	gc.mutex.Unlock()

	for _, id := range ids {
		log.Infof("unsubscribe group appid:%d gid:%d uid:%d\n", id.appid, id.gid, id.uid)
		sc := GetGroupStorageChannel(id.gid)
		sc.UnSubscribeGroup(id.appid, id.gid, id.uid)
	}
}

func (gc *GroupCenter) SubscribeGroupMember(appid int64, gid int64, uid int64) {
	gc.mutex.Lock()
	if _, ok := gc.apps[appid]; !ok {
		gc.mutex.Unlock()
		return
	}

	var id *AppGroupMemberID
	app := gc.apps[appid]
	if sub, ok := app.subs[uid]; ok {
		if sub.groups.IsMember(gid) {
			gc.mutex.Unlock()
			return
		}

		sub.groups.Add(gid)
		id = &AppGroupMemberID{appid:appid, gid:gid, uid:uid}
		log.Infof("subscribe group member:%d %d %d\n", appid, gid, uid)
	}
	gc.mutex.Unlock()

	if id != nil {
		sc := GetGroupStorageChannel(id.gid)
		sc.SubscribeGroup(id.appid, id.gid, id.uid)
	}
}

func (gc *GroupCenter) UnsubscribeGroupMember(appid int64, gid int64, uid int64) {
	gc.mutex.Lock()
	if _, ok := gc.apps[appid]; !ok {
		gc.mutex.Unlock()
		return
	}

	var id *AppGroupMemberID
	app := gc.apps[appid]
	if sub, ok := app.subs[uid]; ok {
		if !sub.groups.IsMember(gid) {
			gc.mutex.Unlock()
			return
		}
		sub.groups.Remove(gid)
		id = &AppGroupMemberID{appid:appid, gid:gid, uid:uid}
		log.Infof("unsubscribe group member:%d %d %d\n", appid, gid, uid)
	}
	
	gc.mutex.Unlock()

	if id != nil {
		sc := GetGroupStorageChannel(id.gid)
		sc.UnSubscribeGroup(id.appid, id.gid, id.uid)
	}
}


