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
	"database/sql"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	log "github.com/sirupsen/logrus"
)

const GROUP_EXPIRE_DURATION = 60 * 60 //60min
const GROUP_MANAGER_STREAM_NAME = "group_manager_stream"
const GROUP_MANAGER_XREAD_TIMEOUT = 60

// group event
const GROUP_EVENT_CREATE = "group_create"
const GROUP_EVENT_DISBAND = "group_disband"
const GROUP_EVENT_UPGRADE = "group_upgrade"
const GROUP_EVENT_CHANGED = "group_changed"
const GROUP_EVENT_MEMBER_ADD = "group_member_add"
const GROUP_EVENT_MEMBER_REMOVE = "group_member_remove"
const GROUP_EVENT_MEMBER_MUTE = "group_member_mute"

type GroupEvent struct {
	Id               string //stream entry id
	ActionId         int64  `redis:"action_id"`
	PreviousActionId int64  `redis:"previous_action_id"`
	Name             string `redis:"name"`
	AppId            int64  `redis:"app_id"`
	GroupId          int64  `redis:"group_id"`
	MemberId         int64  `redis:"member_id"`
	IsSuper          bool   `redis:"super"`
	IsMute           bool   `redis:"mute"`
}

type GroupManager struct {
	mutex  sync.Mutex
	groups map[int64]*Group

	db *sql.DB
}

func NewGroupManager(mysqldb_datasource string) *GroupManager {
	m := new(GroupManager)
	m.groups = make(map[int64]*Group)

	db, err := sql.Open("mysql", mysqldb_datasource)
	if err != nil {
		log.Fatal("open db:", err)
	}
	m.db = db

	return m
}

func (group_manager *GroupManager) LoadGroup(gid int64) *Group {
	group_manager.mutex.Lock()
	if group, ok := group_manager.groups[gid]; ok {
		now := int(time.Now().Unix())
		group.ts = now
		group_manager.mutex.Unlock()
		return group
	}
	group_manager.mutex.Unlock()
	group, err := LoadGroup(group_manager.db, gid)
	if err != nil {
		log.Warningf("load group:%d err:%s", gid, err)
		return nil
	}

	group_manager.mutex.Lock()
	group_manager.groups[gid] = group
	group_manager.mutex.Unlock()
	return group
}

func (group_manager *GroupManager) FindGroup(gid int64) *Group {
	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()
	if group, ok := group_manager.groups[gid]; ok {
		now := int(time.Now().Unix())
		group.ts = now
		return group
	}
	return nil
}

func (group_manager *GroupManager) clear() {
	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()

	group_manager.groups = make(map[int64]*Group)
}

func (group_manager *GroupManager) HandleCreate(event *GroupEvent) {
	gid := event.GroupId
	appid := event.AppId
	super := event.IsSuper

	if gid == 0 || appid == 0 {
		log.Infof("invalid group event:%s, group id:%d appid:%d",
			event.Name, gid, appid)
		return
	}

	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()

	if _, ok := group_manager.groups[gid]; ok {
		log.Infof("group:%d exists\n", gid)
	}
	log.Infof("create group:%d appid:%d", gid, appid)
	if super {
		group_manager.groups[gid] = NewSuperGroup(gid, appid, nil)
	} else {
		group_manager.groups[gid] = NewGroup(gid, appid, nil)
	}
}

func (group_manager *GroupManager) HandleDisband(event *GroupEvent) {
	gid := event.GroupId
	if gid == 0 {
		log.Infof("invalid group event:%s, group id:%d", event.Name, gid)
		return
	}

	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()
	if _, ok := group_manager.groups[gid]; ok {
		log.Info("disband group:", gid)
		delete(group_manager.groups, gid)
	} else {
		log.Infof("group:%d nonexists\n", gid)
	}
}

func (group_manager *GroupManager) HandleUpgrade(event *GroupEvent) {
	gid := event.GroupId
	is_super := event.IsSuper

	if gid == 0 {
		log.Infof("invalid group event:%s, group id:%d",
			event.Name, gid)
		return
	}

	if !is_super {
		log.Warning("super group can't transfer to nomal group")
		return
	}
	group := group_manager.FindGroup(gid)
	if group != nil {
		group.super = is_super
		log.Infof("upgrade group gid:%d super:%t",
			gid, is_super)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (group_manager *GroupManager) HandleChanged(event *GroupEvent) {
	gid := event.GroupId
	if gid == 0 {
		log.Infof("invalid group event:%s, group id:%d",
			event.Name, gid)
		return
	}
	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()
	if _, ok := group_manager.groups[gid]; ok {
		log.Info("group changed, delete group:", gid)
		delete(group_manager.groups, gid)
	} else {
		log.Infof("group:%d nonexists\n", gid)
	}
}

func (group_manager *GroupManager) HandleMemberAdd(event *GroupEvent) {
	gid := event.GroupId
	uid := event.MemberId

	if gid == 0 || uid == 0 {
		log.Infof("invalid group event:%s, group id:%d member id:%d",
			event.Name, gid, uid)
		return
	}

	group := group_manager.FindGroup(gid)
	if group != nil {
		timestamp := int(time.Now().Unix())
		group.AddMember(uid, timestamp)
		log.Infof("add group member gid:%d uid:%d", gid, uid)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (group_manager *GroupManager) HandleMemberRemove(event *GroupEvent) {
	gid := event.GroupId
	uid := event.MemberId
	if gid == 0 || uid == 0 {
		log.Infof("invalid group event:%s, group id:%d member id:%d",
			event.Name, gid, uid)
		return
	}

	group := group_manager.FindGroup(gid)
	if group != nil {
		group.RemoveMember(uid)
		log.Infof("remove group member gid:%d uid:%d", gid, uid)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (group_manager *GroupManager) HandleMute(event *GroupEvent) {
	gid := event.GroupId
	uid := event.MemberId
	is_mute := event.IsMute

	if gid == 0 || uid == 0 {
		log.Infof("invalid group event:%s, group id:%d member id:%d",
			event.Name, gid, uid)
		return
	}

	group := group_manager.FindGroup(gid)
	if group != nil {
		group.SetMemberMute(uid, is_mute)
		log.Infof("set group member gid:%d uid:%d mute:%t", gid, uid, is_mute)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (group_manager *GroupManager) Recycle() {
	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()

	begin := time.Now()

	now := int(time.Now().Unix())
	for k := range group_manager.groups {
		group := group_manager.groups[k]
		if now-group.ts > GROUP_EXPIRE_DURATION {
			//map can delete item when iterate
			delete(group_manager.groups, k)
		}
	}

	end := time.Now()

	log.Info("group manager recyle, time:", end.Sub(begin))
}
