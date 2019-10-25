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
import "strconv"
import "strings"
import "time"
import "errors"
import "github.com/gomodule/redigo/redis"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"


//同redis的长链接保持5minute的心跳
const SUBSCRIBE_HEATBEAT = 5*60
const GROUP_EXPIRE_DURATION = 60*60 //60min
const GROUP_MANAGER_STREAM_NAME = "group_manager_stream"
const GROUP_MANAGER_XREAD_TIMEOUT = 60

//group event
const GROUP_EVENT_CREATE = "group_create"
const GROUP_EVENT_DISBAND = "group_disband"
const GROUP_EVENT_UPGRADE = "group_upgrade"
const GROUP_EVENT_MEMBER_ADD = "group_member_add"
const GROUP_EVENT_MEMBER_REMOVE = "group_member_remove"
const GROUP_EVENT_MEMBER_MUTE = "group_member_mute"


type GroupEvent struct {
	Id string //stream entry id
	ActionId int64 `redis:"action_id"`
	PreviousActionId int64 `redis:"previous_action_id"`
	Name string `redis:"name"`
	AppId int64 `redis:"app_id"`
	GroupId int64 `redis:"group_id"`
	MemberId int64 `redis:"member_id"`
	IsSuper bool `redis:"super"`
	IsMute bool `redis:"mute"`
}


type GroupManager struct {
	mutex  sync.Mutex
	groups map[int64]*Group
	action_id int64
	last_entry_id string
	dirty     bool
	db        *sql.DB
}

func NewGroupManager() *GroupManager {
	m := new(GroupManager)
	m.groups = make(map[int64]*Group)
	m.action_id = 0
	m.last_entry_id = ""
	m.dirty = true
	
	db, err := sql.Open("mysql", config.mysqldb_datasource)
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
		log.Warning("load group:%d err:%s", gid, err)
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


func (group_manager *GroupManager) handleEvent(event *GroupEvent) {
	prev_id := event.PreviousActionId
	action_id := event.ActionId
	log.Infof("group action:%+v %d ", event, group_manager.action_id)

	if group_manager.action_id != prev_id {
		//reload later
		group_manager.dirty = true
		log.Warning("action nonsequence:", group_manager.action_id, prev_id, action_id)
	}

	if event.Name == GROUP_EVENT_CREATE {
		group_manager.HandleCreate(event)
	} else if event.Name == GROUP_EVENT_DISBAND {
		group_manager.HandleDisband(event)
	} else if event.Name == GROUP_EVENT_UPGRADE {
		group_manager.HandleUpgrade(event)		
	} else if event.Name == GROUP_EVENT_MEMBER_ADD {
		group_manager.HandleMemberAdd(event)
	} else if event.Name == GROUP_EVENT_MEMBER_REMOVE {
		group_manager.HandleMemberRemove(event)
	} else if event.Name == GROUP_EVENT_MEMBER_MUTE {
		group_manager.HandleMute(event)
	} else {
		log.Warning("unknow event:", event.Name)
	}
	group_manager.action_id = action_id
	group_manager.last_entry_id = event.Id
}



func (group_manager *GroupManager) getActionID() (int64, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	actions, err := redis.String(conn.Do("GET", "groups_actions"))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0, err
	}
	if actions == "" {
		return 0, nil
	} else {
		arr := strings.Split(actions, ":")
		if len(arr) != 2 {
			log.Error("groups_actions invalid:", actions)
			return 0, errors.New("groups actions invalid")
		}
		_, err := strconv.ParseInt(arr[0], 10, 64)
		if err != nil {
			log.Info("error:", err, actions)
			return 0, err
		}

		action_id, err := strconv.ParseInt(arr[1], 10, 64)
		if err != nil {
			log.Info("error:", err, actions)
			return 0, err			
		}
		return action_id, nil
	}
}



func (group_manager *GroupManager) getLastEntryID() (string, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	r, err := redis.Values(conn.Do("XREVRANGE", GROUP_MANAGER_STREAM_NAME, "+", "-", "COUNT", "1"))

	if err != nil {
		log.Error("redis err:", err)
		return "", err
	}

	if len(r) == 0 {
		return "0-0", nil
	}
	
	var entries []interface{}
	r, err = redis.Scan(r, &entries)
	if err != nil {
		log.Error("redis scan err:", err)
		return "", err
	}

	var id string		
	_, err = redis.Scan(entries, &id, nil)
	if err != nil {
		log.Error("redis scan err:", err)
		return "", err		
	}
	return id, nil
}


func (group_manager *GroupManager) load() {
	//循环直到成功
	for {
		action_id, err := group_manager.getActionID()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		entry_id, err := group_manager.getLastEntryID()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		group_manager.last_entry_id = entry_id
		group_manager.action_id = action_id
		group_manager.dirty = false
		log.Info("group action id:", action_id)
		break
	}
}


func (group_manager *GroupManager) readEvents(c redis.Conn) ([]*GroupEvent, error) {
	//block timeout 60s
	reply, err := redis.Values(c.Do("XREAD", "COUNT", "1000", "BLOCK",
		GROUP_MANAGER_XREAD_TIMEOUT*1000, "STREAMS", GROUP_MANAGER_STREAM_NAME,
		group_manager.last_entry_id))
	if err != nil && err != redis.ErrNil {
		log.Info("redis xread err:", err)
		return nil, err
	}
	if len(reply) == 0 {
		log.Info("redis xread timeout")
		return nil, nil
	}

	var stream_res []interface{}
	_, err = redis.Scan(reply, &stream_res)
	if err != nil {
		log.Info("redis scan err:", err)
		return nil, err
	}

	var r []interface{}
	_, err = redis.Scan(stream_res, nil, &r)
	if err != nil {
		log.Info("redis scan err:", err)
		return nil, err
	}
	
	events := make([]*GroupEvent, 0, 1000)
	for len(r) > 0 {
		var entries []interface{}
		r, err = redis.Scan(r, &entries)
		if err != nil {
			log.Error("redis scan err:", err)
			return nil, err
		}

		var id string
		var fields []interface{}
		_, err = redis.Scan(entries, &id, &fields)
		if err != nil {
			log.Error("redis scan err:", err)
			return nil, err		
		}

		event := &GroupEvent{}
		event.Id = id
		err = redis.ScanStruct(fields, event)
		if err != nil {
			//ignore the error, will skip the event
			log.Error("redis scan err:", err)
		}
		events = append(events, event)
	}
	
	return events, nil
}

func (group_manager *GroupManager) RunOnce() bool {
	c, err := redis.Dial("tcp", config.redis_address)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}

	defer c.Close()
	
	password := config.redis_password
	if len(password) > 0 {
		if _, err := c.Do("AUTH", password); err != nil {
			log.Info("redis auth err:", err)
			return false
		}
	}

	db := config.redis_db
	if db > 0 && db < 16 {
		if _, err := c.Do("SELECT", db); err != nil {
			log.Info("redis select err:", err)
			return false
		}
	}

	var last_clear_ts int64
	for {
		events, err := group_manager.readEvents(c)
		if err != nil {
			log.Warning("group manager read event err:", err)
			break
		}
		
		for _, event := range events {
			group_manager.handleEvent(event)
		}

		now := time.Now().Unix()
		//lazy clear policy
		if group_manager.dirty && now - last_clear_ts >= GROUP_MANAGER_XREAD_TIMEOUT {
			log.Info("clear group manager")
			group_manager.clear()
			group_manager.dirty = false
			last_clear_ts = now
		}
	}
	return true
}

func (group_manager *GroupManager) Run() {
	nsleep := 1
	for {
		connected := group_manager.RunOnce()
		if !connected {
			nsleep *= 2
			if nsleep > 60 {
				nsleep = 60
			}
		} else {
			nsleep = 1
		}
		time.Sleep(time.Duration(nsleep) * time.Second)
	}
}


func (group_manager *GroupManager) Recycle() {
	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()

	begin := time.Now()
	
	now := int(time.Now().Unix())
	for k := range group_manager.groups {
		group := group_manager.groups[k]
		if now - group.ts > GROUP_EXPIRE_DURATION {
			//map can delete item when iterate
			delete(group_manager.groups, k)
		}
	}

	end := time.Now()

	log.Info("group manager recyle, time:", end.Sub(begin))
}


func (group_manager *GroupManager) RecycleLoop() {
	//5 min
	ticker := time.NewTicker(time.Second * 60 * 5)
	for range ticker.C {
		group_manager.Recycle()
	}	
}

func (group_manager *GroupManager) Start() {
	group_manager.load()
	go group_manager.Run()
	go group_manager.RecycleLoop()
}
