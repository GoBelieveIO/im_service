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
import "fmt"
import "math/rand"
import "github.com/gomodule/redigo/redis"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"

//同redis的长链接保持5minute的心跳
const SUBSCRIBE_HEATBEAT = 5*60
const GROUP_EXPIRE_DURATION = 60*60 //60min

type GroupManager struct {
	mutex  sync.Mutex
	groups map[int64]*Group
	ping     string
	action_id int64
	dirty     bool
	db        *sql.DB
}

func NewGroupManager() *GroupManager {
	now := time.Now().Unix()
	r := fmt.Sprintf("ping_%d", now)
	for i := 0; i < 4; i++ {
		n := rand.Int31n(26)
		r = r + string('a' + n)
	}
	
	m := new(GroupManager)
	m.groups = make(map[int64]*Group)
	m.ping = r
	m.action_id = 0
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

func (group_manager *GroupManager) HandleCreate(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	appid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	super, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	group_manager.mutex.Lock()
	defer group_manager.mutex.Unlock()

	if _, ok := group_manager.groups[gid]; ok {
		log.Infof("group:%d exists\n", gid)
	}
	log.Infof("create group:%d appid:%d", gid, appid)
	if super != 0 {
		group_manager.groups[gid] = NewSuperGroup(gid, appid, nil)
	} else {
		group_manager.groups[gid] = NewGroup(gid, appid, nil)
	}
}

func (group_manager *GroupManager) HandleDisband(data string) {
	gid, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		log.Info("error:", err)
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

func (group_manager *GroupManager) HandleUpgrade(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	appid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	super, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	if super == 0 {
		log.Warning("super group can't transfer to nomal group")
		return
	}
	group := group_manager.FindGroup(gid)
	if group != nil {
		group.super = (super == 1)
		log.Infof("upgrade group appid:%d gid:%d super:%d", appid, gid, super)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}


func (group_manager *GroupManager) HandleMemberAdd(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
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

func (group_manager *GroupManager) HandleMemberRemove(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
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

func (group_manager *GroupManager) HandleMute(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	mute, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	group := group_manager.FindGroup(gid)
	if group != nil {
		group.SetMemberMute(uid, mute != 0)
		log.Infof("set group member gid:%d uid:%d mute:%d", gid, uid, mute)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

//保证action id的顺序性
func (group_manager *GroupManager) parseAction(data string) (bool, int64, int64, string) {
	arr := strings.SplitN(data, ":", 3)
	if len(arr) != 3 {
		log.Warning("group action error:", data)
		return false, 0, 0, ""
	}

	prev_id, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err, data)
		return false, 0, 0, ""
	}

	action_id, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err, data)
		return false, 0, 0, ""
	}
	return true, prev_id, action_id, arr[2]
}

func (group_manager *GroupManager) handleAction(data string, channel string) {
	r, prev_id, action_id, content := group_manager.parseAction(data)
	if r {
		log.Info("group action:", prev_id, action_id, group_manager.action_id, " ", channel)
		if group_manager.action_id != prev_id {
			//reload later
			group_manager.dirty = true
			log.Warning("action nonsequence:", group_manager.action_id, prev_id, action_id)
		}

		if channel == "group_create" {
			group_manager.HandleCreate(content)
		} else if channel == "group_disband" {
			group_manager.HandleDisband(content)
		} else if channel == "group_member_add" {
			group_manager.HandleMemberAdd(content)
		} else if channel == "group_member_remove" {
			group_manager.HandleMemberRemove(content)
		} else if channel == "group_upgrade" {
			group_manager.HandleUpgrade(content)
		} else if channel == "group_member_mute" {
			group_manager.HandleMute(content)
		}
		group_manager.action_id = action_id
	}	
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

func (group_manager *GroupManager) load() {
	//循环直到成功
	for {
		action_id, err := group_manager.getActionID()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		group_manager.action_id = action_id
		group_manager.dirty = false
		log.Info("group action id:", action_id)
		break
	}
}

//检查当前的action id 是否变更，变更时则重新加载群组结构
func (group_manager *GroupManager) checkActionID() {
	action_id, err := group_manager.getActionID()
	if err != nil {
		log.Info("check action err:", err)
		return
	}

	log.Infof("check action id:%d %d", action_id, group_manager.action_id)
	if action_id != group_manager.action_id {
		log.Info("clear group manager")
		group_manager.clear()
		group_manager.action_id = action_id
		group_manager.dirty = false
	}
}

func (group_manager *GroupManager) RunOnce() bool {
	t := redis.DialReadTimeout(time.Second*SUBSCRIBE_HEATBEAT)
	c, err := redis.Dial("tcp", config.redis_address, t)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}

	password := config.redis_password
	if len(password) > 0 {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return false
		}
	}

	psc := redis.PubSubConn{c}
	psc.Subscribe("group_create", "group_disband", "group_member_add",
		"group_member_remove", "group_upgrade", "group_member_mute", group_manager.ping)
	
	group_manager.checkActionID()
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if v.Channel == "group_create" ||
				v.Channel == "group_disband" ||
				v.Channel == "group_member_add"	||
				v.Channel == "group_member_remove" ||
				v.Channel == "group_upgrade" ||
				v.Channel == "group_member_mute" {
				group_manager.handleAction(string(v.Data), v.Channel)
			} else if v.Channel == group_manager.ping {
				group_manager.checkActionID()
			} else {
				log.Infof("%s: message: %s\n", v.Channel, v.Data)
			}
		case redis.Subscription:
			log.Infof("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			log.Info("error:", v)
			return true
		}
	}
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

func (group_manager *GroupManager) Ping() {
	conn := redis_pool.Get()
	defer conn.Close()

	_, err := conn.Do("PUBLISH", group_manager.ping, "ping")
	if err != nil {
		log.Info("ping error:", err)
	}
}


func (group_manager *GroupManager) PingLoop() {
	for {
		group_manager.Ping()
		time.Sleep(time.Second*(SUBSCRIBE_HEATBEAT-10))
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
	go group_manager.PingLoop()
	go group_manager.RecycleLoop()
}
