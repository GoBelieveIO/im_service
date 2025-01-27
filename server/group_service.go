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
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"

	_ "github.com/go-sql-driver/mysql"

	log "github.com/sirupsen/logrus"
)

type GroupService struct {
	*GroupManager
	redis_pool   *redis.Pool
	redis_config *RedisConfig

	action_id     int64
	last_entry_id string
	dirty         bool
}

func NewGroupService(redis_pool *redis.Pool, mysqldb_datasource string, redis_config *RedisConfig) *GroupService {
	gm := NewGroupManager(mysqldb_datasource)
	m := &GroupService{GroupManager: gm}
	m.action_id = 0
	m.last_entry_id = ""
	m.dirty = true
	m.redis_pool = redis_pool
	m.redis_config = redis_config
	return m
}

func (group_manager *GroupService) handleEvent(event *GroupEvent) {
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
	} else if event.Name == GROUP_EVENT_CHANGED {
		group_manager.HandleChanged(event)
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

func (group_manager *GroupService) getActionID() (int64, error) {
	conn := group_manager.redis_pool.Get()
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

func (group_manager *GroupService) getLastEntryID() (string, error) {
	conn := group_manager.redis_pool.Get()
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
	_, err = redis.Scan(r, &entries)
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

func (group_manager *GroupService) load() {
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

func (group_manager *GroupService) readEvents(c redis.Conn) ([]*GroupEvent, error) {
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

func (group_manager *GroupService) RunOnce() bool {
	c, err := redis.Dial("tcp", group_manager.redis_config.redis_address)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}

	defer c.Close()

	password := group_manager.redis_config.redis_password
	if len(password) > 0 {
		if _, err := c.Do("AUTH", password); err != nil {
			log.Info("redis auth err:", err)
			return false
		}
	}

	db := group_manager.redis_config.redis_db
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
		if group_manager.dirty && now-last_clear_ts >= GROUP_MANAGER_XREAD_TIMEOUT {
			log.Info("clear group manager")
			group_manager.clear()
			group_manager.dirty = false
			last_clear_ts = now
		}
	}
	return true
}

func (group_manager *GroupService) Run() {
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

func (group_manager *GroupService) RecycleLoop() {
	//5 min
	ticker := time.NewTicker(time.Second * 60 * 5)
	for range ticker.C {
		group_manager.Recycle()
	}
}

func (group_manager *GroupService) Start() {
	group_manager.load()
	go group_manager.Run()
	go group_manager.RecycleLoop()
}
