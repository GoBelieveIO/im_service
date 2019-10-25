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

import "fmt"
import "time"
import "sync"
import "sync/atomic"
import "errors"
import "strconv"
import "strings"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "github.com/gomodule/redigo/redis"
import log "github.com/golang/glog"

const EXPIRE_DURATION = 60*60 //60min
const ACTIVE_DURATION = 10*60
const RELATIONSHIP_POOL_STREAM_NAME = "relationship_stream"
const RELATIONSHIP_POOL_XREAD_TIMEOUT = 60

const RELATIONSHIP_EVENT_FRIEND = "friend"
const RELATIONSHIP_EVENT_BLACKLIST = "blacklist"

type RelationshipEvent struct {
	Id string //stream entry id
	ActionId int64 `redis:"action_id"`
	PreviousActionId int64 `redis:"previous_action_id"`
	Name string `redis:"name"`
	AppId int64 `redis:"app_id"`
	Uid int64 `redis:"uid"`
	FriendUid int64 `redis:"friend_uid"`
	IsFriend bool `redis:"friend"`
	IsBlacklist bool `redis:"blacklist"`
}

type RelationshipItem struct {
	rs Relationship
	ts int //访问时间
}

/**relationship的缓存
*回收：单个缓存项超过一定时间未被使用
*todo:整体内存使用量超过一定阀值时回收
*key: appid:uid:friend_uid
**/
type RelationshipPool struct {
	items  *sync.Map
	item_count int64

	dirty     bool
	last_entry_id string
	action_id int64
	db        *sql.DB	
}

func NewRelationshipPool() *RelationshipPool {
	rp := &RelationshipPool{}
	rp.items = &sync.Map{}

	db, err := sql.Open("mysql", config.mysqldb_datasource)
	if err != nil {
		log.Fatal("open db:", err)		
	}
	rp.db = db
	return rp
}

func (rp *RelationshipPool) GetRelationship(appid, uid, friend_uid int64) Relationship {
	reverse := false
	if uid > friend_uid {
		reverse = true
		t := uid
		uid = friend_uid
		friend_uid = t
	}

	timestamp := int(time.Now().Unix())
	
	key := fmt.Sprintf("%d:%d:%d", appid, uid, friend_uid)

	val, ok := rp.items.Load(key)
	if ok {
		r := val.(*RelationshipItem)
		//更新访问时间
		r.ts = timestamp
		if reverse {
			return r.rs.reverse()
		} else {
			return r.rs
		}
	}
	
	rs, err := GetRelationship(rp.db, appid, uid, friend_uid)
	if err != nil {
		return NoneRelationship
	}

	rp.items.Store(key, &RelationshipItem{rs, timestamp})
	atomic.AddInt64(&rp.item_count, 1)
	
	if reverse {
		return rs.reverse()
	} else {
		return rs
	}
}

func (rp *RelationshipPool) SetMyFriend(appid, uid, friend_uid int64, is_my_friend bool) {
	if (uid > friend_uid) {
		rp.SetYourFriend(appid, friend_uid, uid, is_my_friend)
		return
	}

	key := fmt.Sprintf("%d:%d:%d", appid, uid, friend_uid)	
	v, ok := rp.items.Load(key)
	if !ok {
		log.Info("can not load key:", key)
		return
	}

	r := v.(*RelationshipItem)
	r.rs = NewRelationship(is_my_friend, r.rs.IsYourFriend(), r.rs.IsInMyBlacklist(), r.rs.IsInYourBlacklist())
	log.Infof("new relationship:%d-%d %d", uid, friend_uid, r.rs)
}

func (rp *RelationshipPool) SetYourFriend(appid, uid, friend_uid int64, is_your_friend bool) {
	if (uid > friend_uid) {
		rp.SetMyFriend(appid, friend_uid, uid, is_your_friend)
		return
	}

	key := fmt.Sprintf("%d:%d:%d", appid, uid, friend_uid)		
	v, ok := rp.items.Load(key)
	if !ok {
		log.Info("can not load key:", key)		
		return
	}
	
	r := v.(*RelationshipItem)
	r.rs = NewRelationship(r.rs.IsMyFriend(), is_your_friend, r.rs.IsInMyBlacklist(), r.rs.IsInYourBlacklist())

	log.Infof("new relationship:%d-%d %d", uid, friend_uid, r.rs)
}

func (rp *RelationshipPool) SetInMyBlacklist(appid, uid, friend_uid int64, is_in_my_blacklist bool) {
	if (uid > friend_uid) {
		rp.SetInYourBlacklist(appid, friend_uid, uid, is_in_my_blacklist)
		return
	}
	
	key := fmt.Sprintf("%d:%d:%d", appid, uid, friend_uid)	
	v, ok := rp.items.Load(key)
	if !ok {
		log.Info("can not load key:", key)		
		return
	}
	
	r := v.(*RelationshipItem)
	r.rs = NewRelationship(r.rs.IsMyFriend(), r.rs.IsYourFriend(), is_in_my_blacklist, r.rs.IsInYourBlacklist())
	log.Infof("new relationship:%d-%d %d", uid, friend_uid, r.rs)	
}


func (rp *RelationshipPool) SetInYourBlacklist(appid, uid, friend_uid int64, is_in_your_blacklist bool) {
	if (uid > friend_uid) {
		rp.SetInMyBlacklist(appid, friend_uid, uid, is_in_your_blacklist)
		return
	}

	key := fmt.Sprintf("%d:%d:%d", appid, uid, friend_uid)	
	v, ok := rp.items.Load(key)
	if !ok {
		log.Info("can not load key:", key)
		return
	}
	
	r := v.(*RelationshipItem)	
	r.rs = NewRelationship(r.rs.IsMyFriend(), r.rs.IsYourFriend(), r.rs.IsInMyBlacklist(), is_in_your_blacklist)
	log.Infof("new relationship:%d-%d %d", uid, friend_uid, r.rs)		
}


func (rp *RelationshipPool) Recycle() {
	exp_items := make([]string, 0, 100)
	now := int(time.Now().Unix())

	var count int64
	f := func (key, value interface{}) bool {
		k := key.(string)
		r := value.(*RelationshipItem)
		if now - r.ts > EXPIRE_DURATION {
			exp_items = append(exp_items, k)
		}
		count += 1
		return true
	}

	rp.items.Range(f)
	for _, k := range(exp_items) {
		rp.items.Delete(k)
	}
	count -= int64(len(exp_items))
	atomic.StoreInt64(&rp.item_count, count)
}

func (rp *RelationshipPool) RecycleLoop() {
	//5 min
	ticker := time.NewTicker(time.Second * 60 * 5)
	for range ticker.C {
		rp.Recycle()
	}	
}

func (rp *RelationshipPool) HandleFriend(event *RelationshipEvent) {
	appid := event.AppId
	uid := event.Uid
	friend_uid := event.FriendUid
	is_friend := event.IsFriend
	if appid == 0 || uid == 0 || friend_uid == 0 {
		log.Info("invalid relationship event:%+v", event)
		return
	}
	
	rp.SetMyFriend(appid, uid, friend_uid, is_friend)
	
}

func (rp *RelationshipPool) HandleBlacklist(event *RelationshipEvent) {
	appid := event.AppId
	uid := event.Uid
	friend_uid := event.FriendUid
	is_blacklist := event.IsBlacklist

	if appid == 0 || uid == 0 || friend_uid == 0 {
		log.Info("invalid relationship event:%+v", event)
		return
	}
	rp.SetInMyBlacklist(appid, uid, friend_uid, is_blacklist)
}

func (rp *RelationshipPool) parseKey(k string) (int64, int64, int64, error) {
	arr := strings.Split(k, ":")
	if len(arr) != 3 {
		return 0, 0, 0, errors.New("relationship key invalid")
	}

	appid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	friend_uid, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	return appid, uid, friend_uid, nil
}


func (rp *RelationshipPool) ReloadRelation() {
	active_items := make([]string, 0, 100)
	now := int(time.Now().Unix())
	
	f := func (key, value interface{}) bool {
		k := key.(string)
		r := value.(*RelationshipItem)
		if now - r.ts < ACTIVE_DURATION {
			active_items = append(active_items, k)
		}
		return true
	}

	rp.items.Range(f)

	var count int64
	m := &sync.Map{}
	for _, k := range(active_items) {
		appid, uid, friend_uid, err := rp.parseKey(k)
		if err != nil {
			continue
		}
		
		rs, err := GetRelationship(rp.db, appid, uid, friend_uid)
		if err != nil {
			log.Warning("get relationship err:", err)
			continue
		}

		m.Store(k, &RelationshipItem{rs, now})
		count++;
	}


	atomic.StoreInt64(&rp.item_count, count)
	rp.items = m
}


func (rp *RelationshipPool) getLastEntryID() (string, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	r, err := redis.Values(conn.Do("XREVRANGE", RELATIONSHIP_POOL_STREAM_NAME, "+", "-", "COUNT", "1"))

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


func (rp *RelationshipPool) getActionID() (int64, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	actions, err := redis.String(conn.Do("GET", "friends_actions"))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0, err
	}
	if actions == "" {
		return 0, nil
	} else {
		arr := strings.Split(actions, ":")
		if len(arr) != 2 {
			log.Error("friends_actions invalid:", actions)
			return 0, errors.New("friends actions invalid")
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


func (rp *RelationshipPool) handleEvent(event *RelationshipEvent) {
	prev_id := event.PreviousActionId
	action_id := event.ActionId
	log.Infof("relationship action:%+v %d ", event, rp.action_id)
	if rp.action_id != prev_id {
		rp.dirty = true
		log.Warning("friend action nonsequence:", rp.action_id, prev_id, action_id)
	}

	if event.Name == RELATIONSHIP_EVENT_FRIEND {
		rp.HandleFriend(event)
	} else if event.Name == RELATIONSHIP_EVENT_BLACKLIST {
		rp.HandleBlacklist(event)
	} else {
		log.Warning("unknow event:", event.Name)
	}
	
	rp.action_id = action_id
	rp.last_entry_id = event.Id
}


func (rp *RelationshipPool) load() {
	//循环直到成功
	for {
		action_id, err := rp.getActionID()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		entry_id, err := rp.getLastEntryID()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		rp.last_entry_id = entry_id
		rp.action_id = action_id
		rp.dirty = false
		log.Infof("relationship pool action id:%d stream last entry id:%s",
			action_id, rp.last_entry_id)
		break
	}
}


func (rp *RelationshipPool) readEvents(c redis.Conn) ([]*RelationshipEvent, error) {
	//block timeout 60s
	reply, err := redis.Values(c.Do("XREAD", "COUNT", "1000", "BLOCK",
		RELATIONSHIP_POOL_XREAD_TIMEOUT*1000, "STREAMS",
		RELATIONSHIP_POOL_STREAM_NAME, rp.last_entry_id))
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
	
	events := make([]*RelationshipEvent, 0, 1000)
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

		event := &RelationshipEvent{}
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


func (rp *RelationshipPool) RunOnce() bool {
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
		events, err := rp.readEvents(c)
		if err != nil {
			log.Warning("group manager read event err:", err)
			break
		}

		for _, event := range events {
			rp.handleEvent(event)
		}

		now := time.Now().Unix()		

		//xread timeout, lazy policy
		if rp.dirty && now - last_clear_ts >= RELATIONSHIP_POOL_XREAD_TIMEOUT {
			log.Info("reload relationship")
			rp.ReloadRelation()				
			rp.dirty = false
			last_clear_ts = now
		}

	}
	return true
}

func (rp *RelationshipPool) Run() {
	nsleep := 1
	for {
		connected := rp.RunOnce()
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


func (rp *RelationshipPool) Start() {
	rp.load()
	go rp.Run()
	go rp.RecycleLoop()
}
