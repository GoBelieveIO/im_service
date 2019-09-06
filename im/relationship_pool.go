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

func (rp *RelationshipPool) HandleFriend(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 4 {
		log.Info("message error:", data)
		return
	}

	appid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	friend_uid, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	friend, err := strconv.ParseInt(arr[3], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	
	rp.SetMyFriend(appid, uid, friend_uid, friend != 0)
	
}

func (rp *RelationshipPool) HandleBlacklist(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 4 {
		log.Info("message error:", data)
		return
	}

	appid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	friend_uid, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	friend, err := strconv.ParseInt(arr[3], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	rp.SetInMyBlacklist(appid, uid, friend_uid, friend != 0)
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


//检查当前的action id 是否变更，变更时则重新加载群组结构
func (rp *RelationshipPool) checkActionID() {
	action_id, err := rp.getActionID()
	if err != nil {
		return
	}

	if action_id != rp.action_id {
		rp.ReloadRelation()
		rp.action_id = action_id
		rp.dirty = false
	}
}

//保证action id的顺序性
func (rp *RelationshipPool) parseAction(data string) (bool, int64, int64, string) {
	arr := strings.SplitN(data, ":", 3)
	if len(arr) != 3 {
		log.Warning("friend action error:", data)
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

func (rp *RelationshipPool) HandleAction(data string, channel string) {
	r, prev_id, action_id, content := rp.parseAction(data)
	if r {
		log.Info("friend action:", prev_id, action_id, rp.action_id, " ", channel)
		if rp.action_id != prev_id {
			rp.dirty = true
			log.Warning("friend action nonsequence:", rp.action_id, prev_id, action_id)
		}

		if channel == "channel_friend" {
			rp.HandleFriend(content)
		} else if channel == "channel_blacklist" {
			rp.HandleBlacklist(content)
		}
		rp.action_id = action_id
	}
}

func (rp *RelationshipPool) HandlePing() {
	//check dirty
	if rp.dirty {
		action_id, err := rp.getActionID()
		if err == nil {
			rp.ReloadRelation()
			rp.dirty = false
			rp.action_id = action_id
		} else {
			log.Warning("get action id err:", err)
		}
	} else {
		rp.checkActionID()
	}	
}


func (rp *RelationshipPool) Subcribe(psc redis.PubSubConn) {
	psc.Subscribe("channel_friend", "channel_blacklist")
}


func (rp *RelationshipPool) OnStart() {
	rp.checkActionID()
}
