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
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "fmt"
import "strconv"
import "time"

type Store struct {
	id       int64
	group_id int64
	mode     int
}

type CustomerService struct {
	mutex sync.Mutex
	stores map[int64]*Store
}

func NewCustomerService() *CustomerService {
	cs := new(CustomerService)
	cs.stores = make(map[int64]*Store)
	return cs
}

func (cs *CustomerService) LoadStore(db *sql.DB, store_id int64) (*Store, error) {
	stmtIns, err := db.Prepare("SELECT group_id, mode FROM store WHERE id=?")
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	defer stmtIns.Close()
	row := stmtIns.QueryRow(store_id)

	var group_id int64
	var mode int
	err = row.Scan(&group_id, &mode)
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	s := &Store{}
	s.id = store_id
	s.group_id = group_id
	s.mode = mode
	return s, nil
}

func (cs *CustomerService) GetStore(store_id int64) (*Store, error) {
	cs.mutex.Lock()
	if s, ok := cs.stores[store_id]; ok {
		cs.mutex.Unlock()
		return s, nil
	}
	cs.mutex.Unlock()

	db, err := sql.Open("mysql", config.mysqldb_datasource)
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}
	defer db.Close()

	s, err := cs.LoadStore(db, store_id)
	if err != nil {
		return nil, err
	}
	cs.mutex.Lock()
	cs.stores[store_id] = s
	cs.mutex.Unlock()
	return s, nil
}

func (cs *CustomerService) GetLastSellerID(appid, uid, store_id int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d_%d", appid, uid, store_id)

	seller_id, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		log.Error("get last seller id err:", err)
		return 0
	}
	return seller_id
}

func (cs *CustomerService) SetLastSellerID(appid, uid, store_id, seller_id int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d_%d", appid, uid, store_id)

	//24hour expire
	_, err := conn.Do("SET", key, seller_id, "EX", 3600*24)
	if err != nil {
		log.Error("get last seller id err:", err)
		return
	}
}

//随机获取一个的销售人员
func (cs *CustomerService) GetSellerID(store_id int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("stores_seller_%d", store_id)

	staff_id, err := redis.Int64(conn.Do("SRANDMEMBER", key))
	if err != nil {
		log.Error("srandmember err:", err)
		return 0
	}
	return staff_id
	
}

func (cs *CustomerService) GetOrderSellerID(store_id int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("stores_zseller_%d", store_id)

	r, err := redis.Values(conn.Do("ZRANGE", key, 0, 0))
	if err != nil {
		log.Error("srange err:", err)
		return 0
	}

	log.Info("zrange:", r, key)
	var seller_id int64
	_, err = redis.Scan(r, &seller_id)
	if err != nil {
		log.Error("scan err:", err)
		return 0
	}

	_, err = conn.Do("ZINCRBY", key, 1, seller_id)
	if err != nil {
		log.Error("zincrby err:", err)
	}
	return seller_id
}


//随机获取一个在线的销售人员
func (cs *CustomerService) GetOnlineSellerID(store_id int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("stores_online_seller_%d", store_id)

	staff_id, err := redis.Int64(conn.Do("SRANDMEMBER", key))
	if err != nil {
		log.Error("srandmember err:", err)
		return 0
	}
	return staff_id
}

func (cs *CustomerService) IsOnline(store_id int64, seller_id int64) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("stores_online_seller_%d", store_id)	
	
	on, err := redis.Bool(conn.Do("SISMEMBER", key, seller_id))
	if err != nil {
		log.Error("sismember err:", err)
		return false
	}
	return on
}

func (cs *CustomerService) HandleUpdate(data string) {
	store_id, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	log.Infof("store:%d update", store_id)
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	delete(cs.stores, store_id)
}

func (cs *CustomerService) Clear() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	for k := range cs.stores {
		delete(cs.stores, k)
	}
}

func (cs *CustomerService) RunOnce() bool {
	c, err := redis.Dial("tcp", config.redis_address)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}
	psc := redis.PubSubConn{c}
	psc.Subscribe("store_update")
	cs.Clear()
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if v.Channel == "store_update" {
				cs.HandleUpdate(string(v.Data))
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

func (cs *CustomerService) Run() {
	nsleep := 1
	for {
		connected := cs.RunOnce()
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

func (cs *CustomerService) Start() {
	go cs.Run()
}
