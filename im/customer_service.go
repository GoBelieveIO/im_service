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
import log "github.com/sirupsen/logrus"
import "github.com/gomodule/redigo/redis"
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
	sellers map[int64]int //销售员 sellerid:timestamp
	online_sellers map[int64]int //在线的销售员 sellerid:timestamp
}

func NewCustomerService() *CustomerService {
	cs := new(CustomerService)
	cs.stores = make(map[int64]*Store)
	cs.sellers = make(map[int64]int)
	cs.online_sellers = make(map[int64]int)
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

	_, err := conn.Do("SET", key, seller_id)
	if err != nil {
		log.Error("get last seller id err:", err)
		return
	}
}

//判断销售人员是否合法
func (cs *CustomerService) IsExist(store_id int64, seller_id int64) bool {
	now := int(time.Now().Unix())
	cs.mutex.Lock()
	if t, ok := cs.sellers[seller_id]; ok {
		if now - t < 10*60 {
			cs.mutex.Unlock()
			return true
		}
		//缓存超时
		delete(cs.sellers, seller_id)
	}
	cs.mutex.Unlock()

	
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("stores_seller_%d", store_id)

	exists, err := redis.Bool(conn.Do("SISMEMBER", key, seller_id))
	if err != nil {
		log.Error("sismember err:", err)
		return false
	}

	if exists {
		cs.mutex.Lock()
		cs.sellers[seller_id] = now
		cs.mutex.Unlock()
	}
	return exists
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
	now := int(time.Now().Unix())
	cs.mutex.Lock()
	if t, ok := cs.online_sellers[seller_id]; ok {
		if now - t < 10*60 {
			cs.mutex.Unlock()
			return true
		}
		//缓存超时
		delete(cs.online_sellers, seller_id)
	}
	cs.mutex.Unlock()

	conn := redis_pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("stores_online_seller_%d", store_id)	
	
	on, err := redis.Bool(conn.Do("SISMEMBER", key, seller_id))
	if err != nil {
		log.Error("sismember err:", err)
		return false
	}

	if on {
		cs.mutex.Lock()
		cs.online_sellers[seller_id] = now
		cs.mutex.Unlock()
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

func (cs *CustomerService) HandleMessage(v *redis.Message) {
	if v.Channel == "store_update" {
		cs.HandleUpdate(string(v.Data))
	}
}
