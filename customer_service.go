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

type CustomerService struct {
	mutex sync.Mutex
	app_groups map[int64]int64
	app_modes map[int64]int
}

func NewCustomerService() *CustomerService {
	cs := new(CustomerService)
	cs.app_groups = make(map[int64]int64)
	cs.app_modes = make(map[int64]int)
	return cs
}

func (cs *CustomerService) LoadApplicationConfig(db *sql.DB, appid int64) (int64, int, error) {
	stmtIns, err := db.Prepare("SELECT cs_group_id, cs_mode FROM app WHERE id=?")
	if err != nil {
		log.Info("error:", err)
		return 0, 0, err
	}

	defer stmtIns.Close()
	row := stmtIns.QueryRow(appid)

	var cs_group_id int64
	var cs_mode int
	err = row.Scan(&cs_group_id, &cs_mode)
	log.Infof("application customer service group id:%d mode:%d", cs_group_id, cs_mode)
	return cs_group_id, cs_mode, err
}

func (cs *CustomerService) GetApplicationConfig(appid int64) (int64, int) {
	cs.mutex.Lock()
	if group_id, ok := cs.app_groups[appid]; ok {
		mode := cs.app_modes[appid]
		cs.mutex.Unlock()
		return group_id, mode
	}
	cs.mutex.Unlock()
	db, err := sql.Open("mysql", config.mysqldb_appdatasource)
	if err != nil {
		log.Info("error:", err)
		return 0, 0
	}
	defer db.Close()

	cs_group_id, cs_mode, err := cs.LoadApplicationConfig(db, appid)
	if err != nil {
		log.Error("db error:", err)
		return 0, 0
	}

	cs.mutex.Lock()
	cs.app_groups[appid] = cs_group_id
	cs.app_modes[appid] = cs_mode
	cs.mutex.Unlock()

	return cs_group_id, cs_mode
}

func (cs *CustomerService) GetOnlineStaffID(appid int64) int64 {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("customer_service_online_%d", appid)

	staff_id, err := redis.Int64(conn.Do("SRANDMEMBER", key))
	if err != nil {
		log.Error("srandmember err:", err)
		return 0
	}
	return staff_id
}

func (cs *CustomerService) IsOnline(appid int64, staff_id int64) bool {
	conn := redis_pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("customer_service_online_%d", appid)	
	
	on, err := redis.Bool(conn.Do("SISMEMBER", key, staff_id))
	if err != nil {
		log.Error("sismember err:", err)
		return false
	}
	return on
}

func (cs *CustomerService) HandleUpdate(data string) {
	appid, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	log.Infof("application:%d update", appid)
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	delete(cs.app_groups, appid)
	delete(cs.app_modes, appid)
}

func (cs *CustomerService) Clear() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	for k := range cs.app_groups {
		delete(cs.app_groups, k)
	}

	for k := range cs.app_modes {
		delete(cs.app_modes, k)
	}
}

func (cs *CustomerService) RunOnce() bool {
	c, err := redis.Dial("tcp", config.redis_address)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}
	psc := redis.PubSubConn{c}
	psc.Subscribe("application_update")
	cs.Clear()
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if v.Channel == "application_update" {
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
