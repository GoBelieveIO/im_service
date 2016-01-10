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
