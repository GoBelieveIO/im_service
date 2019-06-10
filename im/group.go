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

import "time"
import "sync"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"

type Group struct {
	gid     int64
	appid   int64
	super   bool //超大群
	mutex   sync.Mutex
	//key:成员id value:入群时间|(mute<<31)
	members map[int64]int64

	ts      int//访问时间
}

func NewGroup(gid int64, appid int64, members map[int64]int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = false
	group.members = members
	group.ts = int(time.Now().Unix())	
	return group
}

func NewSuperGroup(gid int64, appid int64, members map[int64]int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = true
	group.members = members
	group.ts = int(time.Now().Unix())
	return group
}


func (group *Group) Members() map[int64]int64 {
	return group.members
}


func (group *Group) cloneMembers() map[int64]int64 {
	members := group.members
	n := make(map[int64]int64)
	for k, v := range members {
		n[k] = v
	}
	return n
}


//修改成员，在副本修改，避免读取时的lock
func (group *Group) AddMember(uid int64, timestamp int) {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	members := group.cloneMembers()
	members[uid] = int64(timestamp)
	group.members = members
}

func (group *Group) RemoveMember(uid int64) {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	if _, ok := group.members[uid]; !ok {
		return
	}
	members := group.cloneMembers()
	delete(members, uid)
	group.members = members
}

func (group *Group) SetMemberMute(uid int64, mute bool) {
	group.mutex.Lock()
	defer group.mutex.Unlock()

	var m int64
	if mute {
		m = 1
	} else {
		m = 0
	}

	if val, ok := group.members[uid]; ok {
		group.members[uid] = (val & 0x7FFFFFFF) | (m << 31)
	}
}

func (group *Group) IsMember(uid int64) bool {
	_, ok := group.members[uid]
	return ok
}

func (group *Group) GetMemberTimestamp(uid int64) int {
	ts, _ := group.members[uid]
	return int(ts & 0x7FFFFFFF)
}

func (group *Group) GetMemberMute(uid int64) bool {
	t, _ := group.members[uid]
	return int((t >> 31)&0x01) != 0
}

func (group *Group) IsEmpty() bool {
	return len(group.members) == 0
}



func CreateGroup(db *sql.DB, appid int64, master int64, name string, super int8) int64 {
	log.Info("create group super:", super)
	stmtIns, err := db.Prepare("INSERT INTO `group`(appid, master, name, super) VALUES( ?, ?, ?, ? )")
	if err != nil {
		log.Info("error:", err)
		return 0
	}
	defer stmtIns.Close()
	result, err := stmtIns.Exec(appid, master, name, super)
	if err != nil {
		log.Info("error:", err)
		return 0
	}
	gid, err := result.LastInsertId()
	if err != nil {
		log.Info("error:", err)
		return 0
	}
	return gid
}

func DeleteGroup(db *sql.DB, group_id int64) bool {
	var stmt1, stmt2 *sql.Stmt

	tx, err := db.Begin()
	if err != nil {
		log.Info("error:", err)
		return false
	}

	stmt1, err = tx.Prepare("DELETE FROM `group` WHERE id=?")
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}
	defer stmt1.Close()
	_, err = stmt1.Exec(group_id)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}

	stmt2, err = tx.Prepare("DELETE FROM group_member WHERE group_id=?")
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}
	defer stmt2.Close()
	_, err = stmt2.Exec(group_id)
	if err != nil {
		log.Info("error:", err)
		goto ROLLBACK
	}

	tx.Commit()
	return true

ROLLBACK:
	tx.Rollback()
	return false
}

func LoadGroup(db *sql.DB, group_id int64) (*Group, error) {
	stmtIns, err := db.Prepare("SELECT id, appid, super FROM `group` WHERE id=?")
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	defer stmtIns.Close()

	var group *Group
	var id int64
	var appid int64
	var super int8
	
	row := stmtIns.QueryRow()	
	err = row.Scan(&id, &appid, &super)
	if err != nil {
		return nil, err
	}
	
	members, err := LoadGroupMember(db, id)
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	if super != 0 {
		group = NewSuperGroup(id, appid, members)
	} else {
		group = NewGroup(id, appid, members)
	}

	return group, nil	
}

func LoadAllGroup(db *sql.DB) (map[int64]*Group, error) {
	stmtIns, err := db.Prepare("SELECT id, appid, super FROM `group`")
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	defer stmtIns.Close()
	groups := make(map[int64]*Group)
	rows, err := stmtIns.Query()
	for rows.Next() {
		var id int64
		var appid int64
		var super int8
		rows.Scan(&id, &appid, &super)
		members, err := LoadGroupMember(db, id)
		if err != nil {
			log.Info("error:", err)
			return nil, err
		}

		if super != 0 {
			group := NewSuperGroup(id, appid, members)
			groups[group.gid] = group
		} else {
			group := NewGroup(id, appid, members)
			groups[group.gid] = group
		}
	}
	return groups, nil
}

func LoadGroupMember(db *sql.DB, group_id int64) (map[int64]int64, error) {
	stmtIns, err := db.Prepare("SELECT uid, timestamp, mute FROM group_member WHERE group_id=?")
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	defer stmtIns.Close()
	members := make(map[int64]int64)
	rows, err := stmtIns.Query(group_id)
	for rows.Next() {
		var uid int64
		var timestamp int64
		var mute int64
		rows.Scan(&uid, &timestamp, &mute)
		members[uid] = timestamp|(mute<<31)
	}
	return members, nil
}
