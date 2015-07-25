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

type Group struct {
	gid     int64
	appid   int64
	super   bool //超大群
	mutex   sync.Mutex
	members IntSet
}

func NewGroup(gid int64, appid int64, members []int64) *Group {
	group := new(Group)
	group.appid = appid
	group.gid = gid
	group.super = true
	group.members = NewIntSet()
	for _, m := range members {
		group.members.Add(m)
	}
	return group
}

func (group *Group) Members() IntSet {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	return group.members.Clone()
}

func (group *Group) AddMember(uid int64) {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	group.members.Add(uid)
}

func (group *Group) RemoveMember(uid int64) {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	group.members.Remove(uid)
}

func (group *Group) IsMember(uid int64) bool {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	_, ok := group.members[uid]
	return ok
}

func (group *Group) IsEmpty() bool {
	group.mutex.Lock()
	defer group.mutex.Unlock()
	return len(group.members) == 0
}

func CreateGroup(db *sql.DB, appid int64, master int64, name string) int64 {
	stmtIns, err := db.Prepare("INSERT INTO `group`(appid, master, name) VALUES( ?, ?, ? )")
	if err != nil {
		log.Info("error:", err)
		return 0
	}
	defer stmtIns.Close()
	result, err := stmtIns.Exec(appid, master, name)
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

func AddGroupMember(db *sql.DB, group_id int64, uid int64) bool {
	stmtIns, err := db.Prepare("INSERT INTO group_member(group_id, uid) VALUES( ?, ? )")
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(group_id, uid)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	return true
}

func RemoveGroupMember(db *sql.DB, group_id int64, uid int64) bool {
	stmtIns, err := db.Prepare("DELETE FROM group_member WHERE group_id=? AND uid=?")
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(group_id, uid)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	return true
}

func LoadAllGroup(db *sql.DB) (map[int64]*Group, error) {
	stmtIns, err := db.Prepare("SELECT id, appid FROM `group`")
	if err != nil {
		log.Info("error:", err)
		return nil, nil
	}

	defer stmtIns.Close()
	groups := make(map[int64]*Group)
	rows, err := stmtIns.Query()
	for rows.Next() {
		var id int64
		var appid int64
		rows.Scan(&id, &appid)
		members, err := LoadGroupMember(db, id)
		if err != nil {
			log.Info("error:", err)
			continue
		}
		group := NewGroup(id, appid, members)
		groups[group.gid] = group
	}
	return groups, nil
}

func LoadGroupMember(db *sql.DB, group_id int64) ([]int64, error) {
	stmtIns, err := db.Prepare("SELECT uid FROM group_member WHERE group_id=?")
	if err != nil {
		log.Info("error:", err)
		return nil, err
	}

	defer stmtIns.Close()
	members := make([]int64, 0, 4)
	rows, err := stmtIns.Query(group_id)
	for rows.Next() {
		var uid int64
		rows.Scan(&uid)
		members = append(members, uid)
	}
	return members, nil
}
