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
import "database/sql"
import mysql "github.com/go-sql-driver/mysql"
import log "github.com/sirupsen/logrus"

const NoneRelationship = 0

type Relationship int32

func NewRelationship(is_my_friend bool, is_your_friend bool, is_in_my_blacklist bool, is_in_your_blacklist bool) Relationship {
	var r Relationship

	if is_my_friend {
		r |= 0x01
	}
	if is_your_friend {
		r |= 0x02
	}
	if is_in_my_blacklist {
		r |= 0x04
	}
	if is_in_your_blacklist {
		r |= 0x08
	}
	return r
}

func (rs Relationship) IsMyFriend() bool {
	return (rs&0x01) != 0
}

func (rs Relationship) IsYourFriend() bool {
	return (rs&0x02) != 0
}

func (rs Relationship) IsInMyBlacklist() bool {
	return (rs&0x04) != 0
}

func (rs Relationship) IsInYourBlacklist() bool {
	return (rs&0x08) != 0
}

func (rs Relationship) reverse() Relationship {
	return NewRelationship(rs.IsYourFriend(), rs.IsMyFriend(), rs.IsInYourBlacklist(), rs.IsInMyBlacklist())
}


func GetRelationship(db *sql.DB, appid int64, uid int64, friend_uid int64) (Relationship, error) {
	stmtIns, err := db.Prepare("SELECT uid, friend_uid FROM `friend` WHERE appid=? AND uid=? AND friend_uid=?")
	if err == mysql.ErrInvalidConn {
		log.Info("db prepare error:", err)
		stmtIns, err = db.Prepare("SELECT uid, friend_uid FROM `friend` WHERE appid=? AND uid=? AND friend_uid=?")
	}

	if err != nil {
		log.Info("db prepare error:", err)
		return NoneRelationship, err
	}
	
	var is_my_friend, is_your_friend, is_in_my_blacklist, is_in_your_blacklist bool
	var _uid int64
	var _friend_uid int64
	
	row := stmtIns.QueryRow(appid, uid, friend_uid)
	err = row.Scan(&_uid, &_friend_uid)
	if err != nil {
		log.Info("scan error:", err)
	}
	is_my_friend = err == nil


	row = stmtIns.QueryRow(appid, friend_uid, uid)
	err = row.Scan(&_uid, &_friend_uid)
	if err != nil {
		log.Info("scan error:", err)
	}
	is_your_friend = err == nil

	stmtIns.Close()	

	stmtIns, err = db.Prepare("SELECT uid, friend_uid FROM `blacklist` WHERE appid=? AND uid=? AND friend_uid=?")
	if err != nil {
		log.Info("error:", err)
		return NoneRelationship, err
	}
	
	row = stmtIns.QueryRow(appid, uid, friend_uid)
	err = row.Scan(&_uid, &_friend_uid)
	if err != nil {
		log.Info("scan error:", err)
	}
	is_in_my_blacklist = err == nil

	row = stmtIns.QueryRow(appid, friend_uid, uid)
	err = row.Scan(&_uid, &_friend_uid)
	if err != nil {
		log.Info("scan error:", err)
	}
	is_in_your_blacklist = err == nil

	stmtIns.Close()		

	rs := NewRelationship(is_my_friend, is_your_friend, is_in_my_blacklist, is_in_your_blacklist)

	return rs, nil
}
