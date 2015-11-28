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
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "errors"

func LoadUserAccessToken(token string) (int64, int64, string, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("access_token_%s", token)
	var uid int64
	var appid int64
	var uname string

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return 0, 0, "", err
	}
	if !exists {
		return 0, 0, "", errors.New("token non exists")
	}

	reply, err := redis.Values(conn.Do("HMGET", key, "user_id", "app_id", "user_name"))
	if err != nil {
		log.Info("hmget error:", err)
		return 0, 0, "", err
	}

	_, err = redis.Scan(reply, &uid, &appid, &uname)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, 0, "", err
	}
	return appid, uid, uname, nil	
}

func CountUser(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("statistics_users_%d", appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func CountDAU(appid int64, uid int64) {
	conn := redis_pool.Get()
	defer conn.Close()
	
	now := time.Now()
	date := fmt.Sprintf("%d_%d_%d", now.Year(), int(now.Month()), now.Day())
	key := fmt.Sprintf("statistics_dau_%s_%d", date, appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func SetUserUnreadCount(appid int64, uid int64, count int32) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	_, err := conn.Do("HSET", key, "unread", count)
	if err != nil {
		log.Info("hset err:", err)
	}
}
