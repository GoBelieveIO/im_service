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
import "context"
import log "github.com/sirupsen/logrus"
import "github.com/go-redis/redis/v8"

func GetSyncKey(appid int64, uid int64) int64 {
	var ctx = context.Background()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	r, err := redis_client.HGet(ctx, key, "sync_key").Int64()
	if err != nil {
		if err != redis.Nil {
			log.Info("hget error:", err)
		}
		return 0
	}
	return r
}

func GetGroupSyncKey(appid int64, uid int64, group_id int64) int64 {
	var ctx = context.Background()	

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", group_id)

	r, err := redis_client.HGet(ctx, key, field).Int64()
	if err != nil {
		if err != redis.Nil {
			log.Info("hget error:", err)
		}
		return 0
	}
	return r	
}

func SaveSyncKey(appid int64, uid int64, sync_key int64) {
	var ctx = context.Background()		

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	_, err := redis_client.HSet(ctx, key, "sync_key", sync_key).Result()
	if err != nil {
		log.Warning("hset error:", err)
	}
}

func SaveGroupSyncKey(appid int64, uid int64, group_id int64, sync_key int64) {
	var ctx = context.Background()			

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", group_id)

	_, err := redis_client.HSet(ctx, key, field, sync_key).Result()
	if err != nil {
		log.Warning("hset error:", err)
	}	
}

func GetUserPreferences(appid int64, uid int64) (int, bool, error) {
	var ctx = context.Background()				

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	reply := redis_client.HMGet(ctx, key,  "forbidden", "notification_on")
	if reply.Err() != nil {
		return 0, false, reply.Err();
	}

	c := struct{
		//用户禁言		
		Forbidden int `redis:"forbidden"`
		//电脑在线，手机新消息通知				
		NotificationOn int `redis:"notification_on"`
	}{0, 0}

	err := reply.Scan(&c)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, false, err		
	}
	return c.Forbidden, c.NotificationOn != 0, nil
}

func SetUserUnreadCount(appid int64, uid int64, count int32) {
	var ctx = context.Background()					

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	
	_, err := redis_client.HSet(ctx, key, "unread", count).Result()
	if err != nil {
		log.Info("hset err:", err)
	}
}

