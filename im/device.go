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
import "github.com/gomodule/redigo/redis"

func GetDeviceID(device_id string, platform_id int) (int64, error) {
	conn := redis_pool.Get()
	defer conn.Close()
	key := fmt.Sprintf("devices_%s_%d", device_id, platform_id)
	device_ID, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		k := "devices_id"
		device_ID, err = redis.Int64(conn.Do("INCR", k))
		if err != nil {
			return 0, err
		}
		_, err = conn.Do("SET", key, device_ID)
		if err != nil {
			return 0, err
		}
	}
	return device_ID, err
}
