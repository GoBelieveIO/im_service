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

func GetDeviceID(device_id string, platform_id int) (int64, error) {
	var ctx = context.Background()

	key := fmt.Sprintf("devices_%s_%d", device_id, platform_id)
	device_ID, err := redis_client.Get(ctx, key).Int64()
	if err != nil {
		k := "devices_id"
		device_ID, err = redis_client.Incr(ctx, k).Result()
		if err != nil {
			return 0, err
		}

		_, err = redis_client.Set(ctx, key, device_ID, 0).Result()
		if err != nil {
			return 0, err
		}
	}
	return device_ID, err
}
