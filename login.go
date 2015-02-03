package main

import "fmt"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"


func (client *Client) SaveLoginInfo() {
	conn := redis_pool.Get()
	defer conn.Close()

	var platform_id int8 = client.platform_id
	var device_id string = client.device_id

	key := fmt.Sprintf("user_logins_%d_%d", client.appid, client.uid)
	value := fmt.Sprintf("%d_%s", platform_id, device_id)
	_, err := conn.Do("SADD", key, value)
	if err != nil {
		log.Warning("sadd err:", err)
	}
	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)

	key = fmt.Sprintf("user_logins_%d_%d_%d_%s", client.appid, client.uid, platform_id, device_id)
	_, err = conn.Do("HMSET", key, "up_timestamp", client.tm.Unix(), "platform_id", platform_id, "device_id", device_id)
	if err != nil {
		log.Warning("hset err:", err)
	}

	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)
}

func (client *Client) RefreshLoginInfo() {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("user_logins_%d_%d", client.appid, client.uid)

	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)

	key = fmt.Sprintf("user_logins_%d_%d_%s_%s", client.appid, client.uid, client.platform_id, client.device_id)
	
	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)	
}

func (client *Client) RemoveLoginInfo() {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("user_logins_%d_%d", client.appid, client.uid)
	value := fmt.Sprintf("%d_%s", client.platform_id, client.device_id)
	conn.Do("SREM", key, value)

	key = fmt.Sprintf("user_logins_%d_%d_%d_%s", client.appid, client.uid, client.platform_id, client.device_id)
	
	conn.Do("DEL", key)

}

func (client *Client) ListLoginInfo() []*LoginPoint {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("user_logins_%d_%d", client.appid, client.uid)
	members, err := redis.Values(conn.Do("SMEMBERS", key))
	if err != nil {
		log.Error("smembers err:", err)
		return nil
	}
	
	points := make([]*LoginPoint, 0)
	for _, member := range(members) {
		key = fmt.Sprintf("user_logins_%d_%d_%s", client.appid, client.uid, member)
		obj, err := redis.Values(conn.Do("HMGET", key, "up_timestamp", "platform_id", "device_id"))
		if err != nil {
			log.Error("hmget err:", err)
			break
		}
		if obj == nil || (obj[0] == nil && obj[1] == nil && obj[2] == nil) {
			log.Warning("hmget nil")
			continue
		}

		var up_timestamp int64
		var platform_id  int64
		var device_id string
		_, err = redis.Scan(obj, &up_timestamp, &platform_id, &device_id)
		if err != nil {
			log.Warning("scan error:", err)
			break
		}
		point := &LoginPoint{}
		point.up_timestamp = int32(up_timestamp)
		point.platform_id = int8(platform_id)
		point.device_id = device_id
		points = append(points, point)
	}

	return points
}
