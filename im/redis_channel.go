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
import "math/rand"
import "github.com/gomodule/redigo/redis"
import log "github.com/golang/glog"

type RedisSubscriber interface {
	Subcribe(psc redis.PubSubConn)
	OnStart()
	HandleAction(data, channel string)
	HandlePing()
}

type RedisChannel struct {
	ping string
	subscribers []RedisSubscriber
}

func NewRedisChannel() *RedisChannel {
	rc := &RedisChannel{}

	now := time.Now().Unix()
	r := fmt.Sprintf("channel_ping_%d", now)
	for i := 0; i < 4; i++ {
		n := rand.Int31n(26)
		r = r + string('a' + n)
	}
	
	rc.ping = r
	rc.subscribers = make([]RedisSubscriber, 0, 2)
	return rc
}

func (rp *RedisChannel) AddSubscriber(sub RedisSubscriber) {
	rp.subscribers = append(rp.subscribers, sub)
}

func (rp *RedisChannel) RunOnce() bool {
	t := redis.DialReadTimeout(time.Second*SUBSCRIBE_HEATBEAT)
	c, err := redis.Dial("tcp", config.redis_address, t)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}

	password := config.redis_password
	if len(password) > 0 {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return false
		}
	}

	psc := redis.PubSubConn{c}

	psc.Subscribe(rp.ping)	
	for _, sub := range(rp.subscribers) {
		sub.Subcribe(psc)
	}

	for _, sub := range(rp.subscribers) {
		sub.OnStart()			
	}	


	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if v.Channel == rp.ping {
				for _, sub := range(rp.subscribers) {
					sub.HandlePing()
				}
			} else {
				log.Infof("%s: message: %s\n", v.Channel, v.Data)				
				for _, sub := range(rp.subscribers) {
					sub.HandleAction(string(v.Data), v.Channel)					
				}				
			}
		case redis.Subscription:
			log.Infof("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			log.Info("error:", v)
			return true
		}
	}
}

func (rp *RedisChannel) Run() {
	nsleep := 1
	for {
		connected := rp.RunOnce()
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

func (rp *RedisChannel) Ping() {
	conn := redis_pool.Get()
	defer conn.Close()

	_, err := conn.Do("PUBLISH", rp.ping, "ping")
	if err != nil {
		log.Info("ping error:", err)
	}
}


func (rp *RedisChannel) PingLoop() {
	for {
		rp.Ping()
		time.Sleep(time.Second*(SUBSCRIBE_HEATBEAT-10))
	}
}

func (rp *RedisChannel) Start() {
	go rp.Run()
	go rp.PingLoop()
}

