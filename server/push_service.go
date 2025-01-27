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

package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
)

const PUSH_QUEUE_TIMEOUT = 300

type Push struct {
	queue_name string
	content    []byte
}

type PushService struct {
	pwt        chan *Push
	redis_pool *redis.Pool
}

func NewPushService(redis_pool *redis.Pool) *PushService {
	s := &PushService{}
	s.pwt = make(chan *Push, 10000)
	s.redis_pool = redis_pool
	return s
}

func (push_service *PushService) Run() {
	go push_service.Push()
}

func (push_service *PushService) IsROMApp(appid int64) bool {
	return false
}

// 离线消息入apns队列
func (push_service *PushService) PublishPeerMessage(appid int64, im *IMMessage) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	var queue_name string
	if push_service.IsROMApp(appid) {
		queue_name = fmt.Sprintf("push_queue_%d", appid)
	} else {
		queue_name = "push_queue"
	}

	push_service.pushChan(queue_name, b)
}

func (push_service *PushService) PublishCustomerMessageV2(appid int64, im *CustomerMessageV2) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender_appid"] = im.sender_appid
	v["sender"] = im.sender
	v["receiver_appid"] = im.receiver_appid
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	var queue_name string
	if push_service.IsROMApp(appid) {
		queue_name = fmt.Sprintf("customer_push_queue_v2_%d", appid)
	} else {
		queue_name = "customer_push_queue_v2"
	}

	push_service.pushChan(queue_name, b)
}

func (push_service *PushService) PublishGroupMessage(appid int64, receivers []int64, im *IMMessage) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receivers"] = receivers
	v["content"] = im.content
	v["group_id"] = im.receiver

	b, _ := json.Marshal(v)
	var queue_name string
	if push_service.IsROMApp(appid) {
		queue_name = fmt.Sprintf("group_push_queue_%d", appid)
	} else {
		queue_name = "group_push_queue"
	}

	push_service.pushChan(queue_name, b)
}

func (push_service *PushService) PublishSystemMessage(appid, receiver int64, sys *SystemMessage) {
	content := sys.notification
	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["content"] = content

	b, _ := json.Marshal(v)
	queue_name := "system_push_queue"

	push_service.pushChan(queue_name, b)
}

func (push_service *PushService) pushChan(queue_name string, b []byte) {
	select {
	case push_service.pwt <- &Push{queue_name, b}:
	default:
		log.Warning("rpush message timeout")
	}
}

func (push_service *PushService) PushQueue(ps []*Push) {
	conn := push_service.redis_pool.Get()
	defer conn.Close()

	begin := time.Now()
	conn.Send("MULTI")
	for _, p := range ps {
		conn.Send("RPUSH", p.queue_name, p.content)
	}
	_, err := conn.Do("EXEC")

	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Info("multi rpush error:", err)
	} else {
		log.Infof("mmulti rpush:%d time:%s success", len(ps), duration)
	}

	if duration > time.Millisecond*PUSH_QUEUE_TIMEOUT {
		log.Warning("multi rpush slow:", duration)
	}
}

func (push_service *PushService) Push() {
	//单次入redis队列消息限制
	const PUSH_LIMIT = 1000
	const WAIT_TIMEOUT = 500

	closed := false
	ps := make([]*Push, 0, PUSH_LIMIT)
	for !closed {
		ps = ps[:0]
		//blocking for first message
		p := <-push_service.pwt
		if p == nil {
			closed = true
			break
		}
		ps = append(ps, p)

		//non blocking
	Loop1:
		for !closed {
			select {
			case p := <-push_service.pwt:
				if p == nil {
					closed = true
				} else {
					ps = append(ps, p)
					if len(ps) >= PUSH_LIMIT {
						break Loop1
					}
				}
			default:
				break Loop1
			}
		}

		if closed {
			push_service.PushQueue(ps)
			return
		}

		if len(ps) >= PUSH_LIMIT {
			push_service.PushQueue(ps)
			continue
		}

		//blocking with timeout
		begin := time.Now()
		end := begin.Add(time.Millisecond * WAIT_TIMEOUT)
	Loop2:
		for !closed {
			now := time.Now()
			if !end.After(now) {
				break
			}
			d := end.Sub(now)
			select {
			case p := <-push_service.pwt:
				if p == nil {
					closed = true
				} else {
					ps = append(ps, p)
					if len(ps) >= PUSH_LIMIT {
						break Loop2
					}
				}
			case <-time.After(d):
				break Loop2
			}
		}

		if len(ps) > 0 {
			push_service.PushQueue(ps)
		}
	}
}
