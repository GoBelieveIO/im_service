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
import "context"
import "encoding/json"
import log "github.com/sirupsen/logrus"

const PUSH_QUEUE_TIMEOUT = 300

func (client *Client) IsROMApp(appid int64) bool {
	return false
}


//离线消息入apns队列
func (client *Client) PublishPeerMessage(appid int64, im *IMMessage) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("push_queue_%d", appid)
	} else {
		queue_name = "push_queue"
	}
	
	client.PushChan(queue_name, b)		
}

func (client *Client) PublishCustomerMessageV2(appid int64, im *CustomerMessageV2) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender_appid"] = im.sender_appid
	v["sender"] = im.sender
	v["receiver_appid"] = im.receiver_appid
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("customer_push_queue_v2_%d", appid)
	} else {
		queue_name = "customer_push_queue_v2"
	}
	
	client.PushChan(queue_name, b)		
}

func (client *Client) PublishGroupMessage(appid int64, receivers []int64, im *IMMessage) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receivers"] = receivers
	v["content"] = im.content
	v["group_id"] = im.receiver

	b, _ := json.Marshal(v)
	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("group_push_queue_%d", appid)
	} else {
		queue_name = "group_push_queue"
	}
	
	client.PushChan(queue_name, b)	
}

func (client *Client) PublishSystemMessage(appid, receiver int64, content string) {
	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["content"] = content

	b, _ := json.Marshal(v)
	var queue_name string
	queue_name = "system_push_queue"

	client.PushChan(queue_name, b)
}

func (client *Client) PushChan(queue_name string, b []byte) {
	select {
	case client.pwt <- &Push{queue_name, b}:
	default:
		log.Warning("rpush message timeout")		
	}	
}

func (client *Client) PushQueue(ps []*Push) {
	var ctx = context.Background()

	begin := time.Now()

	pipe := redis_client.Pipeline()
	for _, p := range(ps) {
		pipe.RPush(ctx, p.queue_name, p.content)
	}
	_, err := pipe.Exec(ctx)
	
	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Info("multi rpush error:", err)
	} else {
		log.Infof("mmulti rpush:%d time:%s success", len(ps), duration)
	}

	if  duration > time.Millisecond*PUSH_QUEUE_TIMEOUT {
		log.Warning("multi rpush slow:", duration)
	}
}

func (client *Client) Push() {
	//单次入redis队列消息限制
	const PUSH_LIMIT = 1000
	const WAIT_TIMEOUT = 500
	
	closed := false
	ps := make([]*Push, 0, PUSH_LIMIT)
	for !closed {
		ps = ps[:0]
		//blocking for first message
		p := <- client.pwt
		if p == nil {
			closed = true
			break
		}
		ps = append(ps, p)
		
		//non blocking
	Loop1:
		for !closed {
			select {
			case p := <- client.pwt:
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
			client.PushQueue(ps)
			return
		}

		if len(ps) >= PUSH_LIMIT {
			client.PushQueue(ps)
			continue
		}

		//blocking with timeout		
		begin := time.Now()
		end := begin.Add(time.Millisecond*WAIT_TIMEOUT)		
	Loop2:
		for !closed {
			now := time.Now()
			if !end.After(now) {
				break
			}
			d := end.Sub(now)
			select {
			case p:= <-client.pwt:
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
			client.PushQueue(ps)
		}
	}
}

