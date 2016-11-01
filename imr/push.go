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
import "bytes"
import "encoding/binary"
import "encoding/json"
import log "github.com/golang/glog"


func (client *Client) IsROMApp(appid int64) bool {
	return false
}




//离线消息入apns队列
func (client *Client) PublishPeerMessage(appid int64, im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

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
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

func (client *Client) PublishGroupMessage(appid int64, receivers []int64, im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

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
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

func (client *Client) PublishCustomerMessage(appid, receiver int64, cs *CustomerMessage, cmd int) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["command"] = cmd
	v["customer_appid"] = cs.customer_appid
	v["customer"] = cs.customer_id
	v["seller"] = cs.seller_id
	v["store"] = cs.store_id
	v["content"] = cs.content

	b, _ := json.Marshal(v)
	var queue_name string
	queue_name = "customer_push_queue"
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}


func (client *Client) PublishSystemMessage(appid, receiver int64, content string) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["content"] = content

	b, _ := json.Marshal(v)
	var queue_name string
	queue_name = "system_push_queue"
	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

const VOIP_COMMAND_DIAL = 1
const VOIP_COMMAND_DIAL_VIDEO = 9


func (client *Client) GetDialCount(ctl *VOIPControl) int {
	if len(ctl.content) < 4 {
		return 0
	}

	var ctl_cmd int32
	buffer := bytes.NewBuffer(ctl.content)
	binary.Read(buffer, binary.BigEndian, &ctl_cmd)
	if ctl_cmd != VOIP_COMMAND_DIAL && ctl_cmd != VOIP_COMMAND_DIAL_VIDEO {
		return 0
	}

	if len(ctl.content) < 8 {
		return 0
	}
	var dial_count int32
	binary.Read(buffer, binary.BigEndian, &dial_count)

	return int(dial_count)
}



func (client *Client) PublishVOIPMessage(appid int64, ctl *VOIPControl) {
	//首次拨号时发送apns通知
	count := client.GetDialCount(ctl)
	if count != 1 {
		return
	}

	log.Infof("publish invite notification sender:%d receiver:%d", ctl.sender, ctl.receiver)
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["sender"] = ctl.sender
	v["receiver"] = ctl.receiver
	v["appid"] = appid
	b, _ := json.Marshal(v)

	var queue_name string
	if client.IsROMApp(appid) {
		queue_name = fmt.Sprintf("voip_push_queue_%d", appid)
	} else {
		queue_name = "voip_push_queue"
	}

	_, err := conn.Do("RPUSH", queue_name, b)
	if err != nil {
		log.Info("error:", err)
	}
}
