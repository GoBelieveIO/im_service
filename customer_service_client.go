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

import "time"
import log "github.com/golang/glog"
import "math/rand"
import "errors"

const CS_MODE_FIX = 1
const CS_MODE_ONLINE = 2
const CS_MODE_BROADCAST = 3

type CSClient struct {
	*Connection
	staff_id int64 //客服id
}

func (client *CSClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_CUSTOMER_SERVICE:
		client.HandleCustomerService(msg.body.(*CustomerServiceMessage), msg.seq)
	}
}

func (client *CSClient) HandleCustomerService(cs *CustomerServiceMessage, seq int) {
	if (cs.sender != client.uid) {
		log.Warningf("cs message sender:%d client uid:%d\n", cs.sender, client.uid)
		return
	}

	group_id, mode := customer_service.GetApplicationConfig(client.appid)

	group := group_manager.FindGroup(group_id)
	if group == nil {
		log.Warning("can't find group:", group_id)
		return
	}

	//来自客服人员的回复消息，接受者的id不能为0
	if group.IsMember(cs.sender) && cs.receiver == 0 {
		log.Warning("customer service message receiver is 0")
		return
	}

	cs.timestamp = int32(time.Now().Unix())

	log.Infof("customer service mode:%d", mode)
	var err error = nil
	if (mode == CS_MODE_BROADCAST) {
		err = client.Broadcast(cs, group)
	} else if (mode == CS_MODE_ONLINE) {
		err = client.OnlineSend(cs, group)
	} else if (mode == CS_MODE_FIX) {
		err = client.FixSend(cs, group)
	} else {
		log.Warning("do not support customer service mode:", mode)
		return
	}

	if err != nil {
		return
	}

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
}

func (client *CSClient) OnlineSend(cs *CustomerServiceMessage, group *Group) error {
	m := &Message{cmd:MSG_CUSTOMER_SERVICE, body:cs}
	if group.IsMember(cs.sender) {
		//客服人员发送的消息
		SaveMessage(client.appid, cs.sender, client.device_ID, m)
		_, err := SaveMessage(client.appid, cs.receiver, client.device_ID, m)
		return err
	} else {
		//普通用户发送的消息
		if cs.receiver != 0 && client.staff_id == 0 {
			//判断上次会话的客服人员是否还在线
			is_on := customer_service.IsOnline(client.appid, cs.receiver)
			if is_on {
				log.Info("online....")
				client.staff_id = cs.receiver
			}
		}
		if (client.staff_id == 0) {
			//重新分配新的客服人员
			staff_id := customer_service.GetOnlineStaffID(client.appid)
			if staff_id == 0 {
				log.Warning("can't get a online staff")
				return errors.New("can't get a online staff")
			}
			client.staff_id = staff_id
		}

		log.Infof("customer receiver:%d staff id:%d", cs.receiver, client.staff_id)
		SaveMessage(client.appid, cs.sender, client.device_ID, m)
		_, err := SaveMessage(client.appid, client.staff_id, client.device_ID, m)
		return err
	}
}

func (client *CSClient) FixSend(cs *CustomerServiceMessage, group *Group) error {
	m := &Message{cmd:MSG_CUSTOMER_SERVICE, body:cs}
	if group.IsMember(cs.sender) {
		//客服人员发送的消息
		SaveMessage(client.appid, cs.sender, client.device_ID, m)
		_, err := SaveMessage(client.appid, cs.receiver, client.device_ID, m)
		return err
	} else {
		if (cs.receiver != 0 && client.staff_id == 0) {
			//判断上次会话的客服人员是否已经被移除
			if group.IsMember(cs.receiver) {
				client.staff_id = cs.receiver
			}
		}
		if (client.staff_id == 0) {
			members := group.Members()
			if len(members) == 0 {
				log.Warning("customer service has not staffs")
				return errors.New("customer service has not staffs")
			}
			m := make([]int64, len(members))
			i := 0
			for k, _ := range members {
				m[i] = k
				i++
			}
			
			i = int(rand.Int31n(int32(len(members))))
			client.staff_id = m[i]
		}
		log.Infof("customer receiver:%d staff id:%d", cs.receiver, client.staff_id)
		SaveMessage(client.appid, cs.sender, client.device_ID, m)
		_, err := SaveMessage(client.appid, client.staff_id, client.device_ID, m)
		return err
	}
}

func (client *CSClient) Broadcast(cs *CustomerServiceMessage, group *Group) error {
	m := &Message{cmd:MSG_CUSTOMER_SERVICE, body:cs}
	members := group.Members()
	for member := range members {
		_, err := SaveMessage(client.appid, member, client.device_ID, m)
		if err != nil {
			log.Error("save message err:", err)
			return err
		}
	}

	var err error = nil
	if group.IsMember(cs.sender) {
		_, err = SaveMessage(client.appid, cs.receiver, client.device_ID, m)
	} else {
		_, err = SaveMessage(client.appid, cs.sender, client.device_ID, m)
	}

	if err != nil {
		log.Error("save message err:", err)
		return err
	}
	return nil
}
