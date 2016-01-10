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

const CS_MODE_FIX = 1
const CS_MODE_ONLINE = 2
const CS_MODE_BROADCAST = 3

type CSClient struct {
	*Connection
}

func (client *CSClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_CUSTOMER_SERVICE:
		client.HandleCustomerService(msg.body.(*CustomerServiceMessage), msg.seq)
	}
}

func (client *CSClient) HandleCustomerService(cs *CustomerServiceMessage, seq int) {
	group_id, mode := customer_service.GetApplicationConfig(client.appid)

	group := group_manager.FindGroup(group_id)
	if group == nil {
		log.Warning("can't find group:", group_id)
		return
	}

	if mode != CS_MODE_BROADCAST {
		log.Warning("do not support customer service mode:", mode)
		return
	}

	//来自客服人员的回复消息，接受者的id不能为0
	if group.IsMember(cs.sender) && cs.receiver == 0 {
		log.Warning("customer service message receiver is 0")
		return
	}

	cs.timestamp = int32(time.Now().Unix())
	m := &Message{cmd:MSG_CUSTOMER_SERVICE, body:cs}
	members := group.Members()
	for member := range members {
		_, err := SaveMessage(client.appid, member, client.device_ID, m)
		if err != nil {
			log.Error("save message err:", err)
			return
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
		return
	}

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
}
