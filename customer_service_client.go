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
	sellers IntSet
}

func NewCSClient(conn *Connection) *CSClient {
	c := &CSClient{Connection:conn}
	c.sellers = NewIntSet()
	return c
}

func (client *CSClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_CUSTOMER:
		client.HandleCustomerMessage(msg)
	case MSG_CUSTOMER_SUPPORT:
		client.HandleCustomerSupportMessage(msg)
	}
}

//客服->顾客
func (client *CSClient) HandleCustomerSupportMessage(msg *Message) {
	cm := msg.body.(*CustomerMessage)
	store, err := customer_service.GetStore(cm.store_id)
	if err != nil {
		log.Warningf("get store:%d err:%s", cm.store_id, err)
		return
	}
	group := group_manager.FindGroup(store.group_id)
	if group == nil {
		log.Warning("can't find group:", store.group_id)
		return
	}

	if client.appid != config.kefu_appid {
		log.Warningf("client appid:%d kefu appid:%d", 
			client.appid, config.kefu_appid)
		return
	}

	cm.timestamp = int32(time.Now().Unix())
	mode := store.mode

	if (mode == CS_MODE_BROADCAST) {
		m := &Message{cmd:MSG_CUSTOMER_SUPPORT, body:cm}
		err = client.Broadcast(m, group)
	} else if (mode == CS_MODE_ONLINE) {
		m := &Message{cmd:MSG_CUSTOMER_SUPPORT, body:cm}
		SaveMessage(client.appid, cm.seller_id, client.device_ID, m)
		_, err = SaveMessage(cm.customer_appid, cm.customer_id, client.device_ID, m)
	} else if (mode == CS_MODE_FIX) {
		m := &Message{cmd:MSG_CUSTOMER_SUPPORT, body:cm}
		SaveMessage(client.appid, cm.seller_id, client.device_ID, m)
		_, err = SaveMessage(cm.customer_appid, cm.customer_id, client.device_ID, m)
	} else {
		log.Warning("do not support customer service mode:", mode)
		return
	}

	if err != nil {
		return
	}

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
}

//顾客->客服
func (client *CSClient) HandleCustomerMessage(msg *Message) {
	cm := msg.body.(*CustomerMessage)
	store, err := customer_service.GetStore(cm.store_id)
	if err != nil {
		log.Warning("get store err:", err)
		return
	}
	group := group_manager.FindGroup(store.group_id)
	if group == nil {
		log.Warning("can't find group:", store.group_id)
		return
	}

	if cm.customer_appid != client.appid {
		log.Warningf("message appid:%d client appid:%d", 
			cm.customer_appid, client.appid)
		return
	}

	cm.timestamp = int32(time.Now().Unix())
	mode := store.mode

	if (mode == CS_MODE_BROADCAST) {
		m := &Message{cmd:MSG_CUSTOMER, body:cm}
		err = client.Broadcast(m, group)
	} else if (mode == CS_MODE_ONLINE) {
		err = client.OnlineSend(cm)
	} else if (mode == CS_MODE_FIX) {
		err = client.FixSend(cm, group)
	} else {
		log.Warning("do not support customer service mode:", mode)
		return
	}

	if err != nil {
		return
	}

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
}


func (client *CSClient) OnlineSend(cs *CustomerMessage) error {
	m := &Message{cmd:MSG_CUSTOMER, body:cs}
	//普通用户发送的消息
	if cs.seller_id != 0 && !client.sellers.IsMember(cs.seller_id) {
		//判断上次会话的客服人员是否还在线
		is_on := customer_service.IsOnline(cs.store_id, cs.seller_id)
		if is_on {
			log.Info("online....")
			client.sellers.Add(cs.seller_id)
		}
	}

	if !client.sellers.IsMember(cs.seller_id) {
		//重新分配新的客服人员
		seller_id := customer_service.GetOnlineSellerID(cs.store_id)
		if seller_id == 0 {
			log.Warning("can't get a online seller")
			return errors.New("can't get a online seller")
		}
		client.sellers.Add(seller_id)
		log.Infof("new seller id:%d", seller_id)
		cs.seller_id = seller_id
	}

	SaveMessage(cs.customer_appid, cs.customer_id, client.device_ID, m)
	_, err := SaveMessage(config.kefu_appid, cs.seller_id, client.device_ID, m)
	return err
}

func (client *CSClient) FixSend(cs *CustomerMessage, group *Group) error {
	m := &Message{cmd:MSG_CUSTOMER, body:cs}
	if (cs.seller_id != 0 && !client.sellers.IsMember(cs.seller_id)) {
		//判断上次会话的客服人员是否已经被移除
		if group.IsMember(cs.seller_id) {
			client.sellers.Add(cs.seller_id)
		}
	}

	if !client.sellers.IsMember(cs.seller_id) {
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
		cs.seller_id = m[i]
		client.sellers.Add(m[i])
		log.Infof("customer message seller id:%d", cs.seller_id)
	}

	SaveMessage(cs.customer_appid, cs.customer_id, client.device_ID, m)
	_, err := SaveMessage(config.kefu_appid, cs.seller_id, client.device_ID, m)
	return err
}

func (client *CSClient) Broadcast(m *Message, group *Group) error {
	cs := m.body.(*CustomerMessage)
	members := group.Members()
	for member := range members {
		_, err := SaveMessage(config.kefu_appid, member, client.device_ID, m)
		if err != nil {
			log.Error("save message err:", err)
			return err
		}
	}
	_, err := SaveMessage(cs.customer_appid, cs.customer_id, client.device_ID, m)

	if err != nil {
		log.Error("save message err:", err)
		return err
	}
	return nil
}
