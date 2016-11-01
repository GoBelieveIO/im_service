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


type CustomerClient struct {
	*Connection
}

func NewCustomerClient(conn *Connection) *CustomerClient {
	c := &CustomerClient{Connection:conn}
	return c
}

func (client *CustomerClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_CUSTOMER:
		client.HandleCustomerMessage(msg)
	case MSG_CUSTOMER_SUPPORT:
		client.HandleCustomerSupportMessage(msg)
	}
}

//客服->顾客
func (client *CustomerClient) HandleCustomerSupportMessage(msg *Message) {
	cm := msg.body.(*CustomerMessage)
	if client.appid != config.kefu_appid {
		log.Warningf("client appid:%d kefu appid:%d", 
			client.appid, config.kefu_appid)
		return
	}
	if client.uid != cm.seller_id {
		log.Warningf("uid:%d seller id:%d", client.uid, cm.seller_id)
		return
	}

	cm.timestamp = int32(time.Now().Unix())

	msgid, err := SaveMessage(cm.customer_appid, cm.customer_id, client.device_ID, msg)
	if err != nil {
		log.Warning("save customer support message err:", err)
		return
	}

	msgid2, err := SaveMessage(client.appid, cm.seller_id, client.device_ID, msg)
	if err != nil {
		log.Warning("save customer support message err:", err)
		return
	}

	PushMessage(cm.customer_appid, cm.customer_id, msg)
	
	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(cm.customer_appid, cm.customer_id, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)


	ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
	client.EnqueueMessage(ack)
}

//顾客->客服
func (client *CustomerClient) HandleCustomerMessage(msg *Message) {
	cm := msg.body.(*CustomerMessage)
	cm.timestamp = int32(time.Now().Unix())

	log.Infof("customer message customer appid:%d customer id:%d store id:%d seller id:%d", cm.customer_appid, cm.customer_id, cm.store_id, cm.seller_id)
	if cm.customer_appid != client.appid {
		log.Warningf("message appid:%d client appid:%d", 
			cm.customer_appid, client.appid)
		return
	}
	if cm.customer_id != client.uid {
		log.Warningf("message customer id:%d client uid:%d", 
			cm.customer_id, client.uid)
		return
	}

	if cm.seller_id == 0 {
		log.Warningf("message seller id:0")
		return
	}

	msgid, err := SaveMessage(config.kefu_appid, cm.seller_id, client.device_ID, msg)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	msgid2, err := SaveMessage(cm.customer_appid, cm.customer_id, client.device_ID, msg)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	PushMessage(config.kefu_appid, cm.seller_id, msg)
	
	
	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(config.kefu_appid, cm.seller_id, notify)


	//发送给自己的其它登录点
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)


	ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
	client.EnqueueMessage(ack)
}

