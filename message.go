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

import "bytes"
import "encoding/binary"



//persistent
const MSG_IM = 4

//persistent
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8

//persistent
const MSG_SYSTEM = 21

//persistent
const MSG_CUSTOMER = 24 //顾客->客服
const MSG_CUSTOMER_SUPPORT = 25 //客服->顾客



func init() {
	message_creators[MSG_GROUP_NOTIFICATION] = func()IMessage{return new(GroupNotification)}
	message_creators[MSG_SYSTEM] = func()IMessage{return new(SystemMessage)}
	message_creators[MSG_CUSTOMER] = func()IMessage{return new(CustomerMessage)}
	message_creators[MSG_CUSTOMER_SUPPORT] = func()IMessage{return new(CustomerMessage)}
	vmessage_creators[MSG_GROUP_IM] = func()IVersionMessage{return new(IMMessage)}
	vmessage_creators[MSG_IM] = func()IVersionMessage{return new(IMMessage)}
	
	message_descriptions[MSG_IM] = "MSG_IM"
	message_descriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	message_descriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	message_descriptions[MSG_SYSTEM] = "MSG_SYSTEM"
	message_descriptions[MSG_CUSTOMER] = "MSG_CUSTOMER"
	message_descriptions[MSG_CUSTOMER_SUPPORT] = "MSG_CUSTOMER_SUPPORT"

	external_messages[MSG_IM] = true;
	external_messages[MSG_GROUP_IM] = true;
	external_messages[MSG_CUSTOMER] = true;
	external_messages[MSG_CUSTOMER_SUPPORT] = true;
}


type GroupNotification struct {
	notification string
}

func (notification *GroupNotification) ToData() []byte {
	return []byte(notification.notification)
}
 
func (notification *GroupNotification) FromData(buff []byte) bool {
	notification.notification = string(buff)
	return true
}


type IMMessage struct {
	sender    int64
	receiver  int64
	timestamp int32
	msgid     int32
	content   string
}



func (message *IMMessage) ToDataV0() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV0(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[20:])
	return true
}


func (message *IMMessage) ToDataV1() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.timestamp)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV1(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.timestamp)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[24:])
	return true
}


func (im *IMMessage) ToData(version int) []byte {
	if version == 0 {
		return im.ToDataV0()
	} else {
		return im.ToDataV1()
	}
}

func (im *IMMessage) FromData(version int, buff []byte) bool {
	if version == 0 {
		return im.FromDataV0(buff)
	} else {
		return im.FromDataV1(buff)
	}
}


type SystemMessage struct {
	notification string
}

func (sys *SystemMessage) ToData() []byte {
	return []byte(sys.notification)
}

func (sys *SystemMessage) FromData(buff []byte) bool {
	sys.notification = string(buff)
	return true
}


type CustomerMessage struct {
	customer_appid int64//顾客id所在appid
	customer_id    int64//顾客id
	store_id	   int64
	seller_id	   int64
	timestamp	   int32
	content		   string
}

func (cs *CustomerMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, cs.customer_appid)
	binary.Write(buffer, binary.BigEndian, cs.customer_id)
	binary.Write(buffer, binary.BigEndian, cs.store_id)
	binary.Write(buffer, binary.BigEndian, cs.seller_id)
	binary.Write(buffer, binary.BigEndian, cs.timestamp)
	buffer.Write([]byte(cs.content))
	buf := buffer.Bytes()
	return buf
}

func (cs *CustomerMessage) FromData(buff []byte) bool {
	if len(buff) < 36 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &cs.customer_appid)
	binary.Read(buffer, binary.BigEndian, &cs.customer_id)
	binary.Read(buffer, binary.BigEndian, &cs.store_id)
	binary.Read(buffer, binary.BigEndian, &cs.seller_id)
	binary.Read(buffer, binary.BigEndian, &cs.timestamp)

	cs.content = string(buff[36:])

	return true
}

