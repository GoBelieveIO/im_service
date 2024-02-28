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

// 接入服务器消息
const MSG_AUTH_STATUS = 3
const MSG_ACK = 5
const MSG_PING = 13
const MSG_PONG = 14
const MSG_AUTH_TOKEN = 15
const MSG_RT = 17
const MSG_ENTER_ROOM = 18
const MSG_LEAVE_ROOM = 19
const MSG_ROOM_IM = 20
const MSG_UNREAD_COUNT = 22

// persistent, deprecated
const MSG_CUSTOMER_SERVICE_ = 23

// 客户端->服务端
const MSG_SYNC = 26 //同步消息
// 服务端->客服端
const MSG_SYNC_BEGIN = 27
const MSG_SYNC_END = 28

// 通知客户端有新消息
const MSG_SYNC_NOTIFY = 29

// 客户端->服务端
const MSG_SYNC_GROUP = 30 //同步超级群消息
// 服务端->客服端
const MSG_SYNC_GROUP_BEGIN = 31
const MSG_SYNC_GROUP_END = 32

// 通知客户端有新消息
const MSG_SYNC_GROUP_NOTIFY = 33

// 客服端->服务端,更新服务器的synckey
const MSG_SYNC_KEY = 34
const MSG_GROUP_SYNC_KEY = 35

// 系统通知消息, unpersistent
const MSG_NOTIFICATION = 36

// 消息的meta信息
const MSG_METADATA = 37

// im实例使用
const MSG_PENDING_GROUP_MESSAGE = 251

// 服务器消息, 被所有服务器使用
// persistent 点对点消息
const MSG_IM = 4

// persistent
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8

// persistent
const MSG_SYSTEM = 21

// persistent, deprecated
const MSG_CUSTOMER_ = 24         //顾客->客服
const MSG_CUSTOMER_SUPPORT_ = 25 //客服->顾客

// persistent 不同app间的点对点消息
const MSG_CUSTOMER_V2 = 64

// 路由服务器消息
const MSG_SUBSCRIBE = 130
const MSG_UNSUBSCRIBE = 131
const MSG_PUBLISH = 132

const MSG_PUSH = 134
const MSG_PUBLISH_GROUP = 135

const MSG_SUBSCRIBE_ROOM = 136
const MSG_UNSUBSCRIBE_ROOM = 137
const MSG_PUBLISH_ROOM = 138

func init() {
	message_creators[MSG_GROUP_NOTIFICATION] = func() IMessage { return new(GroupNotification) }
	message_creators[MSG_SYSTEM] = func() IMessage { return new(SystemMessage) }
	message_creators[MSG_CUSTOMER_] = func() IMessage { return new(IgnoreMessage) }
	message_creators[MSG_CUSTOMER_SUPPORT_] = func() IMessage { return new(IgnoreMessage) }
	message_creators[MSG_CUSTOMER_V2] = func() IMessage { return new(CustomerMessageV2) }

	vmessage_creators[MSG_GROUP_IM] = func() IVersionMessage { return new(IMMessage) }
	vmessage_creators[MSG_IM] = func() IVersionMessage { return new(IMMessage) }

	message_descriptions[MSG_IM] = "MSG_IM"
	message_descriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	message_descriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	message_descriptions[MSG_SYSTEM] = "MSG_SYSTEM"
	message_descriptions[MSG_CUSTOMER_] = "MSG_CUSTOMER"
	message_descriptions[MSG_CUSTOMER_SUPPORT_] = "MSG_CUSTOMER_SUPPORT"
	message_descriptions[MSG_CUSTOMER_V2] = "MSG_CUSTOMER_V2"

	external_messages[MSG_IM] = true
	external_messages[MSG_GROUP_IM] = true
	external_messages[MSG_CUSTOMER_] = true
	external_messages[MSG_CUSTOMER_SUPPORT_] = true
	external_messages[MSG_CUSTOMER_V2] = true
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

type CustomerMessageV2 struct {
	IMMessage
	sender_appid   int64
	receiver_appid int64
}

func (im *CustomerMessageV2) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, im.sender_appid)
	binary.Write(buffer, binary.BigEndian, im.sender)
	binary.Write(buffer, binary.BigEndian, im.receiver_appid)
	binary.Write(buffer, binary.BigEndian, im.receiver)
	binary.Write(buffer, binary.BigEndian, im.timestamp)
	buffer.Write([]byte(im.content))
	buf := buffer.Bytes()
	return buf
}

func (im *CustomerMessageV2) FromData(buff []byte) bool {
	if len(buff) < 36 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender_appid)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver_appid)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.timestamp)

	im.content = string(buff[36:])

	return true
}
