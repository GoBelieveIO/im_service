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

package protocol

import "fmt"

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

// 主从同步消息
const MSG_STORAGE_SYNC_BEGIN = 220
const MSG_STORAGE_SYNC_MESSAGE = 221
const MSG_STORAGE_SYNC_MESSAGE_BATCH = 222

// 内部文件存储使用
// 超级群消息队列 代替MSG_GROUP_IM_LIST
const MSG_GROUP_OFFLINE = 247

// 个人消息队列 代替MSG_OFFLINE_V3
const MSG_OFFLINE_V4 = 248

// 个人消息队列 代替MSG_OFFLINE_V2
const MSG_OFFLINE_V3_ = 249

// 个人消息队列 代替MSG_OFFLINE
// deprecated  兼容性
const MSG_OFFLINE_V2_ = 250

// im实例使用
const ___MSG_PENDING_GROUP_MESSAGE___ = 251

// 超级群消息队列
// deprecated 兼容性
const MSG_GROUP_IM_LIST_ = 252

// deprecated
const MSG_GROUP_ACK_IN_ = 253

// deprecated 兼容性
const MSG_OFFLINE_ = 254

// deprecated
const MSG_ACK_IN_ = 255

// ---------------------------------------------------
const DEFAULT_VERSION = 2

// 消息标志
// 文本消息 c <-> s
const MESSAGE_FLAG_TEXT = 0x01

// 消息不持久化 c <-> s
const MESSAGE_FLAG_UNPERSISTENT = 0x02

// 群组消息 c -> s
const MESSAGE_FLAG_GROUP = 0x04

// 离线消息由当前登录的用户在当前设备发出 c <- s
const MESSAGE_FLAG_SELF = 0x08

// 消息由服务器主动推到客户端 c <- s
const MESSAGE_FLAG_PUSH = 0x10

// 超级群消息 c <- s
const MESSAGE_FLAG_SUPER_GROUP = 0x20

const MSG_HEADER_SIZE = 12

var message_descriptions map[int]string = make(map[int]string)

type MessageCreator func() IMessage

var message_creators map[int]MessageCreator = make(map[int]MessageCreator)

type VersionMessageCreator func() IVersionMessage

var vmessage_creators map[int]VersionMessageCreator = make(map[int]VersionMessageCreator)

// true client->server
var external_messages [256]bool

func init() {

	message_descriptions[MSG_AUTH_STATUS] = "MSG_AUTH_STATUS"
	message_descriptions[MSG_ACK] = "MSG_ACK"
	message_descriptions[MSG_PING] = "MSG_PING"
	message_descriptions[MSG_PONG] = "MSG_PONG"
	message_descriptions[MSG_AUTH_TOKEN] = "MSG_AUTH_TOKEN"
	message_descriptions[MSG_RT] = "MSG_RT"
	message_descriptions[MSG_ENTER_ROOM] = "MSG_ENTER_ROOM"
	message_descriptions[MSG_LEAVE_ROOM] = "MSG_LEAVE_ROOM"
	message_descriptions[MSG_ROOM_IM] = "MSG_ROOM_IM"
	message_descriptions[MSG_UNREAD_COUNT] = "MSG_UNREAD_COUNT"
	message_descriptions[MSG_CUSTOMER_SERVICE_] = "MSG_CUSTOMER_SERVICE"

	message_descriptions[MSG_SYNC] = "MSG_SYNC"
	message_descriptions[MSG_SYNC_BEGIN] = "MSG_SYNC_BEGIN"
	message_descriptions[MSG_SYNC_END] = "MSG_SYNC_END"
	message_descriptions[MSG_SYNC_NOTIFY] = "MSG_SYNC_NOTIFY"

	message_descriptions[MSG_SYNC_GROUP] = "MSG_SYNC_GROUP"
	message_descriptions[MSG_SYNC_GROUP_BEGIN] = "MSG_SYNC_GROUP_BEGIN"
	message_descriptions[MSG_SYNC_GROUP_END] = "MSG_SYNC_GROUP_END"
	message_descriptions[MSG_SYNC_GROUP_NOTIFY] = "MSG_SYNC_GROUP_NOTIFY"

	message_descriptions[MSG_NOTIFICATION] = "MSG_NOTIFICATION"
	message_descriptions[MSG_METADATA] = "MSG_METADATA"

	message_descriptions[MSG_PENDING_GROUP_MESSAGE] = "MSG_PENDING_GROUP_MESSAGE"

	external_messages[MSG_AUTH_TOKEN] = true
	external_messages[MSG_ACK] = true
	external_messages[MSG_PING] = true
	external_messages[MSG_PONG] = true
	external_messages[MSG_RT] = true
	external_messages[MSG_ENTER_ROOM] = true
	external_messages[MSG_LEAVE_ROOM] = true
	external_messages[MSG_ROOM_IM] = true
	external_messages[MSG_UNREAD_COUNT] = true
	external_messages[MSG_SYNC] = true
	external_messages[MSG_SYNC_GROUP] = true
	external_messages[MSG_SYNC_KEY] = true
	external_messages[MSG_GROUP_SYNC_KEY] = true
	external_messages[MSG_METADATA] = true

	message_descriptions[MSG_IM] = "MSG_IM"
	message_descriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	message_descriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	message_descriptions[MSG_SYSTEM] = "MSG_SYSTEM"
	message_descriptions[MSG_CUSTOMER_] = "MSG_CUSTOMER"
	message_descriptions[MSG_CUSTOMER_SUPPORT_] = "MSG_CUSTOMER_SUPPORT"
	message_descriptions[MSG_CUSTOMER_V2] = "MSG_CUSTOMER_V2"

	message_descriptions[MSG_SUBSCRIBE] = "MSG_SUBSCRIBE"
	message_descriptions[MSG_UNSUBSCRIBE] = "MSG_UNSUBSCRIBE"
	message_descriptions[MSG_PUBLISH] = "MSG_PUBLISH"

	message_descriptions[MSG_PUSH] = "MSG_PUSH"
	message_descriptions[MSG_PUBLISH_GROUP] = "MSG_PUBLISH_GROUP"

	message_descriptions[MSG_SUBSCRIBE_ROOM] = "MSG_SUBSCRIBE_ROOM"
	message_descriptions[MSG_UNSUBSCRIBE_ROOM] = "MSG_UNSUBSCRIBE_ROOM"
	message_descriptions[MSG_PUBLISH_ROOM] = "MSG_PUBLISH_ROOM"

	message_descriptions[MSG_STORAGE_SYNC_BEGIN] = "MSG_STORAGE_SYNC_BEGIN"
	message_descriptions[MSG_STORAGE_SYNC_MESSAGE] = "MSG_STORAGE_SYNC_MESSAGE"
	message_descriptions[MSG_STORAGE_SYNC_MESSAGE_BATCH] = "MSG_STORAGE_SYNC_MESSAGE_BATCH"

	message_descriptions[MSG_GROUP_OFFLINE] = "MSG_GROUP_OFFLINE"
	message_descriptions[MSG_OFFLINE_V4] = "MSG_OFFLINE_V4"
	message_descriptions[MSG_OFFLINE_V3_] = "MSG_OFFLINE_V3"
	message_descriptions[MSG_OFFLINE_V2_] = "MSG_OFFLINE_V2"
	message_descriptions[MSG_GROUP_IM_LIST_] = "MSG_GROUP_IM_LIST"

	external_messages[MSG_IM] = true
	external_messages[MSG_GROUP_IM] = true
	external_messages[MSG_CUSTOMER_] = true
	external_messages[MSG_CUSTOMER_SUPPORT_] = true
	external_messages[MSG_CUSTOMER_V2] = true
}

type Command int

func (cmd Command) String() string {
	c := int(cmd)
	if desc, ok := message_descriptions[c]; ok {
		return desc
	} else {
		return fmt.Sprintf("%d", c)
	}
}

type IMessage interface {
	ToData() []byte
	FromData(buff []byte) bool
}

type IVersionMessage interface {
	ToData(version int) []byte
	FromData(version int, buff []byte) bool
}

type Message struct {
	Cmd     int
	Seq     int
	Version int
	Flag    int

	Body     interface{}
	BodyData []byte

	Meta interface{} //non searialize
}

func (message *Message) ToData() []byte {
	if message.BodyData != nil {
		return message.BodyData
	} else if message.Body != nil {
		if m, ok := message.Body.(IMessage); ok {
			return m.ToData()
		}
		if m, ok := message.Body.(IVersionMessage); ok {
			return m.ToData(message.Version)
		}
		return nil
	} else {
		return nil
	}
}

func (message *Message) FromData(buff []byte) bool {
	cmd := message.Cmd
	if creator, ok := message_creators[cmd]; ok {
		c := creator()
		r := c.FromData(buff)
		message.Body = c
		return r
	}
	if creator, ok := vmessage_creators[cmd]; ok {
		c := creator()
		r := c.FromData(message.Version, buff)
		message.Body = c
		return r
	}

	return len(buff) == 0
}

func RegisterMessageCreator(cmd int, c MessageCreator) {
	message_creators[int(cmd)] = c
}

func RegisterMessageCreatorV(cmd int, c VersionMessageCreator) {
	vmessage_creators[int(cmd)] = c
}

type MessageTime interface {
	Timestamp() int32
}
