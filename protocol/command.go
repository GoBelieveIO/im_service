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

type Command int

func (cmd Command) String() string {
	c := int(cmd)
	if desc, ok := message_descriptions[c]; ok {
		return desc
	} else {
		return fmt.Sprintf("%d", c)
	}
}
