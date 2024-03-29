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

//平台号
const PLATFORM_IOS = 1
const PLATFORM_ANDROID = 2
const PLATFORM_WEB = 3


const ACK_SUCCESS = 0
const ACK_NOT_MY_FRIEND = 1
const ACK_NOT_YOUR_FRIEND = 2
const ACK_IN_YOUR_BLACKLIST = 3
const ACK_NOT_GROUP_MEMBER = 64
const ACK_GROUP_NONEXIST = 65

//version1:IMMessage添加时间戳字段
//version2:MessageACK添加status字段
func init() {
	message_creators[MSG_AUTH_TOKEN] = func()IMessage{return new(AuthenticationToken)}

	message_creators[MSG_RT] = func()IMessage{return new(RTMessage)}
	message_creators[MSG_ENTER_ROOM] = func()IMessage{return new(Room)}
	message_creators[MSG_LEAVE_ROOM] = func()IMessage{return new(Room)}
	message_creators[MSG_ROOM_IM] = func()IMessage{return &RoomMessage{new(RTMessage)}}

	message_creators[MSG_UNREAD_COUNT] = func()IMessage{return new(MessageUnreadCount)}
	message_creators[MSG_CUSTOMER_SERVICE_] = func()IMessage{return new(IgnoreMessage)}


	message_creators[MSG_SYNC] = func()IMessage{return new(SyncKey)}
	message_creators[MSG_SYNC_BEGIN] = func()IMessage{return new(SyncKey)}
	message_creators[MSG_SYNC_END] = func()IMessage{return new(SyncKey)}
	message_creators[MSG_SYNC_KEY] = func()IMessage{return new(SyncKey)}

	message_creators[MSG_SYNC_GROUP] = func()IMessage{return new(GroupSyncKey)}
	message_creators[MSG_SYNC_GROUP_BEGIN] = func()IMessage{return new(GroupSyncKey)}
	message_creators[MSG_SYNC_GROUP_END] = func()IMessage{return new(GroupSyncKey)}
	message_creators[MSG_GROUP_SYNC_KEY] = func()IMessage{return new(GroupSyncKey)}

	message_creators[MSG_SYNC_NOTIFY] = func()IMessage{return new(SyncNotify)}
	message_creators[MSG_SYNC_GROUP_NOTIFY] = func()IMessage{return new(GroupSyncNotify)}
	
	message_creators[MSG_NOTIFICATION] = func()IMessage{return new(SystemMessage)}
	message_creators[MSG_METADATA] = func()IMessage{return new(Metadata)}

	message_creators[MSG_AUTH_STATUS] = func()IMessage{return new(AuthenticationStatus)}

	
	vmessage_creators[MSG_ACK] = func()IVersionMessage{return new(MessageACK)}	

	message_creators[MSG_PENDING_GROUP_MESSAGE] = func()IMessage{return new (PendingGroupMessage)}
	
	
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
	
	external_messages[MSG_AUTH_TOKEN] = true;
	external_messages[MSG_ACK] = true;
	external_messages[MSG_PING] = true;	
	external_messages[MSG_PONG] = true;
	external_messages[MSG_RT] = true;
	external_messages[MSG_ENTER_ROOM] = true;
	external_messages[MSG_LEAVE_ROOM] = true;
	external_messages[MSG_ROOM_IM] = true;
	external_messages[MSG_UNREAD_COUNT] = true;
	external_messages[MSG_SYNC] = true;
	external_messages[MSG_SYNC_GROUP] = true;
	external_messages[MSG_SYNC_KEY] = true;
	external_messages[MSG_GROUP_SYNC_KEY] = true;
	external_messages[MSG_METADATA] = true;
}



type AuthenticationToken struct {
	token       string
	platform_id int8
	device_id   string
}


func (auth *AuthenticationToken) ToData() []byte {
	var l int8

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.platform_id)

	l = int8(len(auth.token))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.token))

	l = int8(len(auth.device_id))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.device_id))

	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationToken) FromData(buff []byte) bool {
	var l uint8
	if (len(buff) <= 3) {
		return false
	}
	auth.platform_id = int8(buff[0])

	buffer := bytes.NewBuffer(buff[1:])

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	token := make([]byte, l)
	buffer.Read(token)

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	device_id := make([]byte, l)
	buffer.Read(device_id)

	auth.token = string(token)
	auth.device_id = string(device_id)
	return true
}

type AuthenticationStatus struct {
	status int32
}

func (auth *AuthenticationStatus) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.status)
	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationStatus) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.status)
	return true
}


type MessageACK struct {
	seq int32
	status int8
}

func (ack *MessageACK) ToData(version int) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.seq)
	if version > 1 {
		binary.Write(buffer, binary.BigEndian, ack.status)
	}
	buf := buffer.Bytes()
	return buf
}

func (ack *MessageACK) FromData(version int, buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.seq)
	if version > 1 {
		binary.Read(buffer, binary.BigEndian, &ack.status)
	}
	return true
}

type MessageUnreadCount struct {
	count int32
}

func (u *MessageUnreadCount) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, u.count)
	buf := buffer.Bytes()
	return buf
}

func (u *MessageUnreadCount) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &u.count)
	return true
}


type Room int64
func (room *Room) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int64(*room))
	buf := buffer.Bytes()
	return buf	
}

func (room *Room) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, (*int64)(room))
	return true
}

func (room *Room) RoomID() int64 {
	return int64(*room)
}



type SyncKey struct {
	sync_key int64
}

func (id *SyncKey) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.sync_key)
	buf := buffer.Bytes()
	return buf
}

func (id *SyncKey) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.sync_key)
	return true
}


type SyncNotify = SyncKey


type GroupSyncKey struct {
	group_id int64
	sync_key int64
}


func (id *GroupSyncKey) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.group_id)
	binary.Write(buffer, binary.BigEndian, id.sync_key)
	buf := buffer.Bytes()
	return buf
}

func (id *GroupSyncKey) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &id.group_id)
	binary.Read(buffer, binary.BigEndian, &id.sync_key)
	return true
}

type GroupSyncNotify = GroupSyncKey

type Metadata struct {
	sync_key int64
	prev_sync_key int64
}


func (sync *Metadata) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, sync.sync_key)
	binary.Write(buffer, binary.BigEndian, sync.prev_sync_key)
	padding := [16]byte{}
	buffer.Write(padding[:])
	buf := buffer.Bytes()
	return buf
}

func (sync *Metadata) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &sync.sync_key)
	binary.Read(buffer, binary.BigEndian, &sync.prev_sync_key)
	return true
}



type RTMessage struct {
	sender    int64
	receiver  int64
	content   string
}
func (message *RTMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (rt *RTMessage) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &rt.sender)
	binary.Read(buffer, binary.BigEndian, &rt.receiver)
	rt.content = string(buff[16:])
	return true
}


type RoomMessage struct {
	*RTMessage
}



//待发送的群组消息临时存储结构
type PendingGroupMessage struct {
    appid     int64
	sender    int64
	device_ID int64 //发送者的设备id
	gid       int64
	timestamp int32

	members   []int64 //需要接受此消息的成员列表
	content   string
}

func (gm *PendingGroupMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, gm.appid)
	binary.Write(buffer, binary.BigEndian, gm.sender)
	binary.Write(buffer, binary.BigEndian, gm.device_ID)
	binary.Write(buffer, binary.BigEndian, gm.gid)
	binary.Write(buffer, binary.BigEndian, gm.timestamp)

	count := int16(len(gm.members))
	binary.Write(buffer, binary.BigEndian, count)
	for _, uid := range gm.members {
		binary.Write(buffer, binary.BigEndian, uid)
	}
	
	buffer.Write([]byte(gm.content))	
	buf := buffer.Bytes()
	return buf
}

func (gm *PendingGroupMessage) FromData(buff []byte) bool {
	if len(buff) < 38 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &gm.appid)
	binary.Read(buffer, binary.BigEndian, &gm.sender)
	binary.Read(buffer, binary.BigEndian, &gm.device_ID)
	binary.Read(buffer, binary.BigEndian, &gm.gid)
	binary.Read(buffer, binary.BigEndian, &gm.timestamp)

	var count int16
	binary.Read(buffer, binary.BigEndian, &count)

	if len(buff) < int(38 + count*8) {
		return false
	}
	
	gm.members = make([]int64, count)
	for i := 0; i < int(count); i++ {
		var uid int64
		binary.Read(buffer, binary.BigEndian, &uid)
		gm.members[i] = uid
	}
	offset := 38 + count*8
	gm.content = string(buff[offset:])	

	return true
}
