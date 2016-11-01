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
import "fmt"

//deprecated
const MSG_HEARTBEAT = 1
const MSG_AUTH = 2

const MSG_AUTH_STATUS = 3
//persistent
const MSG_IM = 4

const MSG_ACK = 5

//deprecated
const MSG_RST = 6

//persistent
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8

//deprecated
const MSG_PEER_ACK = 9
const MSG_INPUTING = 10

//deprecated
const MSG_SUBSCRIBE_ONLINE_STATE = 11
const MSG_ONLINE_STATE = 12

const MSG_PING = 13
const MSG_PONG = 14
const MSG_AUTH_TOKEN = 15
const MSG_LOGIN_POINT = 16
const MSG_RT = 17
const MSG_ENTER_ROOM = 18
const MSG_LEAVE_ROOM = 19
const MSG_ROOM_IM = 20
//persistent
const MSG_SYSTEM = 21

const MSG_UNREAD_COUNT = 22

//persistent, deprecated
const MSG_CUSTOMER_SERVICE_ = 23

//persistent
const MSG_CUSTOMER = 24 //顾客->客服
const MSG_CUSTOMER_SUPPORT = 25 //客服->顾客


//客户端->服务端
const MSG_SYNC = 26 //同步消息
//服务端->客服端
const MSG_SYNC_BEGIN = 27
const MSG_SYNC_END = 28
//通知客户端有新消息
const MSG_SYNC_NOTIFY = 29


//客户端->服务端
const MSG_SYNC_GROUP = 30//同步超级群消息
//服务端->客服端
const MSG_SYNC_GROUP_BEGIN = 31
const MSG_SYNC_GROUP_END = 32
//通知客户端有新消息
const MSG_SYNC_GROUP_NOTIFY = 33




const MSG_VOIP_CONTROL = 64

func init() {
	message_creators[MSG_AUTH] = func()IMessage {return new(Authentication)}
	message_creators[MSG_ACK] = func()IMessage{return new(MessageACK)}
	message_creators[MSG_GROUP_NOTIFICATION] = func()IMessage{return new(GroupNotification)}

	message_creators[MSG_PEER_ACK] = func()IMessage{return new(IgnoreMessage)}
	message_creators[MSG_INPUTING] = func()IMessage{return new(MessageInputing)}
	message_creators[MSG_SUBSCRIBE_ONLINE_STATE] = func()IMessage{return new(IgnoreMessage)}
	message_creators[MSG_ONLINE_STATE] = func()IMessage{return new(IgnoreMessage)}
	message_creators[MSG_AUTH_TOKEN] = func()IMessage{return new(AuthenticationToken)}

	message_creators[MSG_LOGIN_POINT] = func()IMessage{return new(IgnoreMessage)}
	message_creators[MSG_RT] = func()IMessage{return new(RTMessage)}
	message_creators[MSG_ENTER_ROOM] = func()IMessage{return new(Room)}
	message_creators[MSG_LEAVE_ROOM] = func()IMessage{return new(Room)}
	message_creators[MSG_ROOM_IM] = func()IMessage{return &RoomMessage{new(RTMessage)}}
	message_creators[MSG_SYSTEM] = func()IMessage{return new(SystemMessage)}
	message_creators[MSG_UNREAD_COUNT] = func()IMessage{return new(MessageUnreadCount)}
	message_creators[MSG_CUSTOMER_SERVICE_] = func()IMessage{return new(IgnoreMessage)}

	message_creators[MSG_CUSTOMER] = func()IMessage{return new(CustomerMessage)}
	message_creators[MSG_CUSTOMER_SUPPORT] = func()IMessage{return new(CustomerMessage)}


	message_creators[MSG_SYNC] = func()IMessage{return new(SyncKey)}
	message_creators[MSG_SYNC_BEGIN] = func()IMessage{return new(SyncKey)}
	message_creators[MSG_SYNC_END] = func()IMessage{return new(SyncKey)}
	message_creators[MSG_SYNC_NOTIFY] = func()IMessage{return new(SyncKey)}

	message_creators[MSG_SYNC_GROUP] = func()IMessage{return new(GroupSyncKey)}
	message_creators[MSG_SYNC_GROUP_BEGIN] = func()IMessage{return new(GroupSyncKey)}
	message_creators[MSG_SYNC_GROUP_END] = func()IMessage{return new(GroupSyncKey)}
	message_creators[MSG_SYNC_GROUP_NOTIFY] = func()IMessage{return new(GroupSyncKey)}


	message_creators[MSG_VOIP_CONTROL] = func()IMessage{return new(VOIPControl)}

	vmessage_creators[MSG_GROUP_IM] = func()IVersionMessage{return new(IMMessage)}
	vmessage_creators[MSG_IM] = func()IVersionMessage{return new(IMMessage)}

	vmessage_creators[MSG_AUTH_STATUS] = func()IVersionMessage{return new(AuthenticationStatus)}

	message_descriptions[MSG_AUTH] = "MSG_AUTH"
	message_descriptions[MSG_AUTH_STATUS] = "MSG_AUTH_STATUS"
	message_descriptions[MSG_IM] = "MSG_IM"
	message_descriptions[MSG_ACK] = "MSG_ACK"
	message_descriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	message_descriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	message_descriptions[MSG_PEER_ACK] = "MSG_PEER_ACK"
	message_descriptions[MSG_INPUTING] = "MSG_INPUTING"
	message_descriptions[MSG_SUBSCRIBE_ONLINE_STATE] = "MSG_SUBSCRIBE_ONLINE_STATE"
	message_descriptions[MSG_ONLINE_STATE] = "MSG_ONLINE_STATE"
	message_descriptions[MSG_PING] = "MSG_PING"
	message_descriptions[MSG_PONG] = "MSG_PONG"
	message_descriptions[MSG_AUTH_TOKEN] = "MSG_AUTH_TOKEN"
	message_descriptions[MSG_LOGIN_POINT] = "MSG_LOGIN_POINT"
	message_descriptions[MSG_RT] = "MSG_RT"
	message_descriptions[MSG_ENTER_ROOM] = "MSG_ENTER_ROOM"
	message_descriptions[MSG_LEAVE_ROOM] = "MSG_LEAVE_ROOM"
	message_descriptions[MSG_ROOM_IM] = "MSG_ROOM_IM"
	message_descriptions[MSG_SYSTEM] = "MSG_SYSTEM"
	message_descriptions[MSG_UNREAD_COUNT] = "MSG_UNREAD_COUNT"
	message_descriptions[MSG_CUSTOMER_SERVICE_] = "MSG_CUSTOMER_SERVICE"
	message_descriptions[MSG_CUSTOMER] = "MSG_CUSTOMER"
	message_descriptions[MSG_CUSTOMER_SUPPORT] = "MSG_CUSTOMER_SUPPORT"

	message_descriptions[MSG_SYNC] = "MSG_SYNC"
	message_descriptions[MSG_SYNC_BEGIN] = "MSG_SYNC_BEGIN"
	message_descriptions[MSG_SYNC_END] = "MSG_SYNC_END"
	message_descriptions[MSG_SYNC_NOTIFY] = "MSG_SYNC_NOTIFY"

	message_descriptions[MSG_SYNC_GROUP] = "MSG_SYNC_GROUP"
	message_descriptions[MSG_SYNC_GROUP_BEGIN] = "MSG_SYNC_GROUP_BEGIN"
	message_descriptions[MSG_SYNC_GROUP_END] = "MSG_SYNC_GROUP_END"
	message_descriptions[MSG_SYNC_GROUP_NOTIFY] = "MSG_SYNC_GROUP_NOTIFY"

	message_descriptions[MSG_VOIP_CONTROL] = "MSG_VOIP_CONTROL"
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
	cmd  int
	seq  int
	version int
	
	body interface{}
}

func (message *Message) ToData() []byte {
	if message.body != nil {
		if m, ok := message.body.(IMessage); ok {
			return m.ToData()
		}
		if m, ok := message.body.(IVersionMessage); ok {
			return m.ToData(message.version)
		}
		return nil
	} else {
		return nil
	}
}

func (message *Message) FromData(buff []byte) bool {
	cmd := message.cmd
	if creator, ok := message_creators[cmd]; ok {
		c := creator()
		r := c.FromData(buff)
		message.body = c
		return r
	}
	if creator, ok := vmessage_creators[cmd]; ok {
		c := creator()
		r := c.FromData(message.version, buff)
		message.body = c
		return r
	}

	return len(buff) == 0
}

//保存在磁盘中但不再需要处理的消息
type IgnoreMessage struct {
	
}

func (ignore *IgnoreMessage) ToData() []byte {
	return nil
}

func (ignore *IgnoreMessage) FromData(buff []byte) bool {
	return true
}




type Authentication struct {
	uid         int64
}

func (auth *Authentication) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.uid)
	buf := buffer.Bytes()
	return buf
}

func (auth *Authentication) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.uid)
	return true
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
	var l int8
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
	ip int32 //兼容版本0
}

func (auth *AuthenticationStatus) ToData(version int) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.status)
	if version == 0 {
		binary.Write(buffer, binary.BigEndian, auth.ip)
	}
	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationStatus) FromData(version int, buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.status)
	if version == 0 {
		if len(buff) < 8 {
			return false
		}
		binary.Read(buffer, binary.BigEndian, &auth.ip)
	}
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


type MessageACK struct {
	seq int32
}

func (ack *MessageACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.seq)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessageACK) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.seq)
	return true
}


type MessageInputing struct {
	sender   int64
	receiver int64
}

func (inputing *MessageInputing) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, inputing.sender)
	binary.Write(buffer, binary.BigEndian, inputing.receiver)
	buf := buffer.Bytes()
	return buf
}

func (inputing *MessageInputing) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &inputing.sender)
	binary.Read(buffer, binary.BigEndian, &inputing.receiver)
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

type RoomMessage struct {
	*RTMessage
}



type VOIPControl struct {
	sender   int64
	receiver int64
	content  []byte
}

func (ctl *VOIPControl) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ctl.sender)
	binary.Write(buffer, binary.BigEndian, ctl.receiver)
	buffer.Write([]byte(ctl.content))
	buf := buffer.Bytes()
	return buf
}

func (ctl *VOIPControl) FromData(buff []byte) bool {
	if len(buff) <= 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff[:16])
	binary.Read(buffer, binary.BigEndian, &ctl.sender)
	binary.Read(buffer, binary.BigEndian, &ctl.receiver)
	ctl.content = buff[16:]
	return true
}


type AppUserID struct {
	appid    int64
	uid      int64
}

func (id *AppUserID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppUserID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
}

type AppRoomID struct {
	appid    int64
	room_id      int64
}

func (id *AppRoomID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.room_id)
	buf := buffer.Bytes()
	return buf
}

func (id *AppRoomID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.room_id)

	return true
}

type AppGroupMemberID struct {
	appid  int64
	gid    int64
	uid    int64
}

func (id *AppGroupMemberID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.gid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppGroupMemberID) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.gid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
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


