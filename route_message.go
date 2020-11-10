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

//路由服务器消息
const MSG_SUBSCRIBE = 130
const MSG_UNSUBSCRIBE = 131
const MSG_PUBLISH = 132

const MSG_PUSH = 134
const MSG_PUBLISH_GROUP = 135

const MSG_SUBSCRIBE_ROOM = 136
const MSG_UNSUBSCRIBE_ROOM = 137
const MSG_PUBLISH_ROOM = 138

func init() {
	message_creators[MSG_SUBSCRIBE] = func()IMessage{return new(SubscribeMessage)}
	message_creators[MSG_UNSUBSCRIBE] = func()IMessage{return new(AppUserID)}
	message_creators[MSG_PUBLISH] = func()IMessage{return new(AppMessage)}

	message_creators[MSG_PUSH] = func()IMessage{return new(BatchPushMessage)}	
	message_creators[MSG_PUBLISH_GROUP] = func()IMessage{return new(AppMessage)}
	
	message_creators[MSG_SUBSCRIBE_ROOM] = func()IMessage{return new(AppRoomID)}
	message_creators[MSG_UNSUBSCRIBE_ROOM] = func()IMessage{return new(AppRoomID)}
	message_creators[MSG_PUBLISH_ROOM] = func()IMessage{return new(AppMessage)}

	message_descriptions[MSG_SUBSCRIBE] = "MSG_SUBSCRIBE"
	message_descriptions[MSG_UNSUBSCRIBE] = "MSG_UNSUBSCRIBE"
	message_descriptions[MSG_PUBLISH] = "MSG_PUBLISH"

	message_descriptions[MSG_PUSH] = "MSG_PUSH"		
	message_descriptions[MSG_PUBLISH_GROUP] = "MSG_PUBLISH_GROUP"

	message_descriptions[MSG_SUBSCRIBE_ROOM] = "MSG_SUBSCRIBE_ROOM"
	message_descriptions[MSG_UNSUBSCRIBE_ROOM] = "MSG_UNSUBSCRIBE_ROOM"
	message_descriptions[MSG_PUBLISH_ROOM] = "MSG_PUBLISH_ROOM"
}

//批量消息的推送
type BatchPushMessage struct {
	appid    int64
	receivers []int64		
	msg      *Message
}


func (amsg *BatchPushMessage) ToData() []byte {
	if amsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, amsg.appid)

	var count uint16
	count = uint16(len(amsg.receivers))
	binary.Write(buffer, binary.BigEndian, count)

	for _, receiver := range amsg.receivers {
		binary.Write(buffer, binary.BigEndian, receiver)
	}
	
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, amsg.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	buf := buffer.Bytes()
	return buf
}

func (amsg *BatchPushMessage) FromData(buff []byte) bool {
	if len(buff) < 12 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &amsg.appid)

	var count uint16
	binary.Read(buffer, binary.BigEndian, &count)

	if len(buff) < 8+2+int(count)*8+2 {
		return false
	}

	receivers := make([]int64, 0, count)
	for i := 0; i < int(count); i++ {
		var receiver int64
		binary.Read(buffer, binary.BigEndian, &receiver)
		receivers = append(receivers, receiver)
	}
	amsg.receivers = receivers

	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || l < 0 {
		return false
	}

	msg_buf := make([]byte, l)
	buffer.Read(msg_buf)

	mbuffer := bytes.NewBuffer(msg_buf)
	//recusive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return false
	}
	amsg.msg = msg

	return true
}


type AppMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	prev_msgid int64
	device_id int64
	timestamp int64 //纳秒,测试消息从im->imr->im的时间
	msg      *Message
}


func (amsg *AppMessage) ToData() []byte {
	if amsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, amsg.appid)
	binary.Write(buffer, binary.BigEndian, amsg.receiver)
	binary.Write(buffer, binary.BigEndian, amsg.msgid)
	binary.Write(buffer, binary.BigEndian, amsg.device_id)
	binary.Write(buffer, binary.BigEndian, amsg.timestamp)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, amsg.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	buf := buffer.Bytes()
	return buf
}

func (amsg *AppMessage) FromData(buff []byte) bool {
	if len(buff) < 42 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &amsg.appid)
	binary.Read(buffer, binary.BigEndian, &amsg.receiver)
	binary.Read(buffer, binary.BigEndian, &amsg.msgid)
	binary.Read(buffer, binary.BigEndian, &amsg.device_id)
	binary.Read(buffer, binary.BigEndian, &amsg.timestamp)	

	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || l < 0 {
		return false
	}

	msg_buf := make([]byte, l)
	buffer.Read(msg_buf)

	mbuffer := bytes.NewBuffer(msg_buf)
	//recusive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return false
	}
	amsg.msg = msg

	return true
}


type SubscribeMessage struct {
	appid    int64
	uid      int64
	online   int8 //1 or 0
}

func (sub *SubscribeMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, sub.appid)
	binary.Write(buffer, binary.BigEndian, sub.uid)
	binary.Write(buffer, binary.BigEndian, sub.online)	
	buf := buffer.Bytes()
	return buf
}

func (sub *SubscribeMessage) FromData(buff []byte) bool {
	if len(buff) < 17 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &sub.appid)
	binary.Read(buffer, binary.BigEndian, &sub.uid)
	binary.Read(buffer, binary.BigEndian, &sub.online)
	
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

