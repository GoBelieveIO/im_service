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
import log "github.com/golang/glog"



//存储服务器消息
const MSG_SAVE_AND_ENQUEUE = 200
const MSG_DEQUEUE = 201
const MSG_LOAD_OFFLINE = 202
const MSG_RESULT = 203
const MSG_LOAD_HISTORY = 204

const MSG_SAVE_AND_ENQUEUE_GROUP = 205
const MSG_DEQUEUE_GROUP = 206
const MSG_LOAD_OFFLINE_GROUP = 207

//主从同步消息
const MSG_SYNC_BEGIN = 210
const MSG_SYNC_MESSAGE = 211

//内部文件存储使用
const MSG_GROUP_OFFLINE = 252
const MSG_GROUP_ACK_IN = 253
const MSG_OFFLINE = 254
const MSG_ACK_IN = 255


func init() {
	message_creators[MSG_SAVE_AND_ENQUEUE] = func()IMessage{return new(SAEMessage)}
	message_creators[MSG_DEQUEUE] = func()IMessage{return new(OfflineMessage)}
	message_creators[MSG_LOAD_OFFLINE] = func()IMessage{return new(AppUserID)}
	message_creators[MSG_RESULT] = func()IMessage{return new(MessageResult)}
	message_creators[MSG_LOAD_HISTORY] = func()IMessage{return new(LoadHistory)}
	
	message_creators[MSG_SAVE_AND_ENQUEUE_GROUP] = func()IMessage{return new(SAEMessage)}
	message_creators[MSG_DEQUEUE_GROUP] = func()IMessage{return new(OfflineMessage)}
	message_creators[MSG_LOAD_OFFLINE_GROUP] = func()IMessage{return new(LoadGroupOffline)}
	
	message_creators[MSG_GROUP_OFFLINE] = func()IMessage{return new(OfflineMessage)}
	message_creators[MSG_GROUP_ACK_IN] = func()IMessage{return new(OfflineMessage)}

	message_creators[MSG_OFFLINE] = func()IMessage{return new(OfflineMessage)}
	message_creators[MSG_ACK_IN] = func()IMessage{return new(OfflineMessage)}

	message_creators[MSG_SYNC_BEGIN] = func()IMessage{return new(SyncCursor)}
	message_creators[MSG_SYNC_MESSAGE] = func()IMessage{return new(EMessage)}
	
	message_descriptions[MSG_SAVE_AND_ENQUEUE] = "MSG_SAVE_AND_ENQUEUE"
	message_descriptions[MSG_DEQUEUE] = "MSG_DEQUEUE"
	message_descriptions[MSG_LOAD_OFFLINE] = "MSG_LOAD_OFFLINE"
	message_descriptions[MSG_RESULT] = "MSG_RESULT"
	message_descriptions[MSG_LOAD_HISTORY] = "MSG_LOAD_HISTORY"

	message_descriptions[MSG_SAVE_AND_ENQUEUE_GROUP] = "MSG_SAVE_AND_ENQUEUE_GROUP"
	message_descriptions[MSG_DEQUEUE_GROUP] = "MSG_DEQUEUE_GROUP"
	message_descriptions[MSG_LOAD_OFFLINE_GROUP] = "MSG_LOAD_OFFLINE_GROUP"

	message_descriptions[MSG_SYNC_BEGIN] = "MSG_SYNC_BEGIN"
	message_descriptions[MSG_SYNC_MESSAGE] = "MSG_SYNC_MESSAGE"

}
type SyncCursor struct {
	msgid int64
}

func (cursor *SyncCursor) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, cursor.msgid)
	return buffer.Bytes()
}

func (cursor *SyncCursor) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &cursor.msgid)
	return true
}

type EMessage struct {
	msgid int64
	msg   *Message
}

func (emsg *EMessage) ToData() []byte {
	if emsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	mbuffer := new(bytes.Buffer)
	SendMessage(mbuffer, emsg.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)
	buf := buffer.Bytes()
	return buf
	
}

func (emsg *EMessage) FromData(buff []byte) bool {
	if len(buff) < 10 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &emsg.msgid)
	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
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
	emsg.msg = msg

	return true
}

type OfflineMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	prev_msgid  int64
}


func (off *OfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
	buf := buffer.Bytes()
	return buf
}

func (off *OfflineMessage) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	return true
}

type DQMessage OfflineMessage

func (dq *DQMessage) GroupID() int64 {
	return dq.prev_msgid
}

type SAEMessage struct {
	msg       *Message
	appid     int64
	receivers []int64
}

func (sae *SAEMessage) ToData() []byte {
	if sae.msg == nil {
		return nil
	}

	if sae.msg.cmd == MSG_SAVE_AND_ENQUEUE {
		log.Warning("recusive sae message")
		return nil
	}

	buffer := new(bytes.Buffer)
	mbuffer := new(bytes.Buffer)
	SendMessage(mbuffer, sae.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	binary.Write(buffer, binary.BigEndian, sae.appid)

	var count int16 = int16(len(sae.receivers))
	binary.Write(buffer, binary.BigEndian, count)
	for _, r := range(sae.receivers) {
		binary.Write(buffer, binary.BigEndian, r)
	}
	buf := buffer.Bytes()
	return buf
}

func (sae *SAEMessage) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
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
	sae.msg = msg

	
	if buffer.Len() < 10 {
		return false
	}
	binary.Read(buffer, binary.BigEndian, &sae.appid)

	var count int16
	binary.Read(buffer, binary.BigEndian, &count)
	if buffer.Len() < int(count)*8 {
		return false
	}
	sae.receivers = make([]int64, count)
	for i := int16(0); i < count; i++ {
		var r int64
		binary.Read(buffer, binary.BigEndian, &r)
		sae.receivers[i] = r
	}
	return true
}

type MessageResult struct {
	status int32
	content []byte
}
func (result *MessageResult) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, result.status)
	buffer.Write(result.content)
	buf := buffer.Bytes()
	return buf
}

func (result *MessageResult) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &result.status)
	result.content = buff[4:]
	return true
}

type LoadHistory struct {
	app_uid AppUserID
	limit int32
}


func (lh *LoadHistory) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lh.app_uid.appid)
	binary.Write(buffer, binary.BigEndian, lh.app_uid.uid)
	binary.Write(buffer, binary.BigEndian, lh.limit)
	buf := buffer.Bytes()
	return buf
}

func (lh *LoadHistory) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lh.app_uid.appid)
	binary.Read(buffer, binary.BigEndian, &lh.app_uid.uid)
	binary.Read(buffer, binary.BigEndian, &lh.limit)
	return true
}

type LoadGroupOffline struct {
	appid  int64
	gid    int64
	uid    int64
	limit  int32
}

func (lo *LoadGroupOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.gid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.limit)
	buf := buffer.Bytes()
	return buf
}

func (lo *LoadGroupOffline) FromData(buff []byte) bool {
	if len(buff) < 28 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lo.appid)
	binary.Read(buffer, binary.BigEndian, &lo.gid)
	binary.Read(buffer, binary.BigEndian, &lo.uid)
	binary.Read(buffer, binary.BigEndian, &lo.limit)
	return true
}
