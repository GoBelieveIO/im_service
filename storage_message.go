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



//存储服务器命令消息 deprecated
const MSG_SAVE_AND_ENQUEUE = 200
const MSG_DEQUEUE = 201
const MSG_LOAD_OFFLINE = 202
const MSG_LOAD_GROUP_OFFLINE = 203
const MSG_RESULT = 204
const MSG_LOAD_LATEST = 205
const MSG_SAVE_AND_ENQUEUE_GROUP = 206
const MSG_DEQUEUE_GROUP = 207
const MSG_LOAD_HISTORY = 208
const MSG_GET_OFFLINE_COUNT = 211
const MSG_GET_GROUP_OFFLINE_COUNT = 212


//主从同步消息
const MSG_STORAGE_SYNC_BEGIN = 220
const MSG_STORAGE_SYNC_MESSAGE = 221
const MSG_STORAGE_SYNC_MESSAGE_BATCH = 222


//内部文件存储使用
//个人消息队列 代替MSG_OFFLINE_V3
const MSG_OFFLINE_V4 = 248

//个人消息队列 代替MSG_OFFLINE_V2
const MSG_OFFLINE_V3 = 249

//个人消息队列 代替MSG_OFFLINE
//deprecated  兼容性
const MSG_OFFLINE_V2 = 250  

//im实例使用
const MSG_PENDING_GROUP_MESSAGE = 251

//超级群消息队列
const MSG_GROUP_IM_LIST = 252

//deprecated
const MSG_GROUP_ACK_IN = 253

//deprecated 兼容性
const MSG_OFFLINE = 254

//deprecated
const MSG_ACK_IN = 255


func init() {
	message_creators[MSG_SAVE_AND_ENQUEUE] = func()IMessage{return new(SAEMessage)}
	message_creators[MSG_DEQUEUE] = func()IMessage{return new(DQMessage)}
	message_creators[MSG_LOAD_OFFLINE] = func()IMessage{return new(LoadOffline)}
	message_creators[MSG_LOAD_GROUP_OFFLINE] = func()IMessage{return new(LoadGroupOffline)}
	message_creators[MSG_RESULT] = func()IMessage{return new(MessageResult)}
	message_creators[MSG_LOAD_LATEST] = func()IMessage{return new(LoadLatest)}
	message_creators[MSG_LOAD_HISTORY] = func()IMessage{return new(LoadHistory)}
	message_creators[MSG_SAVE_AND_ENQUEUE_GROUP] = func()IMessage{return new(SAEMessage)}
	message_creators[MSG_DEQUEUE_GROUP] = func()IMessage{return new(DQGroupMessage)}
	message_creators[MSG_GET_OFFLINE_COUNT] = func()IMessage{return new(LoadOffline)}
	message_creators[MSG_GET_GROUP_OFFLINE_COUNT] = func()IMessage{return new(LoadGroupOffline)}

	message_creators[MSG_OFFLINE_V4] = func()IMessage{return new (OfflineMessage4)}	
	message_creators[MSG_OFFLINE_V3] = func()IMessage{return new (OfflineMessage3)}
	message_creators[MSG_OFFLINE_V2] = func()IMessage{return new (OfflineMessage2)}
	message_creators[MSG_PENDING_GROUP_MESSAGE] = func()IMessage{return new (PendingGroupMessage)}
	message_creators[MSG_GROUP_IM_LIST] = func()IMessage{return new(GroupOfflineMessage)}
	message_creators[MSG_GROUP_ACK_IN] = func()IMessage{return new(GroupOfflineMessage)}

	message_creators[MSG_OFFLINE] = func()IMessage{return new(OfflineMessage)}
	message_creators[MSG_ACK_IN] = func()IMessage{return new(MessageACKIn)}

	message_creators[MSG_STORAGE_SYNC_BEGIN] = func()IMessage{return new(SyncCursor)}
	message_creators[MSG_STORAGE_SYNC_MESSAGE] = func()IMessage{return new(EMessage)}
	message_creators[MSG_STORAGE_SYNC_MESSAGE_BATCH] = func()IMessage{return new(MessageBatch)}

	message_descriptions[MSG_SAVE_AND_ENQUEUE] = "MSG_SAVE_AND_ENQUEUE"
	message_descriptions[MSG_DEQUEUE] = "MSG_DEQUEUE"
	message_descriptions[MSG_LOAD_OFFLINE] = "MSG_LOAD_OFFLINE"
	message_descriptions[MSG_RESULT] = "MSG_RESULT"
	message_descriptions[MSG_LOAD_LATEST] = "MSG_LOAD_LATEST"
	message_descriptions[MSG_LOAD_HISTORY] = "MSG_LOAD_HISTORY"
	message_descriptions[MSG_SAVE_AND_ENQUEUE_GROUP] = "MSG_SAVE_AND_ENQUEUE_GROUP"
	message_descriptions[MSG_DEQUEUE_GROUP] = "MSG_DEQUEUE_GROUP"
	

	message_descriptions[MSG_STORAGE_SYNC_BEGIN] = "MSG_STORAGE_SYNC_BEGIN"
	message_descriptions[MSG_STORAGE_SYNC_MESSAGE] = "MSG_STORAGE_SYNC_MESSAGE"
	message_descriptions[MSG_STORAGE_SYNC_MESSAGE_BATCH] = "MSG_STORAGE_SYNC_MESSAGE_BATCH"

	message_descriptions[MSG_OFFLINE_V4] = "MSG_OFFLINE_V4"		
	message_descriptions[MSG_OFFLINE_V3] = "MSG_OFFLINE_V3"	
	message_descriptions[MSG_OFFLINE_V2] = "MSG_OFFLINE_V2"	
	message_descriptions[MSG_PENDING_GROUP_MESSAGE] = "MSG_PENDING_GROUP_MESSAGE"
	message_descriptions[MSG_GROUP_IM_LIST] = "MSG_GROUP_IM_LIST"

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
	device_id int64
	msg   *Message
}

func (emsg *EMessage) ToData() []byte {
	if emsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	binary.Write(buffer, binary.BigEndian, emsg.device_id)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, emsg.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)
	buf := buffer.Bytes()
	return buf
	
}

func (emsg *EMessage) FromData(buff []byte) bool {
	if len(buff) < 18 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &emsg.msgid)
	binary.Read(buffer, binary.BigEndian, &emsg.device_id)
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

type MessageBatch struct {
	first_id int64
	last_id  int64
	msgs     []*Message
}

func (batch *MessageBatch) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, batch.first_id)
	binary.Write(buffer, binary.BigEndian, batch.last_id)
	count := int32(len(batch.msgs))
	binary.Write(buffer, binary.BigEndian, count)

	for _, m := range batch.msgs {
		SendMessage(buffer, m)
	}

	buf := buffer.Bytes()
	return buf
}

func (batch *MessageBatch) FromData(buff []byte) bool {
	if len(buff) < 18 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &batch.first_id)
	binary.Read(buffer, binary.BigEndian, &batch.last_id)

	var count int32
	binary.Read(buffer, binary.BigEndian, &count)

	batch.msgs = make([]*Message, 0, count)
	for i := 0; i < int(count); i++ {
		msg := ReceiveMessage(buffer)
		if msg == nil {
			return false
		}
		batch.msgs = append(batch.msgs, msg)
	}

	return true
}

//兼容性
type OfflineMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	device_id int64
	prev_msgid  int64 
}


func (off *OfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
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
	if len(buff) == 40 {
		binary.Read(buffer, binary.BigEndian, &off.device_id)
	}
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	return true
}


type OfflineMessage2 struct {
	appid    int64
	receiver int64
	msgid    int64
	device_id int64
	prev_msgid  int64 	//个人消息队列(点对点消息，群组消息)
	prev_peer_msgid int64 //点对点消息队列
}


func (off *OfflineMessage2) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_peer_msgid)	
	buf := buffer.Bytes()
	return buf
}

func (off *OfflineMessage2) FromData(buff []byte) bool {
	if len(buff) < 48 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.device_id)
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_peer_msgid)	
	return true
}


type OfflineMessage3 struct {
	appid    int64
	receiver int64
	msgid    int64      //消息本体的id
	device_id int64
	prev_msgid  int64 	//个人消息队列(点对点消息，群组消息)
	prev_peer_msgid int64 //点对点消息队列
	prev_batch_msgid int64 //0<-1000<-2000<-3000...构成一个消息队列
}


func (off *OfflineMessage3) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_peer_msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_batch_msgid)
	buf := buffer.Bytes()
	return buf
}

func (off *OfflineMessage3) FromData(buff []byte) bool {
	if len(buff) < 56 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.device_id)
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_peer_msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_batch_msgid)
	return true
}

type OfflineMessage4 struct {
	appid    int64
	receiver int64
	msgid    int64      //消息本体的id
	device_id int64
	seq_id   int64      //消息序号, 1,2,3...
	prev_msgid  int64 	//个人消息队列(点对点消息，群组消息)
	prev_peer_msgid int64 //点对点消息队列
	prev_batch_msgid int64 //0<-1000<-2000<-3000...构成一个消息队列
}


func (off *OfflineMessage4) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.seq_id)	
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_peer_msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_batch_msgid)
	buf := buffer.Bytes()
	return buf
}

func (off *OfflineMessage4) FromData(buff []byte) bool {
	if len(buff) < 64 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.device_id)
	binary.Read(buffer, binary.BigEndian, &off.seq_id)	
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_peer_msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_batch_msgid)
	return true
}




type MessageACKIn struct {
	appid    int64
	receiver int64
	msgid    int64
	device_id  int64
}

func (off *MessageACKIn) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	buf := buffer.Bytes()
	return buf
}

func (off *MessageACKIn) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.device_id)
	return true
}


type DQMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	device_id int64
}

func (dq *DQMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, dq.appid)
	binary.Write(buffer, binary.BigEndian, dq.receiver)
	binary.Write(buffer, binary.BigEndian, dq.msgid)
	binary.Write(buffer, binary.BigEndian, dq.device_id)
	buf := buffer.Bytes()
	return buf
}

func (dq *DQMessage) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &dq.appid)
	binary.Read(buffer, binary.BigEndian, &dq.receiver)
	binary.Read(buffer, binary.BigEndian, &dq.msgid)
	binary.Read(buffer, binary.BigEndian, &dq.device_id)
	return true
}

type DQGroupMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	gid      int64
	device_id int64
}

func (dq *DQGroupMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, dq.appid)
	binary.Write(buffer, binary.BigEndian, dq.receiver)
	binary.Write(buffer, binary.BigEndian, dq.msgid)
	binary.Write(buffer, binary.BigEndian, dq.gid)
	binary.Write(buffer, binary.BigEndian, dq.device_id)
	buf := buffer.Bytes()
	return buf
}

func (dq *DQGroupMessage) FromData(buff []byte) bool {
	if len(buff) < 40 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &dq.appid)
	binary.Read(buffer, binary.BigEndian, &dq.receiver)
	binary.Read(buffer, binary.BigEndian, &dq.msgid)
	binary.Read(buffer, binary.BigEndian, &dq.gid)
	binary.Read(buffer, binary.BigEndian, &dq.device_id)
	return true
}


type GroupOfflineMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	gid      int64
	device_id int64
	prev_msgid  int64
}

func (off *GroupOfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.gid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
	buf := buffer.Bytes()
	return buf
}

func (off *GroupOfflineMessage) FromData(buff []byte) bool {
	if len(buff) < 40 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.gid)
	if len(buff) == 48 {
		binary.Read(buffer, binary.BigEndian, &off.device_id)
	}
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	return true
}


type SAEMessage struct {
	msg       *Message
	appid     int64
	receiver  int64
	device_id int64
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
	WriteMessage(mbuffer, sae.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	binary.Write(buffer, binary.BigEndian, sae.appid)
	binary.Write(buffer, binary.BigEndian, sae.receiver)
	binary.Write(buffer, binary.BigEndian, sae.device_id)
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
	
	if buffer.Len() < 24 {
		return false
	}
	binary.Read(buffer, binary.BigEndian, &sae.appid)
	binary.Read(buffer, binary.BigEndian, &sae.receiver)
	binary.Read(buffer, binary.BigEndian, &sae.device_id)
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

type LoadLatest struct {
	appid    int64
	uid      int64	
	limit int32
}


func (lh *LoadLatest) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lh.appid)
	binary.Write(buffer, binary.BigEndian, lh.uid)
	binary.Write(buffer, binary.BigEndian, lh.limit)
	buf := buffer.Bytes()
	return buf
}

func (lh *LoadLatest) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lh.appid)
	binary.Read(buffer, binary.BigEndian, &lh.uid)
	binary.Read(buffer, binary.BigEndian, &lh.limit)
	return true
}

type LoadHistory struct {
	appid  int64
	uid    int64
	msgid  int64
}


func (lh *LoadHistory) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lh.appid)
	binary.Write(buffer, binary.BigEndian, lh.uid)
	binary.Write(buffer, binary.BigEndian, lh.msgid)
	buf := buffer.Bytes()
	return buf
}

func (lh *LoadHistory) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lh.appid)
	binary.Read(buffer, binary.BigEndian, &lh.uid)
	binary.Read(buffer, binary.BigEndian, &lh.msgid)
	return true
}


type LoadOffline struct {
	appid  int64
	uid    int64
	device_id int64
}

func (lo *LoadOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.device_id)
	buf := buffer.Bytes()
	return buf
}

func (lo *LoadOffline) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lo.appid)
	binary.Read(buffer, binary.BigEndian, &lo.uid)
	binary.Read(buffer, binary.BigEndian, &lo.device_id)
	return true
}


type LoadGroupOffline struct {
	appid  int64
	gid    int64
	uid    int64
	device_id int64
}

func (lo *LoadGroupOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.gid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.device_id)
	buf := buffer.Bytes()
	return buf
}

func (lo *LoadGroupOffline) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lo.appid)
	binary.Read(buffer, binary.BigEndian, &lo.gid)
	binary.Read(buffer, binary.BigEndian, &lo.uid)
	binary.Read(buffer, binary.BigEndian, &lo.device_id)
	return true
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
