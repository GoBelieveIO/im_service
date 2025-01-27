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

package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/GoBelieveIO/im_service/protocol"
	. "github.com/GoBelieveIO/im_service/protocol"
)

func init() {

	protocol.RegisterMessageCreator(protocol.MSG_GROUP_OFFLINE, func() IMessage { return new(OfflineMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_OFFLINE_V4, func() IMessage { return new(OfflineMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_OFFLINE_V3_, func() IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_OFFLINE_V2_, func() IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_GROUP_IM_LIST_, func() IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_GROUP_ACK_IN_, func() IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_OFFLINE_, func() IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_ACK_IN_, func() IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_STORAGE_SYNC_BEGIN, func() IMessage { return new(SyncCursor) })
	protocol.RegisterMessageCreator(protocol.MSG_STORAGE_SYNC_MESSAGE, func() IMessage { return new(EMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_STORAGE_SYNC_MESSAGE_BATCH, func() IMessage { return new(MessageBatch) })

}

// 保存在磁盘中但不再需要处理的消息
type IgnoreMessage struct {
}

func (ignore *IgnoreMessage) ToData() []byte {
	return nil
}

func (ignore *IgnoreMessage) FromData(buff []byte) bool {
	return true
}

type EMessage struct {
	MsgId    int64
	DeviceId int64
	Msg      *Message
}

func (emsg *EMessage) ToData() []byte {
	if emsg.Msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.MsgId)
	binary.Write(buffer, binary.BigEndian, emsg.DeviceId)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, emsg.Msg)
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
	binary.Read(buffer, binary.BigEndian, &emsg.MsgId)
	binary.Read(buffer, binary.BigEndian, &emsg.DeviceId)
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
	emsg.Msg = msg

	return true
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
		WriteMessage(buffer, m)
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

type OfflineMessage struct {
	appid            int64
	receiver         int64 //用户id or 群组id
	msgid            int64 //消息本体的id
	device_id        int64
	seq_id           int64 //v4 消息序号, 1,2,3...
	prev_msgid       int64 //个人消息队列(点对点消息，群组消息)
	prev_peer_msgid  int64 //v2 点对点消息队列
	prev_batch_msgid int64 //v3 0<-1000<-2000<-3000...构成一个消息队列
}

func (off *OfflineMessage) ToData() []byte {
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

func (off *OfflineMessage) FromData(buff []byte) bool {
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
