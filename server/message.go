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

package server

import (
	"bytes"
	"encoding/binary"

	"github.com/GoBelieveIO/im_service/protocol"
)

func init() {

	protocol.RegisterMessageCreator(protocol.MSG_GROUP_NOTIFICATION, func() protocol.IMessage { return new(GroupNotification) })
	protocol.RegisterMessageCreator(protocol.MSG_SYSTEM, func() protocol.IMessage { return new(SystemMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_CUSTOMER_, func() protocol.IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_CUSTOMER_SUPPORT_, func() protocol.IMessage { return new(IgnoreMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_CUSTOMER_V2, func() protocol.IMessage { return new(CustomerMessageV2) })

	protocol.RegisterMessageCreatorV(protocol.MSG_GROUP_IM, func() protocol.IVersionMessage { return new(IMMessage) })
	protocol.RegisterMessageCreatorV(protocol.MSG_IM, func() protocol.IVersionMessage { return new(IMMessage) })

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

func (im *IMMessage) Timestamp() int32 {
	return im.timestamp
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
