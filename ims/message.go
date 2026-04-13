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

import (
	"github.com/GoBelieveIO/im_service/protocol"
)

func init() {
	protocol.RegisterMessageCreator(protocol.MSG_GROUP_NOTIFICATION, func() protocol.IMessage { return new(RawMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_SYSTEM, func() protocol.IMessage { return new(RawMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_CUSTOMER_V2, func() protocol.IMessage { return new(RawMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_GROUP_IM, func() protocol.IMessage { return new(RawMessage) })
	protocol.RegisterMessageCreator(protocol.MSG_IM, func() protocol.IMessage { return new(RawMessage) })
}

// 保存在磁盘中但不再需要处理的消息
type RawMessage struct {
	raw []byte
}

func (raw *RawMessage) ToData() []byte {
	return raw.raw
}

func (raw *RawMessage) FromData(buff []byte) bool {
	raw.raw = buff
	return true
}
