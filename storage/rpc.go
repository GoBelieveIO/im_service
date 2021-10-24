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

type PeerMessage struct {
	AppID     int64
	Uid       int64
	DeviceID  int64
	Cmd       int32
	Raw       []byte
}

type PeerGroupMessage struct {
	AppID     int64
	Members   []int64
	DeviceID  int64
	Cmd       int32
	Raw       []byte
}


type GroupMessage struct {
	AppID     int64
	GroupID   int64
	DeviceID  int64
	Cmd       int32
	Raw       []byte
}


type SyncHistory struct {
	AppID     int64
	Uid       int64
	DeviceID  int64
	LastMsgID int64
}

type SyncGroupHistory struct {
	AppID     int64
	Uid       int64
	DeviceID  int64
	GroupID   int64
	LastMsgID int64
	Timestamp int32
}

type HistoryRequest struct {
	AppID     int64
	Uid       int64
	Limit     int32
}


type HistoryMessageID struct {
	MsgID     int64
	PrevMsgID int64
}

type GroupHistoryMessageID struct {
	MessageIDs []*HistoryMessageID
}

type HistoryMessage struct {
	MsgID     int64
	DeviceID  int64   //消息发送者所在的设备ID
	Cmd       int32
	Raw       []byte
}

type PeerHistoryMessage struct {
	Messages []*HistoryMessage
	LastMsgID int64
	HasMore   bool
}

type GroupHistoryMessage PeerHistoryMessage

type LatestMessage struct {
	Messages []*HistoryMessage
}


type RPCStorage interface {
	SyncMessage(sync_key *SyncHistory, result *PeerHistoryMessage) error

	SyncGroupMessage(sync_key *SyncGroupHistory, result *GroupHistoryMessage) error

	SavePeerMessage(m *PeerMessage, result *HistoryMessageID) error

	SavePeerGroupMessage(m *PeerGroupMessage, result *GroupHistoryMessageID) error

	SaveGroupMessage(m *GroupMessage, result *HistoryMessageID) error

	GetNewCount(sync_key *SyncHistory, new_count *int64) error

	GetLatestMessage(r *HistoryRequest, l *LatestMessage) error

	Ping(int, *int) error
}
