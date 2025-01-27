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
	"sync/atomic"

	. "github.com/GoBelieveIO/im_service/protocol"
	rpc_storage "github.com/GoBelieveIO/im_service/storage"
)

type RPCStorage struct {
}

func (rpc *RPCStorage) SyncMessage(sync_key *rpc_storage.SyncHistory, result *rpc_storage.PeerHistoryMessage) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages, last_msgid, hasMore := storage.LoadHistoryMessagesV3(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID, config.limit, config.hard_limit)

	historyMessages := make([]*rpc_storage.HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &rpc_storage.HistoryMessage{}
		hm.MsgID = emsg.MsgId
		hm.DeviceID = emsg.DeviceId
		hm.Cmd = int32(emsg.Msg.Cmd)

		emsg.Msg.Version = DEFAULT_VERSION
		hm.Raw = emsg.Msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	result.Messages = historyMessages
	result.LastMsgID = last_msgid
	result.HasMore = hasMore
	return nil
}

func (rpc *RPCStorage) SyncGroupMessage(sync_key *rpc_storage.SyncGroupHistory, result *rpc_storage.GroupHistoryMessage) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages, last_msgid := storage.LoadGroupHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.GroupID, sync_key.LastMsgID, sync_key.Timestamp, GROUP_OFFLINE_LIMIT)

	historyMessages := make([]*rpc_storage.HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &rpc_storage.HistoryMessage{}
		hm.MsgID = emsg.MsgId
		hm.DeviceID = emsg.DeviceId
		hm.Cmd = int32(emsg.Msg.Cmd)

		emsg.Msg.Version = DEFAULT_VERSION
		hm.Raw = emsg.Msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	result.Messages = historyMessages
	result.LastMsgID = last_msgid
	result.HasMore = false
	return nil
}

func (rpc *RPCStorage) SavePeerMessage(m *rpc_storage.PeerMessage, result *rpc_storage.HistoryMessageID) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.peer_message_count, 1)
	msg := &Message{Cmd: int(m.Cmd), Version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid, prev_msgid := storage.SavePeerMessage(m.AppID, m.Uid, m.DeviceID, msg)
	result.MsgID = msgid
	result.PrevMsgID = prev_msgid
	return nil
}

func (rpc *RPCStorage) SavePeerGroupMessage(m *rpc_storage.PeerGroupMessage, result *rpc_storage.GroupHistoryMessageID) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.peer_message_count, 1)
	msg := &Message{Cmd: int(m.Cmd), Version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	r := storage.SavePeerGroupMessage(m.AppID, m.Members, m.DeviceID, msg)

	result.MessageIDs = make([]*rpc_storage.HistoryMessageID, 0, len(r)/2)
	for i := 0; i < len(r); i += 2 {
		msgid, prev_msgid := r[i], r[i+1]
		result.MessageIDs = append(result.MessageIDs, &rpc_storage.HistoryMessageID{msgid, prev_msgid})
	}

	return nil
}

func (rpc *RPCStorage) SaveGroupMessage(m *rpc_storage.GroupMessage, result *rpc_storage.HistoryMessageID) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.group_message_count, 1)
	msg := &Message{Cmd: int(m.Cmd), Version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid, prev_msgid := storage.SaveGroupMessage(m.AppID, m.GroupID, m.DeviceID, msg)
	result.MsgID = msgid
	result.PrevMsgID = prev_msgid
	return nil
}

func (rpc *RPCStorage) GetNewCount(sync_key *rpc_storage.SyncHistory, new_count *int64) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	count := storage.GetNewCount(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID)
	*new_count = int64(count)
	return nil
}

func (rpc *RPCStorage) GetLatestMessage(r *rpc_storage.HistoryRequest, l *rpc_storage.LatestMessage) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages := storage.LoadLatestMessages(r.AppID, r.Uid, int(r.Limit))

	historyMessages := make([]*rpc_storage.HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &rpc_storage.HistoryMessage{}
		hm.MsgID = emsg.MsgId
		hm.DeviceID = emsg.DeviceId
		hm.Cmd = int32(emsg.Msg.Cmd)

		emsg.Msg.Version = DEFAULT_VERSION
		hm.Raw = emsg.Msg.ToData()
		historyMessages = append(historyMessages, hm)
	}
	l.Messages = historyMessages

	return nil
}

func (rpc *RPCStorage) Ping(int, *int) error {
	return nil
}
