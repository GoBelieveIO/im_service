
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

import "sync/atomic"

type RPCStorage struct {
	
}

func (rpc *RPCStorage) SyncMessage(sync_key *SyncHistory, result *PeerHistoryMessage) error {
	atomic.AddInt64(&server_summary.nrequests, 1)		
	messages, last_msgid, hasMore := storage.LoadHistoryMessagesV3(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID, config.limit, config.hard_limit)
	
	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range(messages) {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)
 
		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	result.Messages = historyMessages
	result.LastMsgID = last_msgid
	result.HasMore = hasMore
	return nil
}



func (rpc *RPCStorage) SyncGroupMessage(sync_key *SyncGroupHistory, result *GroupHistoryMessage) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages, last_msgid := storage.LoadGroupHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.GroupID, sync_key.LastMsgID, sync_key.Timestamp, GROUP_OFFLINE_LIMIT)
 
	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range(messages) {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)
 
		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	result.Messages = historyMessages
	result.LastMsgID = last_msgid	
	result.HasMore = false
	return nil	
}


func (rpc *RPCStorage) SavePeerMessage(m *PeerMessage, result *HistoryMessageID) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.peer_message_count, 1)
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid, prev_msgid := storage.SavePeerMessage(m.AppID, m.Uid, m.DeviceID, msg)
	result.MsgID = msgid
	result.PrevMsgID = prev_msgid
	return nil	
}

func (rpc *RPCStorage) SavePeerGroupMessage(m *PeerGroupMessage, result *GroupHistoryMessageID) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.peer_message_count, 1)
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	r := storage.SavePeerGroupMessage(m.AppID, m.Members, m.DeviceID, msg)

	result.MessageIDs = make([]*HistoryMessageID, 0, len(r)/2)
	for i := 0; i < len(r); i += 2 {
		msgid, prev_msgid := r[i], r[i+1]
		result.MessageIDs = append(result.MessageIDs, &HistoryMessageID{msgid, prev_msgid})
	}
	
	return nil
}


func (rpc *RPCStorage) SaveGroupMessage(m *GroupMessage, result *HistoryMessageID) error {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.group_message_count, 1)
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid, prev_msgid := storage.SaveGroupMessage(m.AppID, m.GroupID, m.DeviceID, msg)
	result.MsgID = msgid
	result.PrevMsgID = prev_msgid
	return nil
}

func (rpc *RPCStorage) GetNewCount(sync_key *SyncHistory, new_count *int64) error {
	atomic.AddInt64(&server_summary.nrequests, 1)	
	count := storage.GetNewCount(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID)
	*new_count = int64(count)
	return nil
}

func (rpc *RPCStorage) GetLatestMessage(addr string, r *HistoryRequest, l *LatestMessage) error {
	atomic.AddInt64(&server_summary.nrequests, 1)	
	messages := storage.LoadLatestMessages(r.AppID, r.Uid, int(r.Limit))

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range(messages) {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)
 
		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}
	l.Messages = historyMessages
	
	return nil
}

