
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

func SyncMessage(addr string, sync_key *SyncHistory) *PeerHistoryMessage {
	messages, last_msgid := storage.LoadHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID, PEER_OFFLINE_LIMIT)

	
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
	return &PeerHistoryMessage{historyMessages, last_msgid}
}

func SyncGroupMessage(addr string , sync_key *SyncGroupHistory) *GroupHistoryMessage {
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
	return &GroupHistoryMessage{historyMessages, last_msgid}
}


func SavePeerMessage(addr string, m *PeerMessage) (int64, error) {
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SavePeerMessage(m.AppID, m.Uid, m.DeviceID, msg)
	return msgid, nil
}

func SaveGroupMessage(addr string, m *GroupMessage) (int64, error) {
	msg := &Message{cmd:int(m.Cmd), version:DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SaveGroupMessage(m.AppID, m.GroupID, m.DeviceID, msg)
	return msgid, nil
}

func GetNewCount(addr string, sync_key *SyncHistory) (int64, error) {
	count := storage.GetNewCount(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID)
	return int64(count), nil
}

func GetLatestMessage(addr string, r *HistoryRequest) []*HistoryMessage {
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

	return historyMessages
}
