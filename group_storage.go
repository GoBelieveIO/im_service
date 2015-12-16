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


import "fmt"
import "strconv"
import log "github.com/golang/glog"

type AppGroupMemberLoginID struct {
	appid  int64
	gid    int64
	uid    int64
	device_id int64
}

type GroupStorage struct {
	*StorageFile
	group_received   map[AppGroupMemberLoginID]int64
}

func NewGroupStorage(f *StorageFile) *GroupStorage {
	storage := &GroupStorage{StorageFile:f}
	storage.group_received = make(map[AppGroupMemberLoginID]int64)
	return storage
}

func (storage *GroupStorage) SaveGroupMessage(appid int64, gid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	msgid := storage.saveMessage(msg)

	last_id, _ := storage.GetLastGroupMessageID(appid, gid)
	lt := &GroupOfflineMessage{appid:appid, gid:gid, msgid:msgid, device_id:device_id, prev_msgid:last_id}
	m := &Message{cmd:MSG_GROUP_IM_LIST, body:lt}
	
	last_id = storage.saveMessage(m)
	storage.SetLastGroupMessageID(appid, gid, last_id)
	return msgid
}

func (storage *GroupStorage) SetLastGroupMessageID(appid int64, gid int64, msgid int64) {
	key := fmt.Sprintf("g_%d_%d", appid, gid)
	value := fmt.Sprintf("%d", msgid)	
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *GroupStorage) GetLastGroupMessageID(appid int64, gid int64) (int64, error) {
	key := fmt.Sprintf("g_%d_%d", appid, gid)
	value, err := storage.db.Get([]byte(key), nil)
	if err != nil {
		log.Error("get err:", err)
		return 0, err
	}

	msgid, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		log.Error("parseint err:", err)
		return 0, err
	}
	return msgid, nil
}

func (storage *GroupStorage) SetLastGroupReceivedID(appid int64, gid int64, uid int64, msgid int64) {
	key := fmt.Sprintf("g_%d_%d_%d", appid, gid, uid)
	value := fmt.Sprintf("%d", msgid)	
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *GroupStorage) getLastGroupReceivedID(appid int64, gid int64, uid int64, device_id int64) (int64, error) {
	key := fmt.Sprintf("g_%d_%d_%d_%d", appid, gid, uid, device_id)

	id := AppGroupMemberLoginID{appid:appid, gid:gid, uid:uid, device_id:device_id}
	if msgid, ok := storage.group_received[id]; ok {
		return msgid, nil
	}
	value, err := storage.db.Get([]byte(key), nil)
	if err != nil {
		log.Error("get err:", err)
		return 0, err
	}

	msgid, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		log.Error("parseint err:", err)
		return 0, err
	}
	return msgid, nil
}

func (storage *GroupStorage) GetLastGroupReceivedID(appid int64, gid int64, uid int64, device_id int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastGroupReceivedID(appid, gid, uid, device_id)
}

func (storage *GroupStorage) DequeueGroupOffline(msg_id int64, appid int64, gid int64, device_id int64, receiver int64) {
	log.Infof("dequeue group offline:%d %d %d %d\n", appid, gid, receiver, msg_id)
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	last, _ := storage.getLastGroupReceivedID(appid, gid, receiver, device_id)
	if msg_id <= last {
		log.Infof("group ack msgid:%d last:%d\n", msg_id, last)
		return
	}
	id := AppGroupMemberLoginID{appid:appid, gid:gid, uid:receiver}
	storage.group_received[id] = msg_id
}

func (storage *GroupStorage) LoadGroupOfflineMessage(appid int64, gid int64, uid int64, device_id int64, limit int) []*EMessage {
	last_id, err := storage.GetLastGroupMessageID(appid, gid)
	if err != nil {
		log.Info("get last group message id err:", err)
		return nil
	}

	last_received_id, _ := storage.GetLastGroupReceivedID(appid, gid, uid, device_id)

	c := make([]*EMessage, 0, 10)

	msgid := last_id
	for ; msgid > 0; {
		msg := storage.LoadMessage(msgid)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		if msg.cmd != MSG_GROUP_IM_LIST {
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*GroupOfflineMessage)

		if off.msgid == 0 || off.msgid <= last_received_id {
			break
		}

		m := storage.LoadMessage(off.msgid)
		c = append(c, &EMessage{msgid:off.msgid, device_id:off.device_id, msg:m})

		msgid = off.prev_msgid

		if len(c) >= limit {
			break
		}
	}

	if len(c) > 0 {
		//reverse
		size := len(c)
		for i := 0; i < size/2; i++ {
			t := c[i]
			c[i] = c[size-i-1]
			c[size-i-1] = t
		}
	}

	log.Infof("load group offline message appid:%d gid:%d uid:%d count:%d last id:%d last received id:%d\n", appid, gid, uid, len(c), last_id, last_received_id)
	return c
}

func (storage *GroupStorage) FlushReceived() {
	if len(storage.group_received) > 0 {
		log.Infof("flush group received:%d\n", len(storage.group_received))
	}

	if len(storage.group_received) > 0 {
		for id, msg_id := range storage.group_received {
			storage.SetLastGroupReceivedID(id.appid, id.gid, id.uid, msg_id)
			off := &GroupOfflineMessage{appid:id.appid, receiver:id.uid, msgid:msg_id, gid:id.gid}
			msg := &Message{cmd:MSG_GROUP_ACK_IN, body:off}
			storage.saveMessage(msg)
		}
		storage.group_received = make(map[AppGroupMemberLoginID]int64)
	}
}

func (storage *GroupStorage) ExecMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_GROUP_IM_LIST {
		off := msg.body.(*GroupOfflineMessage)
		storage.SetLastGroupMessageID(off.appid, off.gid, msgid)
	} else if msg.cmd == MSG_GROUP_ACK_IN {
		off := msg.body.(*GroupOfflineMessage)
		storage.SetLastGroupReceivedID(off.appid, off.gid, off.receiver, msgid)
	}
}
