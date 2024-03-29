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
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// 后台发送普通群消息
// 普通群消息首先保存到临时文件中，之后按照保存到文件中的顺序依次派发
type GroupMessageDeliver struct {
	GroupMessageFile

	wt chan int64 //通知有新消息等待发送

	cb_mutex            sync.Mutex               //callback变量的锁
	id                  int64                    //自增的callback id
	callbacks           map[int64]chan *Metadata //返回保存到ims的消息id
	callbackid_to_msgid map[int64]int64          //callback -> msgid

	group_manager *GroupManager
	app_route     *AppRoute
	rpc_storage   *RPCStorage
}

func NewGroupMessageDeliver(root string, group_manager *GroupManager, app_route *AppRoute, rpc_storage *RPCStorage) *GroupMessageDeliver {
	storage := new(GroupMessageDeliver)

	storage.root = root
	if _, err := os.Stat(root); os.IsNotExist(err) {
		err = os.Mkdir(root, 0755)
		if err != nil {
			log.Fatal("mkdir err:", err)
		}
	} else if err != nil {
		log.Fatal("stat err:", err)
	}

	storage.wt = make(chan int64, 10)
	storage.callbacks = make(map[int64]chan *Metadata)
	storage.callbackid_to_msgid = make(map[int64]int64)
	storage.group_manager = group_manager
	storage.app_route = app_route
	storage.rpc_storage = rpc_storage
	storage.openWriteFile()
	storage.openCursorFile()
	storage.readLatestMessageID()
	return storage
}

func (storage *GroupMessageDeliver) ClearCallback() {
	storage.cb_mutex.Lock()
	defer storage.cb_mutex.Unlock()

	storage.callbacks = make(map[int64]chan *Metadata)
	storage.callbackid_to_msgid = make(map[int64]int64)
}

func (storage *GroupMessageDeliver) DoCallback(msgid int64, meta *Metadata) {
	storage.cb_mutex.Lock()
	defer storage.cb_mutex.Unlock()

	if ch, ok := storage.callbacks[msgid]; ok {
		//nonblock
		select {
		case ch <- meta:
		default:
		}
	}
}

func (storage *GroupMessageDeliver) AddCallback(msgid int64, ch chan *Metadata) int64 {
	storage.cb_mutex.Lock()
	defer storage.cb_mutex.Unlock()
	storage.id += 1
	storage.callbacks[msgid] = ch
	storage.callbackid_to_msgid[storage.id] = msgid
	return storage.id
}

func (storage *GroupMessageDeliver) RemoveCallback(callback_id int64) {
	storage.cb_mutex.Lock()
	defer storage.cb_mutex.Unlock()

	if msgid, ok := storage.callbackid_to_msgid[callback_id]; ok {
		delete(storage.callbackid_to_msgid, callback_id)
		delete(storage.callbacks, msgid)
	}
}

func (storage *GroupMessageDeliver) SaveMessage(msg *Message, ch chan *Metadata) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid := storage.saveMessage(msg)
	atomic.StoreInt64(&storage.latest_msgid, msgid)

	var callback_id int64
	if ch != nil {
		callback_id = storage.AddCallback(msgid, ch)
	}

	//nonblock
	select {
	case storage.wt <- msgid:
	default:
	}

	return callback_id
}

// device_ID 发送者的设备ID
func (storage *GroupMessageDeliver) sendMessage(appid int64, uid int64, sender int64, device_ID int64, msg *Message) bool {
	msgSender := &Sender{appid: appid, uid: sender, deviceID: device_ID}
	storage.app_route.SendMessage(appid, uid, msg, msgSender)
	return true
}

func (storage *GroupMessageDeliver) sendGroupMessage(gm *PendingGroupMessage) (*Metadata, bool) {
	msg := &IMMessage{sender: gm.sender, receiver: gm.gid, timestamp: gm.timestamp, content: gm.content}
	m := &Message{cmd: MSG_GROUP_IM, version: DEFAULT_VERSION, body: msg}

	metadata := &Metadata{}

	batch_members := make(map[int64][]int64)
	for _, member := range gm.members {
		index := storage.rpc_storage.GetStorageRPCIndex(member)
		if _, ok := batch_members[index]; !ok {
			batch_members[index] = []int64{member}
		} else {
			mb := batch_members[index]
			mb = append(mb, member)
			batch_members[index] = mb
		}
	}

	for _, mb := range batch_members {
		r, err := storage.rpc_storage.SavePeerGroupMessage(gm.appid, mb, gm.device_ID, m)
		if err != nil {
			log.Errorf("save peer group message:%d %d err:%s", gm.sender, gm.gid, err)
			return nil, false
		}
		if len(r) != len(mb) {
			log.Errorf("save peer group message err:%d %d", len(r), len(mb))
			return nil, false
		}

		for i := 0; i < len(r); i++ {
			msgid, prev_msgid := r[i].MsgID, r[i].PrevMsgID
			member := mb[i]
			meta := &Metadata{sync_key: msgid, prev_sync_key: prev_msgid}
			mm := &Message{cmd: MSG_GROUP_IM, version: DEFAULT_VERSION,
				flag: MESSAGE_FLAG_PUSH, body: msg, meta: meta}
			storage.sendMessage(gm.appid, member, gm.sender, gm.device_ID, mm)

			notify := &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncKey{msgid}}
			storage.sendMessage(gm.appid, member, gm.sender, gm.device_ID, notify)

			if member == gm.sender {
				metadata.sync_key = msgid
				metadata.prev_sync_key = prev_msgid
			}
		}
	}

	group_members := make(map[int64]int64)
	for _, member := range gm.members {
		group_members[member] = 0
	}
	group := NewGroup(gm.gid, gm.appid, group_members)
	storage.app_route.PushGroupMessage(gm.appid, group, m)
	return metadata, true
}

func (storage *GroupMessageDeliver) sendPendingMessage() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := storage.readPendingMessages(ctx)
	for {
		m, ok := <-c
		if !ok {
			break
		}
		gm := m.PendingGroupMessage
		msgid := m.msgid
		meta, r := storage.sendGroupMessage(gm)
		if !r {
			log.Warning("send group message failure")
			break
		}

		storage.DoCallback(msgid, meta)
		storage.latest_sended_msgid = msgid
		storage.saveCursor()
	}
}

func (storage *GroupMessageDeliver) flushPendingMessage() {
	latest_msgid := atomic.LoadInt64(&storage.latest_msgid)
	log.Infof("flush pending message latest msgid:%d latest sended msgid:%d",
		latest_msgid, storage.latest_sended_msgid)
	if latest_msgid > storage.latest_sended_msgid {
		storage.sendPendingMessage()

		//文件超过128M时，截断文件
		if storage.latest_sended_msgid > 128*1024*1024 {
			storage.mutex.Lock()
			defer storage.mutex.Unlock()
			latest_msgid = atomic.LoadInt64(&storage.latest_msgid)
			if latest_msgid > storage.latest_sended_msgid {
				storage.sendPendingMessage()
			}

			if latest_msgid == storage.latest_sended_msgid {
				//truncate file
				storage.truncateFile()
				storage.ClearCallback()
			}
		}
	}
}

func (storage *GroupMessageDeliver) run() {
	log.Info("group message deliver running")

	//启动时等待2s检查文件
	select {
	case <-storage.wt:
		storage.flushPendingMessage()
	case <-time.After(time.Second * 2):
		storage.flushPendingMessage()
	}

	for {
		select {
		case <-storage.wt:
			storage.flushPendingMessage()
		case <-time.After(time.Second * 30):
			storage.flushPendingMessage()
		}
	}
}

func (storage *GroupMessageDeliver) Start() {
	go storage.run()
}
