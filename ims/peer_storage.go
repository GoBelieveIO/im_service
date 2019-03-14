
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
import "io"
import "os"
import "time"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"


const BATCH_SIZE = 1000
const PEER_INDEX_FILE_NAME = "peer_index.v2"

type UserID struct {
	appid  int64
	uid    int64
}

type UserIndex struct {
	last_id int64	
	last_peer_id int64
	last_batch_id int64
	last_seq_id int64
}

//在取离线消息时，可以对群组消息和点对点消息分别获取，
//这样可以做到分别控制点对点消息和群组消息读取量，避免单次读取超量的离线消息
type PeerStorage struct {
	*StorageFile
	
	//消息索引全部放在内存中,在程序退出时,再全部保存到文件中，
	//如果索引文件不存在或上次保存失败，则在程序启动的时候，从消息DB中重建索引，这需要遍历每一条消息
	message_index  map[UserID]*UserIndex //记录每个用户最近的消息ID
}

func NewPeerStorage(f *StorageFile) *PeerStorage {
	storage := &PeerStorage{StorageFile:f}
	storage.message_index = make(map[UserID]*UserIndex);
	return storage
}

func (storage *PeerStorage) SavePeerMessage(appid int64, uid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid := storage.saveMessage(msg)

	user_index := storage.getPeerIndex(appid, uid)
	last_id := user_index.last_id
	last_peer_id := user_index.last_peer_id
	last_batch_id := user_index.last_batch_id
	last_seq_id := user_index.last_seq_id
	
	off := &OfflineMessage3{appid:appid, receiver:uid, msgid:msgid, device_id:device_id, prev_msgid:last_id, prev_peer_msgid:last_peer_id, prev_batch_msgid:last_batch_id}
	
	var flag int
	if storage.isGroupMessage(msg) {
		flag = MESSAGE_FLAG_GROUP
	}
	m := &Message{cmd:MSG_OFFLINE_V3, flag:flag, body:off}
	last_id = storage.saveMessage(m)

	if !storage.isGroupMessage(msg) {
		last_peer_id = last_id
	}

	last_seq_id += 1
	if last_seq_id%BATCH_SIZE == 0 {
		last_batch_id = last_id
	}
	
	ui := &UserIndex{last_id, last_peer_id, last_batch_id, last_seq_id}	
	storage.setPeerIndex(appid, uid, ui)
	return msgid
}

func (storage *PeerStorage) GetPeerIndex(appid int64, receiver int64) *UserIndex {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	return storage.getPeerIndex(appid, receiver)
}

func (storage *PeerStorage) getPeerIndex(appid int64, receiver int64) *UserIndex {
	id := UserID{appid, receiver}
	if ui, ok := storage.message_index[id]; ok {
		return ui
	}
	return &UserIndex{}
}

func (storage *PeerStorage) setPeerIndex(appid int64, receiver int64, ui *UserIndex) {
	id := UserID{appid, receiver}
	storage.message_index[id] = ui
	
	if ui.last_id > storage.last_id {
		storage.last_id = ui.last_id
	}
	
}

//获取最近离线消息ID
func (storage *PeerStorage) getLastMessageID(appid int64, receiver int64) (int64, int64) {
	id := UserID{appid, receiver}
	if ui, ok := storage.message_index[id]; ok {
		return ui.last_id, ui.last_peer_id
	}
	return 0, 0
}

//lock
func (storage *PeerStorage) GetLastMessageID(appid int64, receiver int64) (int64, int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastMessageID(appid, receiver)
}


//获取所有消息id大于sync_msgid的消息,
//group_limit&limit:0 表示无限制
//消息超过group_limit后，只获取点对点消息
//总消息数限制在limit
func (storage *PeerStorage) LoadHistoryMessages(appid int64, receiver int64, sync_msgid int64,  group_limit int, limit int) ([]*EMessage, int64, bool) {
	var last_msgid int64
	last_id, _ := storage.GetLastMessageID(appid, receiver)
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		
		var off *OfflineMessage2
		if msg.cmd == MSG_OFFLINE {
			off1 := msg.body.(*OfflineMessage)
			off = &OfflineMessage2{
				appid:off1.appid,
				receiver:off1.receiver,
				msgid:off1.msgid,
				device_id:off1.device_id,
				prev_msgid:off1.prev_msgid,
				prev_peer_msgid:off1.prev_msgid,
			}
		} else if msg.cmd == MSG_OFFLINE_V2 {
			off = msg.body.(*OfflineMessage2)
		} else if msg.cmd == MSG_OFFLINE_V3 {
			off3 := msg.body.(*OfflineMessage3)
			off = &OfflineMessage2{
				appid:off3.appid,
				receiver:off3.receiver,
				msgid:off3.msgid,
				device_id:off3.device_id,
				prev_msgid:off3.prev_msgid,
				prev_peer_msgid:off3.prev_peer_msgid,
			}
		} else {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		if last_msgid == 0 {
			last_msgid = off.msgid
		}
		if off.msgid <= sync_msgid {
			break
		}		
		
		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM && 
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM && 
			msg.cmd != MSG_CUSTOMER && 
			msg.cmd != MSG_CUSTOMER_SUPPORT &&
			msg.cmd != MSG_SYSTEM {
			if group_limit > 0 && len(messages) >= group_limit {
				last_id = off.prev_peer_msgid
			} else {
				last_id = off.prev_msgid
			}			
			continue
		}

		emsg := &EMessage{msgid:off.msgid, device_id:off.device_id, msg:msg}
		messages = append(messages, emsg)
		
		if limit > 0 && len(messages) >= limit {
			break
		}

		if group_limit > 0 && len(messages) >= group_limit {
			last_id = off.prev_peer_msgid
		} else {
			last_id = off.prev_msgid
		}
	}

	if len(messages) > 1000 {
		log.Warningf("appid:%d uid:%d sync msgid:%d history message overflow:%d",
			appid, receiver, sync_msgid, len(messages))
	}
	log.Infof("appid:%d uid:%d sync msgid:%d history message loaded:%d %d",
		appid, receiver, sync_msgid, len(messages), last_msgid)
	
	return messages, last_msgid, false
}

//加载离线消息
//limit 单次加载限制
//hard_limit 总量限制
//hard_limit == 0 or ( hard_limit >= BATCH_SIZE and hard_limit > limit)
func (storage *PeerStorage) LoadHistoryMessagesV3(appid int64, receiver int64, sync_msgid int64, limit int, hard_limit int) ([]*EMessage, int64, bool) {
	var last_msgid int64
	var last_offline_msgid int64
	
	msg_index := storage.GetPeerIndex(appid, receiver)

	var last_batch_id int64
	if msg_index != nil {
		last_batch_id = msg_index.last_batch_id
	}

	hard_batch_count := hard_limit/BATCH_SIZE
	
	batch_count := limit/BATCH_SIZE
	if batch_count == 0 {
		//as if default limit==BATCH_SIZE
		batch_count = 1
	}
	
	batch_ids := make([]int64, 0, 10)

	//搜索和sync_msgid最近的batch_id
	for {
		if last_batch_id <= sync_msgid {
			break
		}
		msg := storage.LoadMessage(last_batch_id)
		if msg == nil {
			break
		}

		if msg.cmd != MSG_OFFLINE_V3 {
			break
		}
		off3 := msg.body.(*OfflineMessage3)		
		if off3.msgid <= sync_msgid {
			break
		}
		
		batch_ids = append(batch_ids, last_batch_id)		
		last_batch_id = off3.prev_batch_msgid

		if hard_batch_count > 0 && len(batch_ids) >= hard_batch_count {
			break
		}
	}

	//只获取点对点消息
	var is_peer bool
	var last_id int64
	if hard_batch_count > 0 && len(batch_ids) >= hard_batch_count && hard_batch_count >= batch_count {
		index := len(batch_ids) - batch_count
		last_id = batch_ids[index]
		//离线消息总数超过hard_limit时，首先加载数量不超过limit的点对点消息
		//尽量保证点对点消息的投递
		is_peer = true
	} else if len(batch_ids) >= batch_count {
		index := len(batch_ids) - batch_count
		last_id = batch_ids[index]
	} else if msg_index != nil {
		last_id = msg_index.last_id
	}
	
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id <= sync_msgid {
			break
		}
		
		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		
		var off *OfflineMessage2
		if msg.cmd == MSG_OFFLINE {
			off1 := msg.body.(*OfflineMessage)
			off = &OfflineMessage2{
				appid:off1.appid,
				receiver:off1.receiver,
				msgid:off1.msgid,
				device_id:off1.device_id,
				prev_msgid:off1.prev_msgid,
				prev_peer_msgid:off1.prev_msgid,
			}
		} else if msg.cmd == MSG_OFFLINE_V2 {
			off = msg.body.(*OfflineMessage2)
		} else if msg.cmd == MSG_OFFLINE_V3 {
			off3 := msg.body.(*OfflineMessage3)
			off = &OfflineMessage2{
				appid:off3.appid,
				receiver:off3.receiver,
				msgid:off3.msgid,
				device_id:off3.device_id,
				prev_msgid:off3.prev_msgid,
				prev_peer_msgid:off3.prev_msgid,
			}			
		} else {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		if last_msgid == 0 {
			last_msgid = off.msgid
			last_offline_msgid = last_id
		}
		if off.msgid <= sync_msgid {
			break
		}		
		
		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM && 
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM && 
			msg.cmd != MSG_CUSTOMER && 
			msg.cmd != MSG_CUSTOMER_SUPPORT &&
			msg.cmd != MSG_SYSTEM {
			if is_peer {
				last_id = off.prev_peer_msgid
			} else {
				last_id = off.prev_msgid
			}
			continue
		}

		emsg := &EMessage{msgid:off.msgid, device_id:off.device_id, msg:msg}
		messages = append(messages, emsg)
		
		if limit > 0 && len(messages) >= limit {
			break
		}

		if is_peer {
			last_id = off.prev_peer_msgid
		} else {
			last_id = off.prev_msgid
		}
	}

	if len(messages) > 1000 {
		log.Warningf("appid:%d uid:%d sync msgid:%d history message overflow:%d",
			appid, receiver, sync_msgid, len(messages))
	}

	var hasMore bool
	if msg_index != nil && last_offline_msgid > 0 &&
		last_offline_msgid < msg_index.last_id {
		hasMore = true
	}

	log.Infof("appid:%d uid:%d sync msgid:%d history message loaded:%d %d, last_id:%d, %d last batch id:%d last seq id:%d has more:%t is peer:%t",
		appid, receiver, sync_msgid, len(messages), last_msgid,
		last_offline_msgid, msg_index.last_id, msg_index.last_batch_id,
		msg_index.last_seq_id, hasMore, is_peer)
	
	return messages, last_msgid, hasMore
}


func (storage *PeerStorage) LoadLatestMessages(appid int64, receiver int64, limit int) []*EMessage {
	last_id, _ := storage.GetLastMessageID(appid, receiver)
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		var msgid int64
		var device_id int64
		var prev_msgid int64
		if msg.cmd == MSG_OFFLINE {
			off := msg.body.(*OfflineMessage)
			msgid = off.msgid
			device_id = off.device_id
			prev_msgid = off.prev_msgid
		} else if msg.cmd == MSG_OFFLINE_V2 {
			off := msg.body.(*OfflineMessage2)
			msgid = off.msgid
			device_id = off.device_id
			prev_msgid = off.prev_msgid
		} else if msg.cmd == MSG_OFFLINE_V3 {
			off := msg.body.(*OfflineMessage3)
			msgid = off.msgid
			device_id = off.device_id
			prev_msgid = off.prev_msgid
		} else {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		
		msg = storage.LoadMessage(msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM && 
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM && 
			msg.cmd != MSG_CUSTOMER && 
			msg.cmd != MSG_CUSTOMER_SUPPORT {
			last_id = prev_msgid
			continue
		}

		emsg := &EMessage{msgid:msgid, device_id:device_id, msg:msg}
		messages = append(messages, emsg)
		if len(messages) >= limit {
			break
		}
		last_id = prev_msgid
	}
	return messages
}

func (client *PeerStorage) isGroupMessage(msg *Message) bool {
	return msg.cmd == MSG_GROUP_IM || msg.flag & MESSAGE_FLAG_GROUP != 0
}

func (client *PeerStorage) isSender(msg *Message, appid int64, uid int64) bool {
	if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
		m := msg.body.(*IMMessage)
		if m.sender == uid {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER {
		m := msg.body.(*CustomerMessage)
		if m.customer_appid == appid && 
			m.customer_id == uid {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER_SUPPORT {
		m := msg.body.(*CustomerMessage)
		if config.kefu_appid == appid && 
			m.seller_id == uid {
			return true
		}
	}
	return false
}


func (storage *PeerStorage) GetNewCount(appid int64, uid int64, last_received_id int64) int {
	last_id, _ := storage.GetLastMessageID(appid, uid)

	count := 0
	log.Infof("last id:%d last received id:%d", last_id, last_received_id)

	msgid := last_id
	for ; msgid > 0; {
		msg := storage.LoadMessage(msgid)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*OfflineMessage)

		if off.msgid == 0 || off.msgid <= last_received_id {
			break
		}

		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}

		if !storage.isSender(msg, appid, uid) {
			count += 1
			break
		}
		msgid = off.prev_msgid
	}

	return count
}

func (storage *PeerStorage) createPeerIndex() {
	log.Info("create message index begin:", time.Now().UnixNano())

	for i := 0; i <= storage.block_NO; i++ {
		file := storage.openReadFile(i)
		if file == nil {
			//历史消息被删除
			continue
		}

		_, err := file.Seek(HEADER_SIZE, os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			file.Close()
			break
		}
		for {
			msgid, err := file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := storage.ReadMessage(file)
			if msg == nil {
				break
			}

			block_NO := i
			msgid = int64(block_NO)*BLOCK_SIZE + msgid			
			if msg.cmd == MSG_OFFLINE {
				off := msg.body.(*OfflineMessage)
				ui := &UserIndex{msgid, msgid, 0, 0}
				storage.setPeerIndex(off.appid, off.receiver, ui)				
			} else if msg.cmd == MSG_OFFLINE_V2 {
				off := msg.body.(*OfflineMessage2)
				last_peer_id := msgid
				if ((msg.flag & MESSAGE_FLAG_GROUP) != 0) {
					_, last_peer_id = storage.getLastMessageID(off.appid, off.receiver)
				}
				ui := &UserIndex{msgid, last_peer_id, 0, 0}
				storage.setPeerIndex(off.appid, off.receiver, ui)				
			} else if msg.cmd == MSG_OFFLINE_V3 {
				off := msg.body.(*OfflineMessage3)
				last_peer_id := msgid

				index := storage.getPeerIndex(off.appid, off.receiver)
				if ((msg.flag & MESSAGE_FLAG_GROUP) != 0) {
					last_peer_id = index.last_peer_id
				}
				last_batch_id := index.last_batch_id
				last_seq_id := index.last_seq_id + 1
				if last_seq_id%BATCH_SIZE == 0 {
					last_batch_id = msgid
				}
				
				ui := &UserIndex{msgid, last_peer_id, last_batch_id, last_seq_id}
				storage.setPeerIndex(off.appid, off.receiver, ui)
			}
		}

		file.Close()
	}
	log.Info("create message index end:", storage.last_id, time.Now().UnixNano())
}


func (storage *PeerStorage) repairPeerIndex() {
	log.Info("repair message index begin:", storage.last_id, time.Now().UnixNano())

	first := storage.getBlockNO(storage.last_id)
	off := storage.getBlockOffset(storage.last_id)
	
	for i := first; i <= storage.block_NO; i++ {
		file := storage.openReadFile(i)
		if file == nil {
			//历史消息被删除
			continue
		}

		offset := HEADER_SIZE
		if i == first {
			offset = off
		}
		
		_, err := file.Seek(int64(offset), os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			file.Close()
			break
		}
		for {
			msgid, err := file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := storage.ReadMessage(file)
			if msg == nil {
				break
			}
			block_NO := i			
			msgid = int64(block_NO)*BLOCK_SIZE + msgid
			if msgid == storage.last_id {
				continue
			}
			
			if msg.cmd == MSG_OFFLINE {
				off := msg.body.(*OfflineMessage)
				ui := &UserIndex{msgid, msgid, 0, 0}
				storage.setPeerIndex(off.appid, off.receiver, ui)				
			} else if msg.cmd == MSG_OFFLINE_V2 {
				off := msg.body.(*OfflineMessage2)
				last_peer_id := msgid

				if ((msg.flag & MESSAGE_FLAG_GROUP) != 0) {
					_, last_peer_id = storage.getLastMessageID(off.appid, off.receiver)
				}
				ui := &UserIndex{msgid, last_peer_id, 0, 0}
				storage.setPeerIndex(off.appid, off.receiver, ui)				
			} else if msg.cmd == MSG_OFFLINE_V3 {
				off := msg.body.(*OfflineMessage3)
				last_peer_id := msgid

				index := storage.getPeerIndex(off.appid, off.receiver)
				if ((msg.flag & MESSAGE_FLAG_GROUP) != 0) {
					last_peer_id = index.last_peer_id
				}
				last_batch_id := index.last_batch_id
				last_seq_id := index.last_seq_id + 1
				if last_seq_id%BATCH_SIZE == 0 {
					last_batch_id = msgid
				}
				ui := &UserIndex{msgid, last_peer_id, last_batch_id, last_seq_id}
				storage.setPeerIndex(off.appid, off.receiver, ui)
			}
		}

		file.Close()
	}
	log.Info("repair message index end:", storage.last_id, time.Now().UnixNano())
}


func (storage *PeerStorage) readPeerIndex() bool {
	path := fmt.Sprintf("%s/%s", storage.root, PEER_INDEX_FILE_NAME)
	log.Info("read message index path:", path)
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("open file:", err)			
		}
		return false
	}
	defer file.Close()
	const INDEX_SIZE = 48
	data := make([]byte, INDEX_SIZE*1000)

	for {
		n, err := file.Read(data)
		if err != nil {
			if err != io.EOF {
				log.Fatal("read err:", err)
			}
			break
		}
		n = n - n%INDEX_SIZE
		buffer := bytes.NewBuffer(data[:n])
		for i := 0; i < n/INDEX_SIZE; i++ {
			id := UserID{}
			var msg_id int64
			var peer_msgid int64
			var batch_id int64
			var seq_id int64
			binary.Read(buffer, binary.BigEndian, &id.appid)
			binary.Read(buffer, binary.BigEndian, &id.uid)
			binary.Read(buffer, binary.BigEndian, &msg_id)
			binary.Read(buffer, binary.BigEndian, &peer_msgid)
			binary.Read(buffer, binary.BigEndian, &batch_id)
			binary.Read(buffer, binary.BigEndian, &seq_id)
			ui := &UserIndex{msg_id, peer_msgid, batch_id, seq_id}
			storage.setPeerIndex(id.appid, id.uid, ui)
		}
	}
	return true
}

func (storage *PeerStorage) removePeerIndex() {
	path := fmt.Sprintf("%s/%s", storage.root, PEER_INDEX_FILE_NAME)
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("remove file:", err)
		}
	}
}


func (storage *PeerStorage) clonePeerIndex() map[UserID]*UserIndex {
	message_index := make(map[UserID]*UserIndex)
	for k, v := range(storage.message_index) {
		message_index[k] = v
	}
	return message_index
}


//appid uid msgid = 24字节
func (storage *PeerStorage) savePeerIndex(message_index  map[UserID]*UserIndex ) {
	path := fmt.Sprintf("%s/peer_index_t", storage.root)
	log.Info("write peer message index path:", path)
	begin := time.Now().UnixNano()
	log.Info("flush peer index begin:", begin)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	defer file.Close()

	buffer := new(bytes.Buffer)
	index := 0
	for id, value := range(message_index) {
		binary.Write(buffer, binary.BigEndian, id.appid)
		binary.Write(buffer, binary.BigEndian, id.uid)
		binary.Write(buffer, binary.BigEndian, value.last_id)
		binary.Write(buffer, binary.BigEndian, value.last_peer_id)
		binary.Write(buffer, binary.BigEndian, value.last_batch_id)
		binary.Write(buffer, binary.BigEndian, value.last_seq_id)
		index += 1
		//batch write to file
		if index % 1000 == 0 {
			buf := buffer.Bytes()
			n, err := file.Write(buf)
			if err != nil {
				log.Fatal("write file:", err)
			}
			if n != len(buf) {
				log.Fatal("can't write file:", len(buf), n)
			}

			buffer.Reset()
		}
	}

	buf := buffer.Bytes()
	n, err := file.Write(buf)
	if err != nil {
		log.Fatal("write file:", err)
	}
	if n != len(buf) {
		log.Fatal("can't write file:", len(buf), n)
	}
	err = file.Sync()
	if err != nil {
		log.Info("sync file err:", err)
	}

	path2 := fmt.Sprintf("%s/%s", storage.root, PEER_INDEX_FILE_NAME)
	err = os.Rename(path, path2)
	if err != nil {
		log.Fatal("rename peer index file err:", err)
	}
	
	end := time.Now().UnixNano()
	log.Info("flush peer index end:", end, " used:", end - begin)
}



func (storage *PeerStorage) execMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_OFFLINE {
		off := msg.body.(*OfflineMessage)
		ui := &UserIndex{msgid, msgid, 0, 0}
		storage.setPeerIndex(off.appid, off.receiver, ui)
	} else if msg.cmd == MSG_OFFLINE_V2 {
		off := msg.body.(*OfflineMessage2)
		last_peer_id := msgid		
		if ((msg.flag & MESSAGE_FLAG_GROUP) != 0) {
			_, last_peer_id = storage.getLastMessageID(off.appid, off.receiver)			
		}
		ui := &UserIndex{msgid, last_peer_id, 0, 0}
		storage.setPeerIndex(off.appid, off.receiver, ui)
	} else if msg.cmd == MSG_OFFLINE_V3 {
		off := msg.body.(*OfflineMessage3)
		last_peer_id := msgid

		index := storage.getPeerIndex(off.appid, off.receiver)
		if ((msg.flag & MESSAGE_FLAG_GROUP) != 0) {
			last_peer_id = index.last_peer_id
		}
		last_batch_id := index.last_batch_id
		last_seq_id := index.last_seq_id + 1
		if last_seq_id%BATCH_SIZE == 0 {
			last_batch_id = msgid
		}
		
		ui := &UserIndex{msgid, last_peer_id, last_batch_id, last_seq_id}
		storage.setPeerIndex(off.appid, off.receiver, ui)
	}
}
