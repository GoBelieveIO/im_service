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
import log "github.com/sirupsen/logrus"

const GROUP_INDEX_FILE_NAME = "group_index.v2"

type GroupID struct {
	appid  int64
	gid    int64
}

type GroupIndex struct {
	last_msgid int64
	last_id int64
	last_batch_id int64
	last_seq_id int64 //最近消息的序号
}

type GroupStorage struct {
	*StorageFile

	message_index  map[GroupID]*GroupIndex //记录每个群组最近的消息ID
}

func NewGroupStorage(f *StorageFile) *GroupStorage {
	storage := &GroupStorage{StorageFile:f}
	storage.message_index = make(map[GroupID]*GroupIndex)
	return storage
}

func (storage *GroupStorage) SaveGroupMessage(appid int64, gid int64, device_id int64, msg *Message) (int64, int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	msgid := storage.saveMessage(msg)

	index := storage.getGroupIndex(appid, gid)
	last_id := index.last_id
	last_batch_id := index.last_batch_id
	last_seq_id := index.last_seq_id
	
	off := &OfflineMessage4{}
	off.appid = appid
	off.receiver = gid
	off.msgid = msgid
	off.device_id = device_id
	off.seq_id = last_seq_id + 1
	off.prev_msgid = last_id
	off.prev_peer_msgid = 0
	off.prev_batch_msgid = last_batch_id

	m := &Message{cmd:MSG_GROUP_OFFLINE, body:off}
	last_id = storage.saveMessage(m)

	last_seq_id += 1
	if last_seq_id%BATCH_SIZE == 0 {
		last_batch_id = last_id
	}
	gi := &GroupIndex{msgid, last_id, last_batch_id, last_seq_id}
	storage.setGroupIndex(appid, gid, gi)
	return msgid, index.last_msgid
}

func (storage *GroupStorage) setGroupIndex(appid int64, gid int64, gi *GroupIndex) {
	id := GroupID{appid, gid}
	storage.message_index[id] = gi
	if gi.last_id > storage.last_id {
		storage.last_id = gi.last_id
	}
}

func (storage *GroupStorage) getGroupIndex(appid int64, gid int64) (*GroupIndex) {
	id := GroupID{appid, gid}
	if gi, ok := storage.message_index[id]; ok {
		return gi
	}
	return &GroupIndex{}
}


func (storage *GroupStorage) GetGroupIndex(appid int64, gid int64) (*GroupIndex) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	return storage.getGroupIndex(appid, gid)
}


//获取所有消息id大于msgid的消息
//ts:入群时间
func (storage *GroupStorage) LoadGroupHistoryMessages(appid int64, uid int64, gid int64, msgid int64, ts int32, limit int) ([]*EMessage, int64) {
	log.Infof("load group history message:%d %d", msgid, ts)
	msg_index := storage.GetGroupIndex(appid, gid)
	last_id := msg_index.last_id
	
	var last_msgid int64
	c := make([]*EMessage, 0, 10)

	for ; last_id > 0; {
		msg := storage.LoadMessage(last_id)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		var off *OfflineMessage
		if ioff, ok := msg.body.(IOfflineMessage); ok {
			off = ioff.body()
		} else {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}		
		if last_msgid == 0 {
			last_msgid = off.msgid
		}
		
		if off.msgid == 0 || off.msgid <= msgid {
			break
		}

		m := storage.LoadMessage(off.msgid)
		if msgid == 0 && m.cmd == MSG_GROUP_IM {
			//不取入群之前的消息
			im := m.body.(*IMMessage)
			if im.timestamp < ts {
				break
			}
		}
		c = append(c, &EMessage{msgid:off.msgid, device_id:off.device_id, msg:m})

		last_id = off.prev_msgid

		if len(c) >= limit {
			break
		}
	}

	log.Infof("load group history message appid:%d gid:%d uid:%d count:%d\n", appid, gid, uid, len(c))
	return c, last_msgid
}

func (storage *GroupStorage) createGroupIndex() {
	log.Info("create group message index begin:", time.Now().UnixNano())

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

			storage.execMessage(msg, msgid)
		}

		file.Close()
	}
	log.Info("create group message index end:", time.Now().UnixNano())
}

func (storage *GroupStorage) repairGroupIndex() {
	log.Info("repair group message index begin:", time.Now().UnixNano())

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

			storage.execMessage(msg, msgid)
		}

		file.Close()
	}
	log.Info("repair group message index end:", time.Now().UnixNano())	
}


func (storage *GroupStorage) readGroupIndex() bool {
	path := fmt.Sprintf("%s/%s", storage.root, GROUP_INDEX_FILE_NAME)
	log.Info("read group message index path:", path)
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
			id := GroupID{}
			var last_msgid int64
			var last_id int64
			var last_batch_id int64
			var last_seq_id int64
			binary.Read(buffer, binary.BigEndian, &id.appid)
			binary.Read(buffer, binary.BigEndian, &id.gid)
			binary.Read(buffer, binary.BigEndian, &last_msgid)
			binary.Read(buffer, binary.BigEndian, &last_id)
			binary.Read(buffer, binary.BigEndian, &last_batch_id)
			binary.Read(buffer, binary.BigEndian, &last_seq_id)

			gi := &GroupIndex{last_msgid, last_id, last_batch_id, last_seq_id}
			storage.setGroupIndex(id.appid, id.gid, gi)
		}
	}
	return true
}

func (storage *GroupStorage) removeGroupIndex() {
	path := fmt.Sprintf("%s/%s", storage.root, GROUP_INDEX_FILE_NAME)
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("remove file:", err)
		}
	}
}

func (storage *GroupStorage) cloneGroupIndex() map[GroupID]*GroupIndex {
	message_index := make(map[GroupID]*GroupIndex)
	for k, v := range(storage.message_index) {
		message_index[k] = v
	}
	return message_index
}


//appid gid msgid = 24字节
func (storage *GroupStorage) saveGroupIndex(message_index map[GroupID]*GroupIndex) {
	path := fmt.Sprintf("%s/group_index_t", storage.root)
	log.Info("write group message index path:", path)
	begin := time.Now().UnixNano()
	log.Info("flush group index begin:", begin)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	defer file.Close()

	buffer := new(bytes.Buffer)
	index := 0
	for id, value := range(message_index) {
		binary.Write(buffer, binary.BigEndian, id.appid)
		binary.Write(buffer, binary.BigEndian, id.gid)
		binary.Write(buffer, binary.BigEndian, value.last_msgid)		
		binary.Write(buffer, binary.BigEndian, value.last_id)
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

	path2 := fmt.Sprintf("%s/%s", storage.root, GROUP_INDEX_FILE_NAME)
	err = os.Rename(path, path2)
	if err != nil {
		log.Fatal("rename group index file err:", err)
	}
	
	end := time.Now().UnixNano()
	log.Info("flush group index end:", end, " used:", end - begin)
}

func (storage *GroupStorage) execMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_GROUP_IM_LIST {
		off := msg.body.(*GroupOfflineMessage)
		gi := &GroupIndex{off.msgid, msgid, 0, 0}
		storage.setGroupIndex(off.appid, off.receiver, gi)
	} else if msg.cmd == MSG_GROUP_OFFLINE {
		off := msg.body.(IOfflineMessage).body()
		index := storage.getGroupIndex(off.appid, off.receiver)
		last_id := msgid
		last_batch_id := index.last_batch_id
		last_seq_id := index.last_seq_id + 1
		if last_seq_id%BATCH_SIZE == 0 {
			last_batch_id = msgid
		}

		gi := &GroupIndex{off.msgid, last_id, last_batch_id, last_seq_id}
		storage.setGroupIndex(off.appid, off.receiver, gi)
	}
}
