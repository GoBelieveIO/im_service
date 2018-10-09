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

type GroupID struct {
	appid  int64
	gid    int64
}

type GroupStorage struct {
	*StorageFile

	message_index  map[GroupID]int64 //记录每个群组最近的消息ID
}

func NewGroupStorage(f *StorageFile) *GroupStorage {
	storage := &GroupStorage{StorageFile:f}
	storage.message_index = make(map[GroupID]int64)
	return storage
}

func (storage *GroupStorage) SaveGroupMessage(appid int64, gid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	msgid := storage.saveMessage(msg)

	last_id, _ := storage.getLastGroupMessageID(appid, gid)
	lt := &GroupOfflineMessage{appid:appid, gid:gid, msgid:msgid, device_id:device_id, prev_msgid:last_id}
	m := &Message{cmd:MSG_GROUP_IM_LIST, body:lt}
	
	last_id = storage.saveMessage(m)
	storage.setLastGroupMessageID(appid, gid, last_id)
	return msgid
}

func (storage *GroupStorage) setLastGroupMessageID(appid int64, gid int64, msgid int64) {
	id := GroupID{appid, gid}
	storage.message_index[id] = msgid
	if msgid >	storage.last_id {
		storage.last_id = msgid
	}
}

func (storage *GroupStorage) SetLastGroupMessageID(appid int64, gid int64, msgid int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	storage.setLastGroupMessageID(appid, gid, msgid)
}

func (storage *GroupStorage) getLastGroupMessageID(appid int64, gid int64) (int64, error) {
	id := GroupID{appid, gid}
	return storage.message_index[id], nil
}

func (storage *GroupStorage) GetLastGroupMessageID(appid int64, gid int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	return storage.getLastGroupMessageID(appid, gid)
}

//获取所有消息id大于msgid的消息
//ts:入群时间
func (storage *GroupStorage) LoadGroupHistoryMessages(appid int64, uid int64, gid int64, msgid int64, ts int32, limit int) ([]*EMessage, int64) {
	log.Infof("load group history message:%d %d", msgid, ts)
	last_id, err := storage.GetLastGroupMessageID(appid, gid)
	if err != nil {
		log.Info("get last group message id err:", err)
		return nil, 0
	}
	var last_msgid int64
	c := make([]*EMessage, 0, 10)

	for ; last_id > 0; {
		msg := storage.LoadMessage(last_id)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		if msg.cmd != MSG_GROUP_IM_LIST {
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*GroupOfflineMessage)
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

			if msg.cmd == MSG_GROUP_IM_LIST {
				off := msg.body.(*GroupOfflineMessage)
				id := GroupID{off.appid, off.gid}
				storage.message_index[id] = msgid
				if msgid > storage.last_id {
					storage.last_id = msgid
				}
			}
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

			if msg.cmd == MSG_GROUP_IM_LIST {
				off := msg.body.(*GroupOfflineMessage)
				id := GroupID{off.appid, off.gid}
				block_NO := i
				msgid = int64(block_NO)*BLOCK_SIZE + msgid
				storage.message_index[id] = msgid
				if msgid > storage.last_id {
					storage.last_id = msgid
				}
			}
		}

		file.Close()
	}
	log.Info("repair group message index end:", time.Now().UnixNano())	
}


func (storage *GroupStorage) readGroupIndex() bool {
	path := fmt.Sprintf("%s/group_index", storage.root)
	log.Info("read group message index path:", path)
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("open file:", err)			
		}
		return false
	}
	defer file.Close()
	const INDEX_SIZE = 24
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
			var msg_id int64
			binary.Read(buffer, binary.BigEndian, &id.appid)
			binary.Read(buffer, binary.BigEndian, &id.gid)
			binary.Read(buffer, binary.BigEndian, &msg_id)

			storage.message_index[id] = msg_id
			if msg_id > storage.last_id {
				storage.last_id = msg_id
			}
		}
	}
	return true
}

func (storage *GroupStorage) removeGroupIndex() {
	path := fmt.Sprintf("%s/group_index", storage.root)
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("remove file:", err)
		}
	}
}

func (storage *GroupStorage) cloneGroupIndex() map[GroupID]int64 {
	message_index := make(map[GroupID]int64)
	for k, v := range(storage.message_index) {
		message_index[k] = v
	}
	return message_index
}


//appid gid msgid = 24字节
func (storage *GroupStorage) saveGroupIndex(message_index map[GroupID]int64) {
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
		binary.Write(buffer, binary.BigEndian, value)

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

	path2 := fmt.Sprintf("%s/group_index", storage.root)
	err = os.Rename(path, path2)
	if err != nil {
		log.Fatal("rename group index file err:", err)
	}
	
	end := time.Now().UnixNano()
	log.Info("flush group index end:", end, " used:", end - begin)
}

func (storage *GroupStorage) ExecMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_GROUP_IM_LIST {
		off := msg.body.(*GroupOfflineMessage)
		storage.SetLastGroupMessageID(off.appid, off.gid, msgid)
	}
}
