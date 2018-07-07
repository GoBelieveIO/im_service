
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


type UserID struct {
	appid  int64
	uid    int64
}

type PeerStorage struct {
	*StorageFile
	
	//消息索引全部放在内存中,在程序退出时,再全部保存到文件中，
	//如果索引文件不存在或上次保存失败，则在程序启动的时候，从消息DB中重建索引，这需要遍历每一条消息
	message_index  map[UserID]int64 //记录每个用户最近的消息ID
}

func NewPeerStorage(f *StorageFile) *PeerStorage {
	storage := &PeerStorage{StorageFile:f}
	storage.message_index = make(map[UserID]int64);

	r := storage.readPeerIndex()
	if !r {
		storage.createPeerIndex()
	}
	//程序退出时再生成Index
	storage.removePeerIndex()
	return storage
}

func (storage *PeerStorage) SavePeerMessage(appid int64, uid int64, device_id int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid := storage.saveMessage(msg)

	last_id, _ := storage.getLastMessageID(appid, uid)
	off := &OfflineMessage{appid:appid, receiver:uid, msgid:msgid, device_id:device_id, prev_msgid:last_id}
	m := &Message{cmd:MSG_OFFLINE, body:off}
	last_id = storage.saveMessage(m)

	storage.setLastMessageID(appid, uid, last_id)
	return msgid
}


//获取最近离线消息ID
func (storage *PeerStorage) getLastMessageID(appid int64, receiver int64) (int64, error) {
	id := UserID{appid, receiver}
	return storage.message_index[id], nil;
}

//lock
func (storage *PeerStorage) GetLastMessageID(appid int64, receiver int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastMessageID(appid, receiver)
}

//设置最近离线消息ID
func (storage *PeerStorage) setLastMessageID(appid int64, receiver int64, msg_id int64) {
	id := UserID{appid, receiver}
	storage.message_index[id] = msg_id
}

//lock
func (storage *PeerStorage) SetLastMessageID(appid int64, receiver int64, msg_id int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	storage.setLastMessageID(appid, receiver, msg_id)
}


//获取所有消息id大于msgid的消息,
//todo 优化当消息数量溢出，效率低下问题
func (storage *PeerStorage) LoadHistoryMessages(appid int64, receiver int64, msgid int64, limit int) ([]*EMessage, int64) {
	last_id, err := storage.GetLastMessageID(appid, receiver)
	if err != nil {
		return nil, 0
	}
	overflow := false
	var last_msgid int64
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		off := msg.body.(*OfflineMessage)
		if off.msgid <= msgid {
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
			last_id = off.prev_msgid
			continue
		}
		if last_msgid == 0 {
			last_msgid = off.msgid
		}
		emsg := &EMessage{msgid:off.msgid, device_id:off.device_id, msg:msg}
		messages = append(messages, emsg)

		if len(messages) >= limit*2 {
			copy(messages, messages[limit:])
			messages = messages[:limit]
			overflow = true
			log.Warning("offline message overflow")
		}
		last_id = off.prev_msgid
	}

	if len(messages) > limit {
		messages = messages[:limit]
		overflow = true
		log.Warning("offline message overflow")
	}
	if !overflow {
		last_msgid = 0
	}
	
	return messages, last_msgid
}


func (storage *PeerStorage) LoadLatestMessages(appid int64, receiver int64, limit int) []*EMessage {
	last_id, err := storage.GetLastMessageID(appid, receiver)
	if err != nil {
		return nil
	}
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		off := msg.body.(*OfflineMessage)
		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM && 
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM && 
			msg.cmd != MSG_CUSTOMER && 
			msg.cmd != MSG_CUSTOMER_SUPPORT {
			last_id = off.prev_msgid
			continue
		}

		emsg := &EMessage{msgid:off.msgid, msg:msg}
		messages = append(messages, emsg)
		if len(messages) >= limit {
			break
		}
		last_id = off.prev_msgid
	}
	return messages
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
	last_id, err := storage.GetLastMessageID(appid, uid)
	if err != nil {
		return 0
	}

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
			break
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

			if msg.cmd == MSG_OFFLINE {
				off := msg.body.(*OfflineMessage)
				id := UserID{off.appid, off.receiver}				
				storage.message_index[id] = msgid
			}
		}

		file.Close()
	}
	log.Info("create message index end:", time.Now().UnixNano())
}

func (storage *PeerStorage) readPeerIndex() bool {
	path := fmt.Sprintf("%s/peer_index", storage.root)
	log.Info("read message index path:", path)
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("open file:", err)			
		}
		return false
	}
	defer file.Close()
	data := make([]byte, 24*1000)

	for {
		n, err := file.Read(data)
		if err != nil {
			if err != io.EOF {
				log.Fatal("read err:", err)
			}
			break
		}
		n = n - n%24
		buffer := bytes.NewBuffer(data[:n])
		for i := 0; i < n/24; i++ {
			id := UserID{}
			var msg_id int64
			binary.Read(buffer, binary.BigEndian, &id.appid)
			binary.Read(buffer, binary.BigEndian, &id.uid)
			binary.Read(buffer, binary.BigEndian, &msg_id)

			storage.message_index[id] = msg_id
		}
	}
	return true
}

func (storage *PeerStorage) removePeerIndex() {
	path := fmt.Sprintf("%s/peer_index", storage.root)
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("remove file:", err)
		}
	}
}

//appid uid msgid = 24字节
func (storage *PeerStorage) savePeerIndex() {
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
	for id, value := range(storage.message_index) {
		binary.Write(buffer, binary.BigEndian, id.appid)
		binary.Write(buffer, binary.BigEndian, id.uid)
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
		log.Infof("sync file err:", err)
	}

	path2 := fmt.Sprintf("%s/peer_index", storage.root)
	err = os.Rename(path, path2)
	if err != nil {
		log.Fatal("rename peer index file err:", err)
	}
	
	end := time.Now().UnixNano()
	log.Info("flush peer index end:", end, " used:", end - begin)
}

func (storage *PeerStorage) ExecMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_OFFLINE {
		off := msg.body.(*OfflineMessage)
		storage.SetLastMessageID(off.appid, off.receiver, msgid)
	}
}
