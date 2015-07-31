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

import "os"
import "fmt"
import "bytes"
import "sync"
import "encoding/binary"
import "strconv"
import "io"
import log "github.com/golang/glog"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/opt"

const HEADER_SIZE = 32
const MAGIC = 0x494d494d
const VERSION = 1 << 16 //1.0


type Storage struct {
	root      string
	db        *leveldb.DB
	mutex     sync.Mutex
	file      *os.File

	group_received   map[AppGroupMemberID]int64
	received         map[AppUserID]int64
}

func NewStorage(root string) *Storage {
	storage := new(Storage)
	storage.group_received = make(map[AppGroupMemberID]int64)
	storage.received = make(map[AppUserID]int64)

	storage.root = root

	path := fmt.Sprintf("%s/%s", storage.root, "messages")
	log.Info("message file path:", path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	file_size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("seek file")
	}
	if file_size < HEADER_SIZE && file_size > 0 {
		log.Info("file header is't complete")
		err = file.Truncate(0)
		if err != nil {
			log.Fatal("truncate file")
		}
		file_size = 0
	}
	if file_size == 0 {
		storage.WriteHeader(file)
	}
	storage.file = file

	path = fmt.Sprintf("%s/%s", storage.root, "offline")
	option := &opt.Options{}
	db, err := leveldb.OpenFile(path, option)
	if err != nil {
		log.Fatal("open leveldb:", err)
	}

	storage.db = db
	
	return storage
}

func (storage *Storage) ListKeyValue() {
	iter := storage.db.NewIterator(nil, nil)
	for iter.Next() {
		log.Info("key:", string(iter.Key()), " value:", string(iter.Value()))
	}
}

func (storage *Storage) ReadMessage(file *os.File) *Message {
	//校验消息起始位置的magic
	var magic int32
	err := binary.Read(file, binary.BigEndian, &magic)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}

	if magic != MAGIC {
		log.Warning("magic err:", magic)
		return nil
	}
	msg := ReceiveMessage(file)
	if msg == nil {
		return msg
	}
	
	err = binary.Read(file, binary.BigEndian, &magic)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}
	
	if magic != MAGIC {
		log.Warning("magic err:", magic)
		return nil
	}
	return msg
}

func (storage *Storage) LoadMessage(msg_id int64) *Message {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	_, err := storage.file.Seek(msg_id, os.SEEK_SET)
	if err != nil {
		log.Warning("seek file")
		return nil
	}
	return storage.ReadMessage(storage.file)
}

func (storage *Storage) ReadHeader(file *os.File) (magic int, version int) {
	header := make([]byte, HEADER_SIZE)
	n, err := file.Read(header)
	if err != nil || n != HEADER_SIZE {
		return
	}
	buffer := bytes.NewBuffer(header)
	var m, v int32
	binary.Read(buffer, binary.BigEndian, &m)
	binary.Read(buffer, binary.BigEndian, &v)
	magic = int(m)
	version = int(v)
	return
}

func (storage *Storage) WriteHeader(file *os.File) {
	var m int32 = MAGIC
	err := binary.Write(file, binary.BigEndian, m)
	if err != nil {
		log.Fatalln(err)
	}
	var v int32 = VERSION
	err = binary.Write(file, binary.BigEndian, v)
	if err != nil {
		log.Fatalln(err)
	}
	pad := make([]byte, HEADER_SIZE-8)
	n, err := file.Write(pad)
	if err != nil || n != (HEADER_SIZE-8) {
		log.Fatalln(err)
	}
}

func (storage *Storage) WriteMessage(file io.Writer, msg *Message) {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(MAGIC))
	WriteMessage(buffer, msg)
	binary.Write(buffer, binary.BigEndian, int32(MAGIC))
	buf := buffer.Bytes()
	n, err := file.Write(buf)
	if err != nil {
		log.Fatal("file write err:", err)
	}
	if n != len(buf) {
		log.Fatal("file write size:", len(buf), " nwrite:", n)
	}
}

//save without lock
func (storage *Storage) saveMessage(msg *Message) int64 {
	msgid, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	storage.WriteMessage(storage.file, msg)
	master.ewt <- &EMessage{msgid:msgid, msg:msg}
	log.Info("save message:", Command(msg.cmd), " ", msgid)
	return msgid
	
}

func (storage *Storage) SaveMessage(msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.saveMessage(msg)
}

func (storage *Storage) SavePeerMessage(appid int64, uid int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid := storage.saveMessage(msg)

	last_id, _ := storage.GetLastMessageID(appid, uid)
	off := &OfflineMessage{appid:appid, receiver:uid, msgid:msgid, prev_msgid:last_id}
	m := &Message{cmd:MSG_OFFLINE, body:off}
	last_id = storage.saveMessage(m)

	storage.SetLastMessageID(appid, uid, last_id)
	return msgid
}

//获取最近离线消息ID
func (storage *Storage) GetLastMessageID(appid int64, receiver int64) (int64, error) {
	key := fmt.Sprintf("%d_%d_0", appid, receiver)
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

//设置最近离线消息ID
func (storage *Storage) SetLastMessageID(appid int64, receiver int64, msg_id int64) {
	key := fmt.Sprintf("%d_%d_0", appid, receiver)
	value := fmt.Sprintf("%d", msg_id)
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *Storage) SetLastReceivedID(appid int64, uid int64, msgid int64) {
	key := fmt.Sprintf("%d_%d_1", appid, uid)
	value := fmt.Sprintf("%d", msgid)
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *Storage) getLastReceivedID(appid int64, uid int64) (int64, error) {
	key := fmt.Sprintf("%d_%d_1", appid, uid)

	id := AppUserID{appid:appid, uid:uid}
	if msgid, ok := storage.received[id]; ok {
		return msgid, nil
	}

	value, err := storage.db.Get([]byte(key), nil)
	if err != nil {
		log.Error("put err:", err)
		return 0, err
	}

	msgid, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		log.Error("parseint err:", err)
		return 0, err
	}
	return msgid, nil
}

func (storage *Storage) GetLastReceivedID(appid int64, uid int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastReceivedID(appid, uid)
}


func (storage *Storage) DequeueOffline(msg_id int64, appid int64, receiver int64) {
	log.Infof("dequeue offline:%d %d %d\n", appid, receiver, msg_id)
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	last, _ := storage.getLastReceivedID(appid, receiver)
	if msg_id <= last {
		log.Infof("ack msgid:%d last:%d\n", msg_id, last)
		return
	}
	id := AppUserID{appid:appid, uid:receiver}
	storage.received[id] = msg_id
}

func (storage *Storage) SaveGroupMessage(appid int64, gid int64, msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	msgid := storage.saveMessage(msg)

	last_id, _ := storage.GetLastGroupMessageID(appid, gid)
	lt := &GroupOfflineMessage{appid:appid, gid:gid, msgid:msgid, prev_msgid:last_id}
	m := &Message{cmd:MSG_GROUP_IM_LIST, body:lt}
	
	last_id = storage.saveMessage(m)
	storage.SetLastGroupMessageID(appid, gid, last_id)
	return msgid
}

func (storage *Storage) SetLastGroupMessageID(appid int64, gid int64, msgid int64) {
	key := fmt.Sprintf("g_%d_%d", appid, gid)
	value := fmt.Sprintf("%d", msgid)	
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *Storage) GetLastGroupMessageID(appid int64, gid int64) (int64, error) {
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

func (storage *Storage) SetLastGroupReceivedID(appid int64, gid int64, uid int64, msgid int64) {
	key := fmt.Sprintf("g_%d_%d_%d", appid, gid, uid)
	value := fmt.Sprintf("%d", msgid)	
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
}

func (storage *Storage) getLastGroupReceivedID(appid int64, gid int64, uid int64) (int64, error) {
	key := fmt.Sprintf("g_%d_%d_%d", appid, gid, uid)

	id := AppGroupMemberID{appid:appid, gid:gid, uid:uid}
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

func (storage *Storage) GetLastGroupReceivedID(appid int64, gid int64, uid int64) (int64, error) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastGroupReceivedID(appid, gid, uid)
}

func (storage *Storage) DequeueGroupOffline(msg_id int64, appid int64, gid int64, receiver int64) {
	log.Infof("dequeue group offline:%d %d %d %d\n", appid, gid, receiver, msg_id)
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	last, _ := storage.getLastGroupReceivedID(appid, gid, receiver)
	if msg_id <= last {
		log.Infof("group ack msgid:%d last:%d\n", msg_id, last)
		return
	}
	id := AppGroupMemberID{appid:appid, gid:gid, uid:receiver}
	storage.group_received[id] = msg_id
}

func (storage *Storage) FlushReceived() {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	if len(storage.received) > 0 || len(storage.group_received) > 0 {
		log.Infof("flush received:%d %d\n", len(storage.received), len(storage.group_received))
	}

	if len(storage.received) > 0 {
		for id, msg_id := range storage.received {
			storage.SetLastReceivedID(id.appid, id.uid, msg_id)
			off := &OfflineMessage{appid:id.appid, receiver:id.uid, msgid:msg_id}
			msg := &Message{cmd:MSG_ACK_IN, body:off}
			storage.saveMessage(msg)
		}
		storage.received = make(map[AppUserID]int64)
	}

	if len(storage.group_received) > 0 {
		for id, msg_id := range storage.group_received {
			storage.SetLastGroupReceivedID(id.appid, id.gid, id.uid, msg_id)
			off := &GroupOfflineMessage{appid:id.appid, receiver:id.uid, msgid:msg_id, gid:id.gid}
			msg := &Message{cmd:MSG_GROUP_ACK_IN, body:off}
			storage.saveMessage(msg)
		}
		storage.group_received = make(map[AppGroupMemberID]int64)
	}
}

func (storage *Storage) LoadOfflineMessage(appid int64, uid int64) []*EMessage {
	last_id, err := storage.GetLastMessageID(appid, uid)
	if err != nil {
		return nil
	}

	last_received_id, _ := storage.GetLastReceivedID(appid, uid)
	c := make([]*EMessage, 0, 10)
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

		m := storage.LoadMessage(off.msgid)
		c = append(c, &EMessage{msgid:off.msgid, msg:m})

		msgid = off.prev_msgid
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

	log.Infof("load offline message appid:%d uid:%d count:%d\n", appid, uid, len(c))
	return c
}

func (storage *Storage) LoadGroupOfflineMessage(appid int64, gid int64, uid int64, limit int) []*EMessage {
	last_id, err := storage.GetLastGroupMessageID(appid, gid)
	if err != nil {
		log.Info("get last group message id err:", err)
		return nil
	}

	last_received_id, _ := storage.GetLastGroupReceivedID(appid, gid, uid)

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
		c = append(c, &EMessage{msgid:off.msgid, msg:m})

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

func (storage *Storage) NextMessageID() int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	return msgid
}

func (storage *Storage) ExecMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_OFFLINE {
		off := msg.body.(*OfflineMessage)
		storage.SetLastMessageID(off.appid, off.receiver, msgid)
	} else if msg.cmd == MSG_ACK_IN {
		off := msg.body.(*OfflineMessage)
		storage.SetLastReceivedID(off.appid, off.receiver, off.msgid)
	} else if msg.cmd == MSG_GROUP_IM_LIST {
		off := msg.body.(*GroupOfflineMessage)
		storage.SetLastGroupMessageID(off.appid, off.gid, msgid)
	} else if msg.cmd == MSG_GROUP_ACK_IN {
		off := msg.body.(*GroupOfflineMessage)
		storage.SetLastGroupReceivedID(off.appid, off.gid, off.receiver, msgid)
	}
}

func (storage *Storage) SaveSyncMessageBatch(mb *MessageBatch) error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	filesize, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	if mb.first_id != filesize {
		log.Warningf("file size:%d, msgid:%d is't equal", filesize, mb.first_id)
		if mb.first_id < filesize {
			log.Warning("skip msg:", mb.first_id)
		} else {
			log.Warning("write padding:", mb.first_id-filesize)
			padding := make([]byte, mb.first_id - filesize)
			_, err = storage.file.Write(padding)
			if err != nil {
				log.Fatal("file write:", err)
			}
		}
	}
	
	id := mb.first_id
	buffer := new(bytes.Buffer)
	for _, m := range mb.msgs {
		storage.ExecMessage(m, id)
		storage.WriteMessage(buffer, m)
		id += int64(buffer.Len())
	}

	buf := buffer.Bytes()
	_, err = storage.file.Write(buf)
	if err != nil {
		log.Fatal("file write:", err)
	}

	log.Infof("save batch sync message first id:%d last id:%d\n", mb.first_id, mb.last_id)
	return nil
}

func (storage *Storage) SaveSyncMessage(emsg *EMessage) error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	
	filesize, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	if emsg.msgid != filesize {
		log.Warningf("file size:%d, msgid:%d is't equal", filesize, emsg.msgid)
		if emsg.msgid < filesize {
			log.Warning("skip msg:", emsg.msgid)
		} else {
			log.Warning("write padding:", emsg.msgid-filesize)
			padding := make([]byte, emsg.msgid - filesize)
			_, err = storage.file.Write(padding)
			if err != nil {
				log.Fatal("file write:", err)
			}
		}
	}
	
	storage.WriteMessage(storage.file, emsg.msg)
	storage.ExecMessage(emsg.msg, emsg.msgid)
	log.Info("save sync message:", emsg.msgid)
	return nil
}

func (storage *Storage) LoadLatestMessages(appid int64, receiver int64, limit int) []*EMessage {
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
		if msg.cmd != MSG_GROUP_IM && msg.cmd != MSG_IM {
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


func (storage *Storage) LoadSyncMessagesInBackground(msgid int64) chan *MessageBatch {
	c := make(chan *MessageBatch, 10)
	go func() {
		defer close(c)
		path := fmt.Sprintf("%s/%s", storage.root, "messages")
		log.Info("message file path:", path)
		file, err := os.Open(path)
		if err != nil {
			log.Info("open file err:", err)
			return
		}
		defer file.Close()

		file_size, err := file.Seek(0, os.SEEK_END)
		if err != nil {
			log.Fatal("seek file err:", err)
			return
		}
		if file_size < HEADER_SIZE {
			log.Info("file header is't complete")
			return
		}
		
		_, err = file.Seek(msgid, os.SEEK_SET)
		if err != nil {
			log.Info("seek file err:", err)
			return
		}
		
		const BATCH_COUNT = 5000
		batch := &MessageBatch{msgs:make([]*Message, 0, BATCH_COUNT)}
		for {
			msgid, err = file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := storage.ReadMessage(file)
			if msg == nil {
				break
			}

			if batch.first_id == 0 {
				batch.first_id = msgid
			}

			batch.last_id = msgid
			batch.msgs = append(batch.msgs, msg)

			if len(batch.msgs) >= BATCH_COUNT {
				c <- batch
				batch = &MessageBatch{msgs:make([]*Message, 0, BATCH_COUNT)}
			}
		}
		if len(batch.msgs) > 0 {
			c <- batch
		}
	}()
	return c
}
