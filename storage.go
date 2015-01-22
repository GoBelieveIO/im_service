package main

import "os"
import "fmt"
import "bytes"
import "sync"
import "encoding/binary"
import "strconv"
import log "github.com/golang/glog"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/syndtr/goleveldb/leveldb/util"

const HEADER_SIZE = 32
const MAGIC = 0x494d494d
const VERSION = 1 << 16 //1.0

type Storage struct {
	root  string
	db    *leveldb.DB
	mutex sync.Mutex
	file  *os.File
}

type EMessage struct {
	msgid int64
	msg   *Message
}

func NewStorage(root string) *Storage {
	storage := new(Storage)
	storage.root = root

	path := fmt.Sprintf("%s/%s", storage.root, "messages")
	log.Info("message file path:", path)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("open file")
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
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		log.Fatal("open leveldb")
	}

	storage.db = db
	return storage
}

func (storage *Storage) ReadMessage(msg_id int64) *Message {
	_, err := storage.file.Seek(msg_id, os.SEEK_SET)
	if err != nil {
		log.Warning("seek file")
		return nil
	}
	return ReceiveMessage(storage.file)
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

func (storage *Storage) SaveMessage(msg *Message) int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	
	SendMessage(storage.file, msg)
	return msgid
}

func (storage *Storage) EnqueueOffline(msg_id int64, receiver int64) {
	key := fmt.Sprintf("%d_%d", receiver, msg_id)
	value := fmt.Sprintf("%d", msg_id)
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
	off := &OfflineMessage{receiver:receiver, msgid:msg_id}
	storage.SaveMessage(&Message{cmd:MSG_OFFLINE, body:off})
}

func (storage *Storage) DequeueOffline(msg_id int64, receiver int64) {
	key := fmt.Sprintf("%d_%d", receiver, msg_id)
	err := storage.db.Delete([]byte(key), nil)
	if err != nil {
		log.Error("delete err:", err)
	}

	msg := &Message{cmd:MSG_ACK_IN, body:msg_id}
	storage.SaveMessage(msg)
}

func (storage *Storage) LoadOfflineMessage(uid int64) []*EMessage {
	return nil
	c := make([]*EMessage, 0, 10)
	prefix := fmt.Sprintf("%d_", uid)
	r := util.BytesPrefix([]byte(prefix))
	iter := storage.db.NewIterator(r, nil)
	for iter.Next() {
		value := iter.Value()
		msgid, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			log.Error("parseint err:", err)
			continue
		}

		msg := storage.ReadMessage(msgid)
		if msg == nil {
			continue
		}
		c = append(c, &EMessage{msgid:msgid, msg:msg})
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Warning("iterator err:", err)
	}
	
	return c
}
