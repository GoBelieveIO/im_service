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
import "github.com/syndtr/goleveldb/leveldb/opt"

const HEADER_SIZE = 32
const MAGIC = 0x494d494d
const VERSION = 1 << 16 //1.0

type OfflineComparer struct{}

//appid, uid, msgid
func (oc OfflineComparer) Split(a []byte) ([]byte, []byte, []byte) {
	index1 := bytes.IndexByte(a, '_')
	if index1 == -1 || index1 + 1 >= len(a) {
		return nil, nil, nil
	}
	index2 := bytes.IndexByte(a[index1+1:], '_')
	if index2 == -1 || index2 + 1 >= len(a) {
		return nil, nil, nil
	}
	
	return a[:index1], a[index1+1:index1+1+index2], a[index1+1+index2+1:]
}

func (oc OfflineComparer) Compare(a, b []byte) int {
	p1, p2, p3 := oc.Split(a)
	p4, p5, p6 := oc.Split(b)

	if p1 == nil || p4 == nil {
		log.Infof("can't find seperate, a:%s b:%s compare bytes...\n", string(a), string(b))
		return bytes.Compare(a, b)
	}

	r1 := bytes.Compare(p1, p4)
	if r1 != 0 {
		return r1
	}

	r2 := bytes.Compare(p2, p5)
	if r2 != 0 {
		return r2
	}

	v1, err1 := strconv.ParseInt(string(p3), 10, 64)
	v2, err2 := strconv.ParseInt(string(p6), 10, 64)
	if err1 != nil || err2 != nil {
		log.Infof("parse int err, a:%s b:%s compare bytes...\n", string(a), string(b))
		return bytes.Compare(p3, p6)
	}

	if v1 < v2 {
		return -1
	} else if v1 == v2 {
		return 0
	} else {
		return 1
	}
}

func (OfflineComparer) Name() string {
	return "im.OfflineComparator"
}

func (OfflineComparer) Separator(dst, a, b []byte) []byte {
	return nil
}

func (OfflineComparer) Successor(dst, b []byte) []byte {

	return nil
}


type Storage struct {
	root  string
	db    *leveldb.DB
	mutex sync.Mutex
	file  *os.File
}

func NewStorage(root string) *Storage {
	storage := new(Storage)
	storage.root = root

	path := fmt.Sprintf("%s/%s", storage.root, "messages")
	log.Info("message file path:", path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
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
	opt := &opt.Options{Comparer:OfflineComparer{}}
	db, err := leveldb.OpenFile(path, opt)
	if err != nil {
		log.Fatal("open leveldb:", err)
	}

	storage.db = db

	storage.ListKeyValue()
	
	return storage
}

func (storage *Storage) ListKeyValue() {
	iter := storage.db.NewIterator(nil, nil)
	for iter.Next() {
		log.Info("key:", string(iter.Key()), " value:", string(iter.Value()))
	}
}
func (storage *Storage) ReadMessage(msg_id int64) *Message {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
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

func (storage *Storage) EnqueueOffline(msg_id int64, appid int64, receiver int64) {
	key := fmt.Sprintf("%d_%d_%d", appid, receiver, msg_id)
	value := fmt.Sprintf("%d", msg_id)
	err := storage.db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		log.Error("put err:", err)
		return
	}
	log.Infof("enqueue offline:%s %d %d %d\n", key, appid, receiver, msg_id)
	off := &OfflineMessage{appid:appid, receiver:receiver, msgid:msg_id}
	storage.SaveMessage(&Message{cmd:MSG_OFFLINE, body:off})
}

func (storage *Storage) DequeueOffline(msg_id int64, appid int64, receiver int64) {
	key := fmt.Sprintf("%d_%d_%d", appid, receiver, msg_id)
	has, err := storage.db.Has([]byte(key), nil)
	if err != nil {
		log.Error("check key err:", err)
	}
	if !has {
		log.Info("no offline msg:", appid, receiver, msg_id)
		return
	}

	err = storage.db.Delete([]byte(key), nil)
	if err != nil {
		//can't get ErrNotFound
		log.Error("delete err:", err)
	}

	log.Infof("dequeue offline:%s %d %d %d\n", key, appid, receiver, msg_id)
	off := &OfflineMessage{appid:appid, receiver:receiver, msgid:msg_id}
	msg := &Message{cmd:MSG_ACK_IN, body:off}
	storage.SaveMessage(msg)
}

func (storage *Storage) LoadOfflineMessage(appid int64, uid int64) []*EMessage {
	log.Infof("load offline message appid:%d uid:%d\n", appid, uid)
	c := make([]*EMessage, 0, 10)
	start := fmt.Sprintf("%d_%d_0", appid, uid)
	end := fmt.Sprintf("%d_%d_9223372036854775807", appid, uid)

	r := &util.Range{Start:[]byte(start), Limit:[]byte(end)}
	iter := storage.db.NewIterator(r, nil)
	for iter.Next() {
		value := iter.Value()
		msgid, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			log.Error("parseint err:", err)
			continue
		}
		log.Info("offline msgid:", msgid)
		msg := storage.ReadMessage(msgid)
		if msg == nil {
			log.Error("can't load offline message:", msgid)
			continue
		}
		c = append(c, &EMessage{msgid:msgid, msg:msg})
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Warning("iterator err:", err)
	}
	log.Info("offline count:", len(c))
	return c
}
