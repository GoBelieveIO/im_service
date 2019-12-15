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
import "time"
import "encoding/binary"
import "sync/atomic"
import log "github.com/golang/glog"



const HEADER_SIZE = 32
const MAGIC = 0x494d494d
const F_VERSION = 1 << 16 //1.0


type GroupLoader struct {
	gid int64
	c   chan *Group
}

//后台发送普通群消息
//普通群消息首先保存到临时文件中，之后按照保存到文件中的顺序依次派发
type GroupMessageDeliver struct {
	root                string
	mutex               sync.Mutex //写文件的锁
	file                *os.File
	
	cursor_file         *os.File //不会被并发访问
	
	latest_msgid        int64 //最近保存的消息id
	latest_sended_msgid int64 //最近发送出去的消息id

	wt     chan int64	 //通知有新消息等待发送

	//保证单个群组结构只会在一个线程中被加载
	lt     chan *GroupLoader //加载group结构到内存
	dt     chan *AppMessage  //dispatch 群组消息


	cb_mutex               sync.Mutex //callback变量的锁
	id int64      //自增的callback id	
	callbacks     map[int64]chan *Metadata //返回保存到ims的消息id
	callbackid_to_msgid map[int64]int64 //callback -> msgid
}

func NewGroupMessageDeliver(root string) *GroupMessageDeliver {
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
	storage.lt = make(chan *GroupLoader)
	storage.dt = make(chan *AppMessage, 1000)
	storage.callbacks = make(map[int64]chan *Metadata)
	storage.callbackid_to_msgid = make(map[int64]int64)
	
	storage.openWriteFile()
	storage.openCursorFile()
	storage.readLatestMessageID()
	return storage
}

func (storage *GroupMessageDeliver) openCursorFile() {
	path := fmt.Sprintf("%s/cursor", storage.root)
	log.Info("open/create cursor file path:", path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	file_size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal("seek file")
	}
	if file_size < 8 && file_size > 0 {
		log.Info("file header is't complete")
		err = file.Truncate(0)
		if err != nil {
			log.Fatal("truncate file")
		}
		file_size = 0
	}

	var cursor int64	
	if file_size == 0 {
		err = binary.Write(file, binary.BigEndian, cursor)
		if err != nil {
			log.Fatal("write file")
		}
	} else {
		_, err = file.Seek(0, os.SEEK_SET)
		if err != nil {
			log.Fatal("seek file")
		}
		err = binary.Read(file, binary.BigEndian, &cursor)
		if err != nil {
			log.Fatal("read file")
		}
	}

	storage.latest_sended_msgid = cursor
	storage.cursor_file = file
}

//保存最近发出的消息id到cursor file
func (storage *GroupMessageDeliver) saveCursor() {
	_, err := storage.cursor_file.Seek(0, os.SEEK_SET)
	if err != nil {
		log.Fatal("write file")
	}

	err = binary.Write(storage.cursor_file, binary.BigEndian, storage.latest_sended_msgid)

	if err != nil {
		log.Fatal("write file")
	}
}

func (storage *GroupMessageDeliver) openWriteFile() {
	path := fmt.Sprintf("%s/pending_group_messages", storage.root)
	log.Info("open/create message file path:", path)
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
}

func (storage *GroupMessageDeliver) readLatestMessageID() {
	offset, err := storage.file.Seek(-8, os.SEEK_END)

	if offset <= HEADER_SIZE {
		storage.latest_msgid = 0
		return
	}

	buff := make([]byte, 8)
	n, err := storage.file.Read(buff)
	if err != nil || n != 8 {
		log.Fatal("read file")
	}

	buffer := bytes.NewBuffer(buff)
	var msg_len, magic int32
	binary.Read(buffer, binary.BigEndian, &msg_len)
	binary.Read(buffer, binary.BigEndian, &magic)

	if magic != MAGIC {
		log.Fatal("file need repair")
	}

	storage.latest_msgid = offset - int64(msg_len) - 8
}

func (storage *GroupMessageDeliver) ReadMessage(file *os.File) *Message {
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

	var msg_len int32
	err = binary.Read(file, binary.BigEndian, &msg_len)
	if err != nil {
		log.Info("read file err:", err)
		return nil
	}
	
	msg := ReceiveStorageMessage(file)
	if msg == nil {
		return msg
	}

	err = binary.Read(file, binary.BigEndian, &msg_len)
	if err != nil {
		log.Info("read file err:", err)
		return nil
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

func (storage *GroupMessageDeliver) LoadMessage(msg_id int64) *Message {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	offset := msg_id

	file := storage.file
	_, err := file.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		log.Warning("seek file")
		return nil
	}
	return storage.ReadMessage(file)
}

func (storage *GroupMessageDeliver) ReadHeader(file *os.File) (magic int, version int) {
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

func (storage *GroupMessageDeliver) WriteHeader(file *os.File) {
	var m int32 = MAGIC
	err := binary.Write(file, binary.BigEndian, m)
	if err != nil {
		log.Fatalln(err)
	}
	var v int32 = F_VERSION
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


//save without lock
func (storage *GroupMessageDeliver) saveMessage(msg *Message) int64 {
	msgid, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(MAGIC))

	body := msg.ToData()
	var msg_len int32 = MSG_HEADER_SIZE + int32(len(body))
	binary.Write(buffer, binary.BigEndian, msg_len)
	
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd),
		byte(msg.version), byte(msg.flag), buffer)
	buffer.Write(body)

	binary.Write(buffer, binary.BigEndian, msg_len)
	binary.Write(buffer, binary.BigEndian, int32(MAGIC))
	buf := buffer.Bytes()

	n, err := storage.file.Write(buf)
	if err != nil {
		log.Fatal("file write err:", err)
	}
	if n != len(buf) {
		log.Fatal("file write size:", len(buf), " nwrite:", n)
	}

	log.Info("save message:", Command(msg.cmd), " ", msgid)
	return msgid
	
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



func (storage *GroupMessageDeliver) openReadFile() *os.File {
	//open file readonly mode
	path := fmt.Sprintf("%s/pending_group_messages", storage.root)
	log.Info("open message block file path:", path)
	file, err := os.Open(path)
	if err != nil {
		log.Error("open pending_group_messages error:", err)
		return nil
	}
	return file
}


//device_ID 发送者的设备ID
func (storage *GroupMessageDeliver) sendMessage(appid int64, uid int64, sender int64, device_ID int64, msg *Message) bool {

	PublishMessage(appid, uid, msg)

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't send message, appid:%d uid:%d cmd:%s", appid, uid, Command(msg.cmd))
		return false
	}
	clients := route.FindClientSet(uid)
	if len(clients) == 0 {
		return false
	}

	for c, _ := range(clients) {
		//不再发送给自己
		if c.device_ID == device_ID && sender == uid {
			continue
		}

		c.EnqueueNonBlockMessage(msg)
	}

	return true
}

func (storage *GroupMessageDeliver) sendGroupMessage(gm *PendingGroupMessage) (*Metadata, bool) {
	msg := &IMMessage{sender: gm.sender, receiver: gm.gid, timestamp: gm.timestamp, content: gm.content}
	m := &Message{cmd: MSG_GROUP_IM, version:DEFAULT_VERSION, body: msg}

	metadata := &Metadata{}

	batch_members := make(map[int64][]int64)
	for _, member := range gm.members {
		index := GetStorageRPCIndex(member)
		if _, ok := batch_members[index]; !ok {
			batch_members[index] = []int64{member}
		} else {
			mb := batch_members[index]
			mb = append(mb, member)
			batch_members[index] = mb
		}
	}
	
	for _, mb := range(batch_members) {
		r, err := SavePeerGroupMessage(gm.appid, mb, gm.device_ID, m)
		if err != nil {
			log.Errorf("save peer group message:%d %d err:%s", gm.sender, gm.gid, err)
			return nil, false
		}
		if len(r) != len(mb)*2 {
			log.Errorf("save peer group message err:%d %d", len(r), len(mb))
			return nil, false
		}

		for i := 0; i < len(r); i += 2 {
			msgid, prev_msgid := r[i], r[i+1]
			member := mb[i/2]
			meta := &Metadata{sync_key:msgid, prev_sync_key:prev_msgid}
			mm := &Message{cmd:MSG_GROUP_IM, version:DEFAULT_VERSION,
				flag:MESSAGE_FLAG_PUSH, body:msg, meta:meta}
			storage.sendMessage(gm.appid, member, gm.sender, gm.device_ID, mm)

			notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
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
	PushGroupMessage(gm.appid, group, m)
	return metadata, true
}

func (storage *GroupMessageDeliver) sendPendingMessage() {
	file := storage.openReadFile()
	if file == nil {
		return
	}
	defer file.Close()
	
	offset := storage.latest_sended_msgid
	if offset == 0 {
		offset = HEADER_SIZE
	}
	
	_, err := file.Seek(offset, os.SEEK_SET)
	if err != nil {
		log.Error("seek file err:", err)
		return
	}

	for {
		msgid, err := file.Seek(0, os.SEEK_CUR)
		if err != nil {
			log.Error("seek file err:", err)
			break
		}
		msg := storage.ReadMessage(file)
		if msg == nil {
			break
		}

		if msgid <= storage.latest_sended_msgid {
			continue
		}

		if msg.cmd != MSG_PENDING_GROUP_MESSAGE {
			continue
		}

		gm := msg.body.(*PendingGroupMessage)
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

func (storage *GroupMessageDeliver) truncateFile() {
	err := storage.file.Truncate(HEADER_SIZE)
	if err != nil {
		log.Fatal("truncate err:", err)
	}

	storage.latest_msgid = 0
	storage.latest_sended_msgid = 0
	storage.saveCursor()
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
	
	for  {
		select {
		case <-storage.wt:
			storage.flushPendingMessage()			
		case <-time.After(time.Second * 30):
			storage.flushPendingMessage()
		}
	}
}


func (storage *GroupMessageDeliver) LoadGroup(gid int64) *Group {
	group := group_manager.FindGroup(gid)
	if group != nil {
		return group
	}
	
	l := &GroupLoader{gid, make(chan *Group)}
	storage.lt <- l

	group = <- l.c
	return group
}


func (storage *GroupMessageDeliver) DispatchMessage(msg *AppMessage) {
	group := group_manager.FindGroup(msg.receiver)
	if group != nil {
		DispatchMessageToGroup(msg.msg, group, msg.appid, nil)
	} else {
		select {
		case storage.dt <- msg:
		default:
			log.Warning("can't dispatch group message nonblock")
		}		
	}
}

func (storage *GroupMessageDeliver) dispatchMessage(msg *AppMessage) {
	group := group_manager.LoadGroup(msg.receiver)
	if group == nil {
		log.Warning("load group nil, can't dispatch group message")
		return
	}
	DispatchMessageToGroup(msg.msg, group, msg.appid, nil)
}

func (storage *GroupMessageDeliver) loadGroup(gl *GroupLoader) {
	group := group_manager.LoadGroup(gl.gid)
	gl.c <- group
}

func (storage *GroupMessageDeliver) run2() {
	log.Info("group message deliver running loop2")

	for  {
		select {
		case gl := <-storage.lt:
			storage.loadGroup(gl)			
		case m := <-storage.dt:
			storage.dispatchMessage(m)
		}
	}
}


func (storage *GroupMessageDeliver) Start() {
	go storage.run()
	go storage.run2()	
}
