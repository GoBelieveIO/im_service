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
import "net"
import "fmt"
import "bytes"
import "time"
import "runtime"
import "flag"
import "encoding/binary"
import log "github.com/golang/glog"

var storage *Storage
var config *StorageConfig
var master *Master
var group_manager *GroupManager

type Client struct {
	conn   *net.TCPConn
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn 
	return client
}

func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) HandleSaveAndEnqueueGroup(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}
	msgid := storage.SaveMessage(sae.msg)
	if sae.msg.cmd != MSG_GROUP_IM {
		log.Error("sae msg cmd:", sae.msg.cmd)
		return
	}
	appid := sae.appid
	im := sae.msg.body.(*IMMessage)
	gid := im.receiver

	group := group_manager.FindGroup(gid)
	if group == nil {
		log.Warning("can't find group:", gid)
	}
	members := group.Members()
	for member := range members {
		if im.sender == member {
			continue
		}
		storage.EnqueueGroupOffline(msgid, appid, gid, member)		
	}

	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleDQGroupMessage(dq *DQMessage) {
	storage.DequeueGroupOffline(dq.msgid, dq.appid, dq.gid, dq.receiver)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleSaveAndEnqueue(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}
	msgid := storage.SaveMessage(sae.msg)
	for _, r := range(sae.receivers) {
		storage.EnqueueOffline(msgid, sae.appid, r)
	}
	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleDQMessage(dq *DQMessage) {
	storage.DequeueOffline(dq.msgid, dq.appid, dq.receiver)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) WriteEMessage(emsg *EMessage) []byte{
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	SendMessage(buffer, emsg.msg)
	return buffer.Bytes()
}

func (client *Client) HandleLoadOffline(app_user_id *AppUserID) {
	messages := storage.LoadOfflineMessage(app_user_id.appid, app_user_id.uid)
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleLoadHistory(lh *LoadHistory) {
	messages := storage.LoadLatestMessages(lh.app_uid.appid, lh.app_uid.uid, int(lh.limit))
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)	
}

func (client *Client) HandleLoadGroupOffline(lo *LoadGroupOffline) {
	messages := storage.LoadGroupOfflineMessage(lo.appid, lo.gid, lo.uid, int(lo.limit))
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)	
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", msg.cmd)
	switch msg.cmd {
	case MSG_LOAD_OFFLINE:
		client.HandleLoadOffline(msg.body.(*AppUserID))
	case MSG_SAVE_AND_ENQUEUE:
		client.HandleSaveAndEnqueue(msg.body.(*SAEMessage))
	case MSG_DEQUEUE:
		client.HandleDQMessage(msg.body.(*DQMessage))
	case MSG_LOAD_HISTORY:
		client.HandleLoadHistory((*LoadHistory)(msg.body.(*LoadHistory)))
	case MSG_SAVE_AND_ENQUEUE_GROUP:
		client.HandleSaveAndEnqueueGroup(msg.body.(*SAEMessage))
	case MSG_DEQUEUE_GROUP:
		client.HandleDQGroupMessage(msg.body.(*DQMessage))
	case MSG_LOAD_OFFLINE_GROUP:
		client.HandleLoadGroupOffline(msg.body.(*LoadGroupOffline))
	default:
		log.Warning("unknown msg:", msg.cmd)
	}
}

func (client *Client) Run() {
	go client.Read()
}

func (client *Client) read() *Message {
	return ReceiveMessage(client.conn)
}

func (client *Client) send(msg *Message) {
	SendMessage(client.conn, msg)
}

func (client *Client) close() {
	client.conn.Close()
}

func handle_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(*net.TCPConn), listen_addr string) {
	listen, err := net.Listen("tcp", listen_addr)
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	tcp_listener, ok := listen.(*net.TCPListener)
	if !ok {
		fmt.Println("listen error")
		return
	}

	for {
		client, err := tcp_listener.AcceptTCP()
		if err != nil {
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handle_client, config.listen)
}

func handle_sync_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config = read_storage_cfg(flag.Args()[0])
	log.Infof("listen:%s storage root:%s sync listen:%s master address:%s\n", 
		config.listen, config.storage_root, config.sync_listen, config.master_address)
	storage = NewStorage(config.storage_root)
	
	master = NewMaster()
	master.Start()
	if len(config.master_address) > 0 {
		slaver := NewSlaver(config.master_address)
		slaver.Start()
	}

	group_manager = NewGroupManager()
	group_manager.Start()

	go ListenSyncClient()
	ListenClient()
}
