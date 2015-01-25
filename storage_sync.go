package main

import "net"
import "sync"
import "time"
import log "github.com/golang/glog"


type SyncClient struct {
	conn      *net.TCPConn
	ewt       chan *EMessage
}

func NewSyncClient(conn *net.TCPConn) *SyncClient {
	c := new(SyncClient)
	c.conn = conn
	c.ewt = make(chan *EMessage, 10)
	return c
}

func (client *SyncClient) RunLoop() {
	seq := 0
	msg := ReceiveMessage(client.conn)
	if msg == nil {
		return
	}
	if msg.cmd != MSG_SYNC_BEGIN {
		return
	}

	cursor := msg.body.(*SyncCursor)
	log.Info("cursor msgid:", cursor.msgid)
	c := storage.LoadSyncMessagesInBackground(cursor.msgid)
	
	for emsg := range(c) {
		msg := &Message{cmd:MSG_SYNC_MESSAGE, body:emsg}
		seq = seq + 1
		msg.seq = seq
		SendMessage(client.conn, msg)
	}

	master.AddClient(client)
	defer master.RemoveClient(client)

	for {
		emsg := <- client.ewt
		if emsg == nil {
			log.Warning("chan closed")
			break
		}

		msg := &Message{cmd:MSG_SYNC_MESSAGE, body:emsg}
		seq = seq + 1
		msg.seq = seq
		err := SendMessage(client.conn, msg)
		if err != nil {
			break
		}
	}
}

func (client *SyncClient) Run() {
	go client.RunLoop()
}

type Master struct {
	ewt       chan *EMessage

	mutex     sync.Mutex
	clients   map[*SyncClient]struct{}
}

func NewMaster() *Master {
	master := new(Master)
	master.clients = make(map[*SyncClient]struct{})
	master.ewt = make(chan *EMessage, 10)
	return master
}

func (master *Master) AddClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.clients[client] = struct{}{}
}

func (master *Master) RemoveClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	delete(master.clients, client)
}

func (master *Master) CloneClientSet() map[*SyncClient]struct{} {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	clone := make(map[*SyncClient]struct{})
	for k, v := range(master.clients) {
		clone[k] = v
	}
	return clone
}

func (master *Master) Run() {
	for {
		emsg := <- master.ewt
		clients := master.CloneClientSet()
		for c := range(clients) {
			c.ewt <- emsg
		}
	}
}

func (master *Master) Start() {
	go master.Run()
}

type Slaver struct {
	addr string
}

func NewSlaver(addr string) *Slaver {
	s := new(Slaver)
	s.addr = addr
	return s
}

func (slaver *Slaver) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	seq := 0

	msgid := storage.NextMessageID()
	cursor := &SyncCursor{msgid}
	log.Info("cursor msgid:", msgid)

	msg := &Message{cmd:MSG_SYNC_BEGIN, body:cursor}
	seq += 1
	msg.seq = seq
	SendMessage(conn, msg)

	for {
		msg := ReceiveMessage(conn)
		if msg == nil {
			return
		}
		if msg.cmd == MSG_SYNC_MESSAGE {
			emsg := msg.body.(*EMessage)
			storage.SaveSyncMessage(emsg)
		} else {
			log.Error("unknown message cmd:", Command(msg.cmd))
		}
	}
}

func (slaver *Slaver) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", slaver.addr)
		if err != nil {
			log.Info("connect master server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("slaver sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("slaver connected with master")
		nsleep = 100
		slaver.RunOnce(tconn)
	}
}

func (slaver *Slaver) Start() {
	go slaver.Run()
}
