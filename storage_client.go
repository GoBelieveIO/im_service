package main

import "errors"
import "net"
import "bytes"
import "time"
import "sync"
import "container/list"
import "encoding/binary"
import log "github.com/golang/glog"

type StorageConn struct {
	conn net.Conn
	e    bool
}

func NewStorageConn() *StorageConn {
	c := new(StorageConn)
	return c
}

func (client *StorageConn) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		client.e = true
		return err
	}
	client.conn = conn
	return nil
}

func (client *StorageConn) Close() {
	if client.conn != nil {
		client.conn.Close()
	}
}

func (client *StorageConn) SaveAndEnqueueMessage(sae *SAEMessage) (int64, error) {
	msg := &Message{cmd:MSG_SAVE_AND_ENQUEUE, body:sae}
	SendMessage(client.conn, msg)
	r := ReceiveMessage(client.conn)
	if r == nil {
		client.e = true
		return 0, errors.New("error connection")
	}
	if r.cmd != MSG_RESULT {
		return 0, errors.New("error cmd")
	}
	result := r.body.(*MessageResult)
	if result.status != 0 {
		return 0, errors.New("error status")
	}
	if len(result.content) != 8 {
		return 0, errors.New("error content length")
	}

	var msgid int64
	buffer := bytes.NewBuffer(result.content)
	binary.Read(buffer, binary.BigEndian, &msgid)
	return msgid, nil
}

func (client *StorageConn) DequeueMessage(dq *DQMessage) error {
	msg := &Message{cmd:MSG_DEQUEUE, body:(*OfflineMessage)(dq)}
	SendMessage(client.conn, msg)
	r := ReceiveMessage(client.conn)
	if r == nil {
		client.e = true
		return errors.New("error connection")
	}
	if r.cmd != MSG_RESULT {
		return errors.New("error cmd")
	}
	result := r.body.(*MessageResult)
	if result.status != 0 {
		return errors.New("error status")
	}
	return nil
}

func (client *StorageConn) ReadEMessage(buf []byte) *EMessage {
	if len(buf) < 8 {
		return nil
	}
	emsg := &EMessage{}
	buffer := bytes.NewBuffer(buf)
	binary.Read(buffer, binary.BigEndian, &emsg.msgid)
	emsg.msg = ReceiveMessage(buffer)
	if emsg.msg == nil {
		return nil
	}
	return emsg
}

func (client *StorageConn) LoadOfflineMessage(appid int64, uid int64) ([]*EMessage, error) {
	id := &AppUserID{appid:appid, uid:uid}
	msg := &Message{cmd:MSG_LOAD_OFFLINE, body:id}
	SendMessage(client.conn, msg)
	r := ReceiveMessage(client.conn)
	if r == nil {
		client.e = true
		return nil, errors.New("error connection")
	}
	if r.cmd != MSG_RESULT {
		return nil, errors.New("error cmd")
	}
	result := r.body.(*MessageResult)
	if result.status != 0 {
		return nil, errors.New("error status")
	}

	buffer := bytes.NewBuffer(result.content)
	if buffer.Len() < 2 {
		return nil, errors.New("error length")
	}

	var count int16
	binary.Read(buffer, binary.BigEndian, &count)
	
	messages := make([]*EMessage, count)
	for i := 0; i < int(count); i++ {
		var size int16
		err := binary.Read(buffer, binary.BigEndian, &size)
		if err != nil {
			return nil, err
		}
		if buffer.Len() < int(size) {
			return nil, errors.New("error size")
		}
		msg_buf := make([]byte, size)
		buffer.Read(msg_buf)
		emsg := client.ReadEMessage(msg_buf)
		messages[i] = emsg
	}
	return messages, nil
}

var nowFunc = time.Now // for testing

type idleConn struct {
	c *StorageConn
	t time.Time
}

type StorageConnPool struct {

	Dial           func()(*StorageConn, error)

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// mu protects fields defined below.
	mu     sync.Mutex
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
	
	sem  chan int
}

func NewStorageConnPool(max_idle int, max_active int, 
	idle_timeout time.Duration, 
	dial func()(*StorageConn, error)) *StorageConnPool {
	if max_idle > max_active {
		return nil
	}
	pool := new(StorageConnPool)
	pool.MaxIdle = max_idle
	pool.MaxActive = max_active
	pool.IdleTimeout = idle_timeout
	pool.Dial = dial

	pool.sem = make(chan int, max_active)
	for i := 0; i < max_active; i++ {
		pool.sem <- 0
	}
	return pool
}

func (p *StorageConnPool) Get() (*StorageConn, error) {
	<- p.sem

	p.mu.Lock()
	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(nowFunc()) {
				break
			}
			p.idle.Remove(e)
			p.active -= 1
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	// Get idle connection.
	for i, n := 0, p.idle.Len(); i < n; i++ {
		e := p.idle.Front()
		if e == nil {
			break
		}
		ic := e.Value.(idleConn)
		p.idle.Remove(e)
		p.mu.Unlock()
		return ic.c, nil
	}

	if p.MaxActive > 0 && p.active >= p.MaxActive {
		log.Error("storage pool exhausted")
		p.sem <- 0
		return nil, errors.New("exhausted")
	}

	// No idle connection, create new.
	dial := p.Dial
	p.active += 1
	p.mu.Unlock()
	c, err := dial()
	if err != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		c = nil
		p.sem <- 0
	}
	return c, err
}

func (p *StorageConnPool) Release(c *StorageConn) {
	defer func() {
		p.sem <- 0
	}()

	if !c.e {
		p.mu.Lock()
		p.idle.PushFront(idleConn{t: nowFunc(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
		p.mu.Unlock()
	}
	if c != nil {
		p.mu.Lock()
		p.active -= 1
		p.mu.Unlock()
		c.Close()
	}
}
