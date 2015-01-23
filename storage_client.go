package main

import "errors"
import "net"
import "bytes"
import "encoding/binary"

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
	msg := &Message{cmd:MSG_DEQUEUE, body:dq}
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


