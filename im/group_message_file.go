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

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

const HEADER_SIZE = 32
const MAGIC = 0x494d494d
const F_VERSION = 1 << 16 //1.0

type EPendingGroupMessage struct {
	msgid int64
	*PendingGroupMessage
}

// 普通群消息临时文件
type GroupMessageFile struct {
	root  string
	mutex sync.Mutex //写文件的锁
	file  *os.File

	cursor_file *os.File //不会被并发访问

	latest_msgid        int64 //最近保存的消息id
	latest_sended_msgid int64 //最近发送出去的消息id
}

func (storage *GroupMessageFile) openCursorFile() {
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

// 保存最近发出的消息id到cursor file
func (storage *GroupMessageFile) saveCursor() {
	_, err := storage.cursor_file.Seek(0, os.SEEK_SET)
	if err != nil {
		log.Fatal("write file")
	}

	err = binary.Write(storage.cursor_file, binary.BigEndian, storage.latest_sended_msgid)

	if err != nil {
		log.Fatal("write file")
	}
}

func (storage *GroupMessageFile) openWriteFile() {
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

func (storage *GroupMessageFile) readLatestMessageID() {
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

func (storage *GroupMessageFile) ReadMessage(file *os.File) *Message {
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

func (storage *GroupMessageFile) LoadMessage(msg_id int64) *Message {
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

func (storage *GroupMessageFile) ReadHeader(file *os.File) (magic int, version int) {
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

func (storage *GroupMessageFile) WriteHeader(file *os.File) {
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

// save without lock
func (storage *GroupMessageFile) saveMessage(msg *Message) int64 {
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

func (storage *GroupMessageFile) openReadFile() *os.File {
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

func (storage *GroupMessageFile) truncateFile() {
	err := storage.file.Truncate(HEADER_SIZE)
	if err != nil {
		log.Fatal("truncate err:", err)
	}

	storage.latest_msgid = 0
	storage.latest_sended_msgid = 0
	storage.saveCursor()
}

func (storage *GroupMessageFile) readPendingMessages(ctx context.Context) chan *EPendingGroupMessage {
	c := make(chan *EPendingGroupMessage)

	go func() {
		defer close(c)
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
			done := false
			select {
			case <-ctx.Done():
				done = true
			case c <- &EPendingGroupMessage{msgid, gm}:
			}
			if done {
				break
			}
		}
	}()
	return c
}
