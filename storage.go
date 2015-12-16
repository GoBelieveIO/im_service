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

import log "github.com/golang/glog"

type Storage struct {
	*StorageFile
	*PeerStorage
	*GroupStorage
}

func NewStorage(root string) *Storage {
	f := NewStorageFile(root)
	ps := NewPeerStorage(f)
	gs := NewGroupStorage(f)
	return &Storage{f, ps, gs}
}

func (storage *Storage) FlushReceived() {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	storage.PeerStorage.FlushReceived()
	storage.GroupStorage.FlushReceived()
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
	storage.PeerStorage.ExecMessage(msg, msgid)
	storage.GroupStorage.ExecMessage(msg, msgid)
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
