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

	storage := &Storage{f, ps, gs}

	r1 := storage.readPeerIndex()
	r2 := storage.readGroupIndex()
	storage.last_saved_id = storage.last_id
	
	if r1 {
		storage.repairPeerIndex()		
	}
	if r2 {
		storage.repairGroupIndex()
	}

	if !r1 {
		storage.createPeerIndex()
	}
	if !r2 {
		storage.createGroupIndex()
	}
	
	log.Infof("last id:%d last saved id:%d", storage.last_id, storage.last_saved_id)
	storage.FlushIndex()
	return storage
}

func (storage *Storage) NextMessageID() int64 {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	offset, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}
	return offset + int64(storage.block_NO)*BLOCK_SIZE
}

func (storage *Storage) execMessage(msg *Message, msgid int64) {
	storage.PeerStorage.execMessage(msg, msgid)
	storage.GroupStorage.execMessage(msg, msgid)
}

func (storage *Storage) ExecMessage(msg *Message, msgid int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	storage.execMessage(msg, msgid)
}

func (storage *Storage) SaveSyncMessageBatch(mb *MessageBatch) error {
	id := mb.first_id
	//all message come from one block
	for _, m := range mb.msgs {
		emsg := &EMessage{id, 0, m}
		buffer := new(bytes.Buffer)
		storage.WriteMessage(buffer, m)
		id += int64(buffer.Len())
		storage.SaveSyncMessage(emsg)
	}

	log.Infof("save batch sync message first id:%d last id:%d\n", mb.first_id, mb.last_id)
	return nil
}

func (storage *Storage) SaveSyncMessage(emsg *EMessage) error {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	n := storage.getBlockNO(emsg.msgid)
	o := storage.getBlockOffset(emsg.msgid)

	if n < storage.block_NO || (n - storage.block_NO) > 1 {
		log.Warning("skip msg:", emsg.msgid)
		return nil
	}

	if (n - storage.block_NO) == 1 {
		storage.file.Close()
		storage.openWriteFile(n)
	}

	offset, err := storage.file.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatalln(err)
	}

	if o < int(offset) {
		log.Warning("skip msg:", emsg.msgid)
		return nil
	} else if o > int(offset) {
		log.Warning("write padding:", o - int(offset))
		padding := make([]byte, o - int(offset))
		_, err = storage.file.Write(padding)
		if err != nil {
			log.Fatal("file write:", err)
		}
	}

	storage.WriteMessage(storage.file, emsg.msg)
	storage.execMessage(emsg.msg, emsg.msgid)
	log.Info("save sync message:", emsg.msgid)
	return nil
}

func (storage *Storage) LoadSyncMessagesInBackground(cursor int64) chan *MessageBatch {
	c := make(chan *MessageBatch, 10)
	go func() {
		defer close(c)

		block_NO := storage.getBlockNO(cursor)
		offset := storage.getBlockOffset(cursor)

		n := block_NO
		for {
			file := storage.openReadFile(n)
			if file == nil {
				break
			}

			if n == block_NO {
				file_size, err := file.Seek(0, os.SEEK_END)
				if err != nil {
					log.Fatal("seek file err:", err)
					return
				}

				if file_size < int64(offset) {
					break
				}

				_, err = file.Seek(int64(offset), os.SEEK_SET)
				if err != nil {
					log.Info("seek file err:", err)
					break
				}
			} else {
				file_size, err := file.Seek(0, os.SEEK_END)
				if err != nil {
					log.Fatal("seek file err:", err)
					return
				}

				if file_size < int64(offset) {
					break
				}

				_, err = file.Seek(HEADER_SIZE, os.SEEK_SET)
				if err != nil {
					log.Info("seek file err:", err)
					break
				}
			}

			const BATCH_COUNT = 5000
			batch := &MessageBatch{msgs:make([]*Message, 0, BATCH_COUNT)}
			for {
				position, err := file.Seek(0, os.SEEK_CUR)
				if err != nil {
					log.Info("seek file err:", err)
					break
				}
				msg := storage.ReadMessage(file)
				if msg == nil {
					break
				}
				msgid := storage.getMsgId(n, int(position))
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

			n++
		}
		
		
	}()
	return c
}

func (storage *Storage) SaveIndexFileAndExit() {
	storage.flushIndex()
	os.Exit(0)
}

func (storage *Storage) flushIndex() {
	storage.mutex.Lock()
	last_id := storage.last_id
	peer_index := storage.clonePeerIndex()
	group_index := storage.cloneGroupIndex()
	storage.mutex.Unlock()

	storage.savePeerIndex(peer_index)
	storage.saveGroupIndex(group_index)
	storage.last_saved_id = last_id	
}

func (storage *Storage) FlushIndex() {
	do_flush := false
	if storage.last_id - storage.last_saved_id > 2*BLOCK_SIZE {
		do_flush = true
	}
	if do_flush {
		storage.flushIndex()
	}
}
