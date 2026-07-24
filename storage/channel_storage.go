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

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	. "github.com/GoBelieveIO/im_service/protocol"
)

const BATCH_SIZE = 1000
const CHANNEL_INDEX_FILE_NAME = "channel_index.v1"

const CHANNLE_TYPE_PEER = 1
const CHANNEL_TYPE_GROUP = 2

type ChannelID struct {
	appid        int64
	channel_id   int64
	channel_type uint8
}

type ChannelIndex struct {
	last_msgid    int64
	last_id       int64
	last_peer_id  int64
	last_batch_id int64
	last_seq_id   int64
}

// 在取离线消息时，可以对群组消息和点对点消息分别获取，
// 这样可以做到分别控制点对点消息和群组消息读取量，避免单次读取超量的离线消息
type ChannelStorage struct {
	*StorageFile

	//消息索引全部放在内存中,在程序退出时,再全部保存到文件中，
	//如果索引文件不存在或上次保存失败，则在程序启动的时候，从消息DB中重建索引，这需要遍历每一条消息
	message_index map[ChannelID]*ChannelIndex //记录每个用户最近的消息ID
}

func NewChannelStorage(f *StorageFile) *ChannelStorage {
	storage := &ChannelStorage{StorageFile: f}
	storage.message_index = make(map[ChannelID]*ChannelIndex)
	return storage
}

func (storage *ChannelStorage) SaveChannelMessage(appid int64, channel_id int64, channel_type uint8, device_id int64, msg *Message) (int64, int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	msgid := storage.saveMessage(msg)

	user_index := storage.getChannelIndex(appid, channel_id, channel_type)
	last_id := user_index.last_id
	last_peer_id := user_index.last_peer_id
	last_batch_id := user_index.last_batch_id
	last_seq_id := user_index.last_seq_id

	off := &OfflineMessageV5{}
	off.appid = appid
	off.receiver = channel_id
	off.channel_type = channel_type
	off.msgid = msgid
	off.device_id = device_id
	off.seq_id = last_seq_id + 1
	off.prev_msgid = last_id
	off.prev_peer_msgid = last_peer_id
	off.prev_batch_msgid = last_batch_id

	var flag int
	if storage.isGroupMessage(msg) {
		flag = MESSAGE_FLAG_GROUP
	}
	m := &Message{Cmd: MSG_OFFLINE_V4, Flag: flag, Body: off}
	last_id = storage.saveMessage(m)

	if !storage.isGroupMessage(msg) {
		last_peer_id = last_id
	}

	last_seq_id += 1
	if last_seq_id%BATCH_SIZE == 0 {
		last_batch_id = last_id
	}

	ui := &ChannelIndex{msgid, last_id, last_peer_id, last_batch_id, last_seq_id}
	storage.setChannelIndex(appid, channel_id, channel_type, ui)
	return msgid, user_index.last_msgid
}

func (storage *ChannelStorage) SavePeerGroupMessage(appid int64, members []int64, channel_type uint8, device_id int64, msg *Message) []int64 {
	r := make([]int64, 0, len(members)*2)
	for _, uid := range members {
		msgid, prev_msgid := storage.SaveChannelMessage(appid, uid, channel_type, device_id, msg)
		r = append(r, msgid)
		r = append(r, prev_msgid)
	}
	return r
}

func (storage *ChannelStorage) GetChannelIndex(appid int64, channel_id int64, channel_type uint8) *ChannelIndex {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	return storage.getChannelIndex(appid, channel_id, channel_type)
}

func (storage *ChannelStorage) getChannelIndex(appid int64, channel_id int64, channel_type uint8) *ChannelIndex {
	id := ChannelID{appid, channel_id, channel_type}
	if ui, ok := storage.message_index[id]; ok {
		return ui
	}
	return &ChannelIndex{}
}

func (storage *ChannelStorage) setChannelIndex(appid int64, channel_id int64, channel_type uint8, ui *ChannelIndex) {
	id := ChannelID{appid, channel_id, channel_type}
	storage.message_index[id] = ui

	if ui.last_id > storage.last_id {
		storage.last_id = ui.last_id
	}
}

// 获取最近离线消息ID
func (storage *ChannelStorage) getLastMessageID(appid int64, channel_id int64, channel_type uint8) (int64, int64) {
	id := ChannelID{appid, channel_id, channel_type}
	if ui, ok := storage.message_index[id]; ok {
		return ui.last_id, ui.last_peer_id
	}
	return 0, 0
}

// lock
func (storage *ChannelStorage) GetLastMessageID(appid int64, channel_id int64, channel_type uint8) (int64, int64) {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()
	return storage.getLastMessageID(appid, channel_id, channel_type)
}

// 加载离线消息
// limit 单次加载限制
// hard_limit 总量限制
// hard_limit == 0 or ( hard_limit >= BATCH_SIZE and hard_limit > limit)
func (storage *ChannelStorage) LoadHistoryMessagesV3(appid int64, channel_id int64, channel_type uint8, sync_msgid int64, ts int32, limit int, hard_limit int) ([]*EMessage, int64, bool) {
	var last_msgid int64
	var last_offline_msgid int64

	msg_index := storage.getChannelIndex(appid, channel_id, channel_type)

	var last_batch_id int64
	if msg_index != nil {
		last_batch_id = msg_index.last_batch_id
	}

	hard_batch_count := hard_limit / BATCH_SIZE

	batch_count := limit / BATCH_SIZE
	if batch_count == 0 {
		//as if default limit==BATCH_SIZE
		batch_count = 1
	}

	batch_ids := make([]int64, 0, 10)

	//搜索和sync_msgid最近的batch_id
	for {
		if last_batch_id <= sync_msgid {
			break
		}
		msg := storage.LoadMessage(last_batch_id)
		if msg == nil {
			break
		}

		off, ok := msg.Body.(*OfflineMessage)
		if !ok {
			log.Warning("invalid message cmd:", msg.Cmd)
			break
		}

		if off.msgid <= sync_msgid {
			break
		}

		batch_ids = append(batch_ids, last_batch_id)
		last_batch_id = off.prev_batch_msgid

		if hard_batch_count > 0 && len(batch_ids) >= hard_batch_count {
			break
		}
	}

	//只获取点对点消息
	var is_peer bool
	var last_id int64
	if hard_batch_count > 0 && len(batch_ids) >= hard_batch_count && hard_batch_count >= batch_count {
		index := len(batch_ids) - batch_count
		last_id = batch_ids[index]
		//离线消息总数超过hard_limit时，首先加载数量不超过limit的点对点消息
		//尽量保证点对点消息的投递
		is_peer = true
	} else if len(batch_ids) >= batch_count {
		index := len(batch_ids) - batch_count
		last_id = batch_ids[index]
	} else if msg_index != nil {
		last_id = msg_index.last_id
	}

	messages := make([]*EMessage, 0, 10)
	for {
		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}

		var off *OfflineMessage
		if msg.Cmd == MSG_OFFLINE_V5 {
			off_v5, ok := msg.Body.(*OfflineMessageV5)
			if ok {
				off = &off_v5.OfflineMessage
			}
		} else {
			off_v4, ok := msg.Body.(*OfflineMessage)
			if ok {
				off = off_v4
			}
		}
		if off == nil {
			log.Warning("invalid message cmd:", msg.Cmd)
			break
		}

		if last_msgid == 0 {
			last_msgid = off.msgid
			last_offline_msgid = last_id
		}
		if off.msgid <= sync_msgid {
			break
		}

		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}

		if sync_msgid == 0 && msg.Cmd == MSG_GROUP_IM && ts > 0 {
			//不取入群之前的消息
			im := msg.Body.(MessageTime)
			if im.Timestamp() < ts {
				break
			}
		}

		if msg.Cmd == MSG_GROUP_IM ||
			msg.Cmd == MSG_GROUP_NOTIFICATION ||
			msg.Cmd == MSG_IM ||
			msg.Cmd == MSG_CUSTOMER_V2 ||
			msg.Cmd == MSG_SYSTEM {

			emsg := &EMessage{MsgId: off.msgid, DeviceId: off.device_id, Msg: msg}
			messages = append(messages, emsg)

			if limit > 0 && len(messages) >= limit {
				break
			}
		}

		if is_peer {
			last_id = off.prev_peer_msgid
		} else {
			last_id = off.prev_msgid
		}
	}

	if len(messages) > 1000 {
		log.Warningf("appid:%d channel id:%d channel type:%d sync msgid:%d history message overflow:%d",
			appid, channel_id, channel_type, sync_msgid, len(messages))
	}

	var hasMore bool
	if msg_index != nil && last_offline_msgid > 0 &&
		last_offline_msgid < msg_index.last_id {
		hasMore = true
	}

	log.Infof("appid:%d channel id:%d channel type:%d sync msgid:%d history message loaded:%d %d, last_id:%d, %d last batch id:%d last seq id:%d has more:%t only peer:%t",
		appid, channel_id, channel_type, sync_msgid, len(messages), last_msgid,
		last_offline_msgid, msg_index.last_id, msg_index.last_batch_id,
		msg_index.last_seq_id, hasMore, is_peer)

	return messages, last_msgid, hasMore
}

func (storage *ChannelStorage) LoadLatestMessages(appid int64, channel_id int64, channel_type uint8, limit int) []*EMessage {
	last_id, _ := storage.GetLastMessageID(appid, channel_id, channel_type)
	messages := make([]*EMessage, 0, 10)
	for {
		if last_id == 0 {
			break
		}

		msg := storage.LoadMessage(last_id)
		if msg == nil {
			break
		}

		off, ok := msg.Body.(*OfflineMessage)
		if !ok {
			log.Warning("invalid message cmd:", msg.Cmd)
			break
		}

		msg = storage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.Cmd == MSG_GROUP_IM ||
			msg.Cmd == MSG_GROUP_NOTIFICATION ||
			msg.Cmd == MSG_IM ||
			msg.Cmd == MSG_CUSTOMER_V2 {
			emsg := &EMessage{MsgId: off.msgid, DeviceId: off.device_id, Msg: msg}
			messages = append(messages, emsg)
			if len(messages) >= limit {
				break
			}
		}
		last_id = off.prev_msgid
	}
	return messages
}

func (client *ChannelStorage) isGroupMessage(msg *Message) bool {
	return msg.Cmd == MSG_GROUP_IM || msg.Flag&MESSAGE_FLAG_GROUP != 0
}

func (storage *ChannelStorage) GetNewCount(appid int64, channel_id int64, channel_type uint8, last_received_id int64) int {
	storage.mutex.Lock()
	defer storage.mutex.Unlock()

	user_index := storage.getChannelIndex(appid, channel_id, channel_type)
	last_seq_id := user_index.last_seq_id

	if last_received_id == 0 {
		return int(last_seq_id)
	}

	blockNO := storage.getBlockNO(last_received_id)
	blockOffSet := storage.getBlockOffset(last_received_id)

	file := storage.getFile(blockNO)
	if file == nil {
		log.Warning("can not get file", blockNO)
		return 0
	}

	_, err := file.Seek(int64(blockOffSet), os.SEEK_SET)
	if err != nil {
		log.Warning("seek file err:", err)
		return 0
	}

	m := storage.ReadMessage(file)
	if m == nil {
		log.Warning("read message failure")
		return 0
	}

	off_m := storage.ReadMessage(file)
	if off_m == nil {
		file = storage.getFile(blockNO + 1)
		if file == nil {
			log.Warning("can not get file", blockNO+1)
			return 0
		}

		_, err := file.Seek(HEADER_SIZE, os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			return 0
		}

		off_m = storage.ReadMessage(file)
		if off_m == nil {
			log.Warning("read message failure")
			return 0
		}
	}

	off, ok := off_m.Body.(*OfflineMessage)
	if !ok {
		log.Warning("invalid message cmd:", off_m.Cmd)
		return 0
	}

	if off.msgid != last_received_id {
		log.Warning("invalid offline message:", off.msgid, last_received_id)
		return 0
	}

	if off.appid != appid || off.receiver != channel_id {
		log.Warningf("invalid appid %d != %d or channel_id %d != %d", off.appid, appid, off.receiver, channel_id)
		return 0
	}
	if last_seq_id < off.seq_id {
		return 0
	}

	return int(last_seq_id - off.seq_id)
}

func (storage *ChannelStorage) createChannelIndex() {
	log.Info("create message index begin:", time.Now().UnixNano())

	for i := 0; i <= storage.block_NO; i++ {
		file := storage.openReadFile(i)
		if file == nil {
			//历史消息被删除
			continue
		}

		_, err := file.Seek(HEADER_SIZE, os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			file.Close()
			break
		}
		for {
			msgid, err := file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := storage.ReadMessage(file)
			if msg == nil {
				break
			}

			block_NO := i
			msgid = int64(block_NO)*BLOCK_SIZE + msgid
			storage.execMessage(msg, msgid)
		}

		file.Close()
	}
	log.Info("create message index end:", storage.last_id, time.Now().UnixNano())
}

func (storage *ChannelStorage) repairChannelIndex() {
	log.Info("repair message index begin:", storage.last_id, time.Now().UnixNano())

	first := storage.getBlockNO(storage.last_id)
	off := storage.getBlockOffset(storage.last_id)

	for i := first; i <= storage.block_NO; i++ {
		file := storage.openReadFile(i)
		if file == nil {
			//历史消息被删除
			continue
		}

		offset := HEADER_SIZE
		if i == first {
			offset = off
		}

		_, err := file.Seek(int64(offset), os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			file.Close()
			break
		}
		for {
			msgid, err := file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := storage.ReadMessage(file)
			if msg == nil {
				break
			}
			block_NO := i
			msgid = int64(block_NO)*BLOCK_SIZE + msgid
			if msgid == storage.last_id {
				continue
			}

			storage.execMessage(msg, msgid)
		}

		file.Close()
	}
	log.Info("repair message index end:", storage.last_id, time.Now().UnixNano())
}

func (storage *ChannelStorage) readChannelIndex() bool {
	path := fmt.Sprintf("%s/%s", storage.root, CHANNEL_INDEX_FILE_NAME)
	log.Info("read message index path:", path)
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("open file:", err)
		}
		return false
	}
	defer file.Close()
	const INDEX_SIZE = 56
	data := make([]byte, INDEX_SIZE*1000)

	for {
		n, err := file.Read(data)
		if err != nil {
			if err != io.EOF {
				log.Fatal("read err:", err)
			}
			break
		}
		n = n - n%INDEX_SIZE
		buffer := bytes.NewBuffer(data[:n])
		for i := 0; i < n/INDEX_SIZE; i++ {
			id := ChannelID{}
			var last_msgid int64
			var last_id int64
			var peer_msgid int64
			var batch_id int64
			var seq_id int64
			binary.Read(buffer, binary.BigEndian, &id.appid)
			binary.Read(buffer, binary.BigEndian, &id.channel_id)
			binary.Read(buffer, binary.BigEndian, &id.channel_type)
			binary.Read(buffer, binary.BigEndian, &last_msgid)
			binary.Read(buffer, binary.BigEndian, &last_id)
			binary.Read(buffer, binary.BigEndian, &peer_msgid)
			binary.Read(buffer, binary.BigEndian, &batch_id)
			binary.Read(buffer, binary.BigEndian, &seq_id)
			ui := &ChannelIndex{last_msgid, last_id, peer_msgid, batch_id, seq_id}
			storage.setChannelIndex(id.appid, id.channel_id, id.channel_type, ui)
		}
	}
	return true
}

func (storage *ChannelStorage) removeChannelIndex() {
	path := fmt.Sprintf("%s/%s", storage.root, CHANNEL_INDEX_FILE_NAME)
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("remove file:", err)
		}
	}
}

func (storage *ChannelStorage) cloneChannelIndex() map[ChannelID]*ChannelIndex {
	message_index := make(map[ChannelID]*ChannelIndex)
	for k, v := range storage.message_index {
		message_index[k] = v
	}
	return message_index
}

// appid uid msgid = 24字节
func (storage *ChannelStorage) saveChannelIndex(message_index map[ChannelID]*ChannelIndex) {
	path := fmt.Sprintf("%s/channel_index_t", storage.root)
	log.Info("write channel message index path:", path)
	begin := time.Now().UnixNano()
	log.Info("flush channel index begin:", begin)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	defer file.Close()

	buffer := new(bytes.Buffer)
	index := 0
	for id, value := range message_index {
		binary.Write(buffer, binary.BigEndian, id.appid)
		binary.Write(buffer, binary.BigEndian, id.channel_id)
		binary.Write(buffer, binary.BigEndian, id.channel_type)
		binary.Write(buffer, binary.BigEndian, value.last_msgid)
		binary.Write(buffer, binary.BigEndian, value.last_id)
		binary.Write(buffer, binary.BigEndian, value.last_peer_id)
		binary.Write(buffer, binary.BigEndian, value.last_batch_id)
		binary.Write(buffer, binary.BigEndian, value.last_seq_id)
		index += 1
		//batch write to file
		if index%1000 == 0 {
			buf := buffer.Bytes()
			n, err := file.Write(buf)
			if err != nil {
				log.Fatal("write file:", err)
			}
			if n != len(buf) {
				log.Fatal("can't write file:", len(buf), n)
			}

			buffer.Reset()
		}
	}

	buf := buffer.Bytes()
	n, err := file.Write(buf)
	if err != nil {
		log.Fatal("write file:", err)
	}
	if n != len(buf) {
		log.Fatal("can't write file:", len(buf), n)
	}
	err = file.Sync()
	if err != nil {
		log.Info("sync file err:", err)
	}

	path2 := fmt.Sprintf("%s/%s", storage.root, CHANNEL_INDEX_FILE_NAME)
	err = os.Rename(path, path2)
	if err != nil {
		log.Fatal("rename peer index file err:", err)
	}

	end := time.Now().UnixNano()
	log.Info("flush peer index end:", end, " used:", end-begin)
}

func (storage *ChannelStorage) execMessage(msg *Message, msgid int64) {
	if msg.Cmd == MSG_OFFLINE_V5 {
		off := msg.Body.(*OfflineMessageV5)
		last_peer_id := msgid

		index := storage.getChannelIndex(off.appid, off.receiver, off.channel_type)
		if (msg.Flag & MESSAGE_FLAG_GROUP) != 0 {
			last_peer_id = index.last_peer_id
		}
		last_batch_id := index.last_batch_id
		last_seq_id := index.last_seq_id + 1
		if last_seq_id%BATCH_SIZE == 0 {
			last_batch_id = msgid
		}

		ui := &ChannelIndex{off.msgid, msgid, last_peer_id, last_batch_id, last_seq_id}
		storage.setChannelIndex(off.appid, off.receiver, off.channel_type, ui)
	}

	//兼容历史数据
	if msg.Cmd == MSG_OFFLINE_V4 {
		off := msg.Body.(*OfflineMessage)
		last_peer_id := msgid

		index := storage.getChannelIndex(off.appid, off.receiver, CHANNLE_TYPE_PEER)
		if (msg.Flag & MESSAGE_FLAG_GROUP) != 0 {
			last_peer_id = index.last_peer_id
		}
		last_batch_id := index.last_batch_id
		last_seq_id := index.last_seq_id + 1
		if last_seq_id%BATCH_SIZE == 0 {
			last_batch_id = msgid
		}

		ui := &ChannelIndex{off.msgid, msgid, last_peer_id, last_batch_id, last_seq_id}
		storage.setChannelIndex(off.appid, off.receiver, CHANNLE_TYPE_PEER, ui)
	}

	//兼容历史数据
	if msg.Cmd == MSG_GROUP_OFFLINE {
		off := msg.Body.(*OfflineMessage)
		index := storage.getChannelIndex(off.appid, off.receiver, CHANNEL_TYPE_GROUP)
		last_id := msgid
		last_batch_id := index.last_batch_id
		last_seq_id := index.last_seq_id + 1
		if last_seq_id%BATCH_SIZE == 0 {
			last_batch_id = msgid
		}

		gi := &ChannelIndex{off.msgid, msgid, last_id, last_batch_id, last_seq_id}
		storage.setChannelIndex(off.appid, off.receiver, CHANNEL_TYPE_GROUP, gi)
	}

}
