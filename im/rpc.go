
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
import "net/rpc"
import "context"
import "time"
import "github.com/jackc/puddle"
import log "github.com/sirupsen/logrus"
import "github.com/GoBelieveIO/im_service/storage"

const MAX_STORAGE_RPC_POOL_SIZE = 100


type RPCStorage struct {
	rpc_pools []*puddle.Pool
	group_rpc_pools []*puddle.Pool	
}


func NewRPCPool(addr string) *puddle.Pool {
	newStorageClient := func(ctx context.Context) (interface{}, error) {
		client, err := rpc.DialHTTP("tcp", addr)
		return client, err
	}

	freeStorageClient := func(value interface{}) {
		value.(*rpc.Client).Close()
	}
	
	pool := puddle.NewPool(newStorageClient, freeStorageClient, MAX_STORAGE_RPC_POOL_SIZE)
	return pool
}

func NewRPCStorage(storage_rpc_addrs  []string, group_storage_rpc_addrs []string) *RPCStorage {
	r := &RPCStorage{}

	rpc_pools := make([]*puddle.Pool, 0, len(storage_rpc_addrs))
	for _, addr := range(storage_rpc_addrs) {
		pool := NewRPCPool(addr)
		rpc_pools = append(rpc_pools, pool)
	}

	var group_rpc_pools []*puddle.Pool
	if len(group_storage_rpc_addrs) > 0 {
		group_rpc_pools = make([]*puddle.Pool, 0, len(group_storage_rpc_addrs)) 
		for _, addr := range(group_storage_rpc_addrs) {
			pool := NewRPCPool(addr)			
			group_rpc_pools = append(group_rpc_pools, pool)
		}
	} else {
		group_rpc_pools = rpc_pools
	}

	r.rpc_pools = rpc_pools
	r.group_rpc_pools = group_rpc_pools
	
	return r
}


func (rpc_s *RPCStorage) SyncMessage(appid int64, uid int64, device_id int64, last_msgid int64) (*storage.PeerHistoryMessage, error) {
	dc, err := rpc_s.GetStorageRPCClient(uid)

	if err != nil {
		return nil , err
	}
	defer dc.Release()

	s := &storage.SyncHistory{
		AppID:appid, 
		Uid:uid, 
		DeviceID:device_id, 
		LastMsgID:last_msgid,
	}

	var resp storage.PeerHistoryMessage	
	err = dc.Value().(*rpc.Client).Call("RPCStorage.SyncMessage", s, &resp)
	if err != nil {
		log.Warning("sync message err:", err)
		return nil, err
	}

	return &resp, nil
}

func (rpc_s *RPCStorage) SyncGroupMessage(appid int64, uid int64, device_id int64, group_id int64, last_msgid int64, ts int32) (*storage.GroupHistoryMessage, error) {
	dc, err := rpc_s.GetGroupStorageRPCClient(group_id)

	if err != nil {
		return nil , err
	}
	defer dc.Release()

	
	s := &storage.SyncGroupHistory{
		AppID:appid, 
		Uid:uid, 
		DeviceID:device_id, 
		GroupID:group_id, 
		LastMsgID:last_msgid,
		Timestamp:ts,
	}

	var resp storage.GroupHistoryMessage
	err = dc.Value().(*rpc.Client).Call("RPCStorage.SyncGroupMessage", s, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (rpc_s *RPCStorage) SaveGroupMessage(appid int64, gid int64, device_id int64, msg *Message) (int64, int64, error) {
	dc, err := rpc_s.GetGroupStorageRPCClient(gid)

	if err != nil {
		log.Warning("save group message err:", err)
		return 0, 0, err
	}
	defer dc.Release()
	
	gm := &storage.GroupMessage{
		AppID:appid,
		GroupID:gid,
		DeviceID:device_id,
		Cmd:int32(msg.cmd),
		Raw:msg.ToData(),
	}

	var resp storage.HistoryMessageID
	err = dc.Value().(*rpc.Client).Call("RPCStorage.SaveGroupMessage", gm, &resp)
	if err != nil {
		log.Warning("save group message err:", err)
		return 0, 0, err
	}

	msgid := resp.MsgID
	prev_msgid := resp.PrevMsgID
	log.Infof("save group message:%d %d %d\n", appid, gid, msgid)
	return msgid, prev_msgid, nil
}

func (rpc_s *RPCStorage) SavePeerGroupMessage(appid int64, members []int64, device_id int64, m *Message) ([]*storage.HistoryMessageID, error) {

	if len(members) == 0 {
		return nil, nil
	}
	
	dc, err := rpc_s.GetStorageRPCClient(members[0])
	if err != nil {
		return nil, nil
	}
	defer dc.Release()
	
	pm := &storage.PeerGroupMessage{
		AppID:appid,
		Members:members,
		DeviceID:device_id,
		Cmd:int32(m.cmd),
		Raw:m.ToData(),
	}

	var resp storage.GroupHistoryMessageID

	err = dc.Value().(*rpc.Client).Call("RPCStorage.SavePeerGroupMessage", pm, &resp)
	if err != nil {
		log.Error("save peer group message err:", err)
		return nil, err
	}

	r := resp.MessageIDs
	log.Infof("save peer group message:%d %v %d %v\n", appid, members, device_id, r)
	return r, nil
}

func (rpc_s *RPCStorage) SaveMessage(appid int64, uid int64, device_id int64, m *Message) (int64, int64, error) {
	dc, err := rpc_s.GetStorageRPCClient(uid)
	if err != nil {
		return 0, 0, err
	}
	defer dc.Release()
	
	pm := &storage.PeerMessage{
		AppID:appid,
		Uid:uid,
		DeviceID:device_id,
		Cmd:int32(m.cmd),
		Raw:m.ToData(),
	}

	var resp storage.HistoryMessageID
	err = dc.Value().(*rpc.Client).Call("RPCStorage.SavePeerMessage", pm, &resp)
	if err != nil {
		log.Error("save peer message err:", err)
		return 0, 0, err
	}

	msgid := resp.MsgID
	prev_msgid := resp.PrevMsgID
	log.Infof("save peer message:%d %d %d %d", appid, uid, device_id, msgid)
	return msgid, prev_msgid, nil
}


func (rpc_s *RPCStorage) GetLatestMessage(appid int64, uid int64, limit int32) ([]*storage.HistoryMessage, error) {
	dc, err := rpc_s.GetStorageRPCClient(uid)
	if err != nil {
		return nil, err
	}
	defer dc.Release()
	
	s := &storage.HistoryRequest{
		AppID:appid, 
		Uid:uid, 
		Limit:int32(limit),
	}

	var resp storage.LatestMessage
	err = dc.Value().(*rpc.Client).Call("RPCStorage.GetLatestMessage", s, &resp)
	if err != nil {
		return nil, err
	}

	return resp.Messages, nil
}

//获取是否接收到新消息,只会返回0/1
func (rpc_s *RPCStorage) GetNewCount(appid int64, uid int64, last_msgid int64) (int64, error) {
	dc, err := rpc_s.GetStorageRPCClient(uid)
	if err != nil {
		return 0, err
	}
	defer dc.Release()
	
	var count int64
	sync_key := storage.SyncHistory{AppID:appid, Uid:uid, LastMsgID:last_msgid}	
	err = dc.Value().(*rpc.Client).Call("RPCStorage.GetNewCount", sync_key, &count)

	if err != nil {
		return 0, err
	}
	return count, nil
}


//个人消息／普通群消息／客服消息
func (rpc_s *RPCStorage) GetStorageRPCClient(uid int64) (*puddle.Resource, error) {
	if uid < 0 {
		uid = -uid
	}
	index := uid%int64(len(rpc_s.rpc_pools))
	pool := rpc_s.rpc_pools[index]
	return rpc_s.AcquireResource(pool)
}


func (rpc_s *RPCStorage) GetStorageRPCIndex(uid int64) int64 {
	if uid < 0 {
		uid = -uid
	}
	index := uid%int64(len(rpc_s.rpc_pools))
	return index
}


//超级群消息
func (rpc_s *RPCStorage) GetGroupStorageRPCClient(group_id int64) (*puddle.Resource, error) {
	if group_id < 0 {
		group_id = -group_id
	}
	index := group_id%int64(len(rpc_s.group_rpc_pools))
	pool := rpc_s.group_rpc_pools[index]

	return rpc_s.AcquireResource(pool)
}


func (rpc_s *RPCStorage) AcquireResource(pool *puddle.Pool) (*puddle.Resource, error) {
	var result *puddle.Resource
	var e error
	for {
		res, err := pool.Acquire(context.Background())

		if err == nil {
			idle_duration := res.IdleDuration()

			//check rpc.client expired or alive
			if idle_duration >= time.Minute*30 {
				log.Info("rpc client expired, destroy rpc client")
				res.Destroy()
				continue
			} else if idle_duration > time.Minute*12 {
				var p int
				err = res.Value().(*rpc.Client).Call("RPCStorage.Ping", 1, &p)
				if err != nil {
					log.Info("rpc client ping error, destroy rpc client")
					res.Destroy()
					continue
				}
			}
		}
		result = res
		e = err
		break
	}

	return result, e
}
