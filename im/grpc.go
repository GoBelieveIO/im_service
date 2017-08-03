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


import "time"
import "net"
import log "github.com/golang/glog"
import "google.golang.org/grpc"
import pb "github.com/GoBelieveIO/im_service/rpc"
import "golang.org/x/net/context"


//grpc
type RPCServer struct {}

func (s *RPCServer) PostGroupNotification(ctx context.Context, n *pb.GroupNotification) (*pb.Reply, error){
	members := NewIntSet()
	for _, m := range n.Members {
		members.Add(m)
	}

	group := group_manager.FindGroup(n.GroupId)
	if group != nil {
		ms := group.Members()
		for m, _ := range ms {
			members.Add(m)
		}
	}

	if len(members) == 0 {
		return &pb.Reply{1}, nil
	}

	SendGroupNotification(n.Appid, n.GroupId, n.Content, members)

	log.Info("post group notification success:", members)
	return &pb.Reply{0}, nil
}

func (s *RPCServer) PostPeerMessage(ctx context.Context, p *pb.PeerMessage) (*pb.Reply, error) {
	im := &IMMessage{}
	im.sender = p.Sender
	im.receiver = p.Receiver
	im.msgid = 0
	im.timestamp = int32(time.Now().Unix())
	im.content = p.Content

	SendIMMessage(im, p.Appid)
	log.Info("post peer message success")
	return &pb.Reply{0}, nil
}


func (s *RPCServer) PostGroupMessage(ctx context.Context, p *pb.GroupMessage) (*pb.Reply, error) {
	im := &IMMessage{}
	im.sender = p.Sender
	im.receiver = p.GroupId
	im.msgid = 0
	im.timestamp = int32(time.Now().Unix())
	im.content = p.Content

	SendGroupIMMessage(im, p.Appid)
	log.Info("post group message success")
	return &pb.Reply{0}, nil
}


func (s *RPCServer) PostSystemMessage(ctx context.Context, sm *pb.SystemMessage) (*pb.Reply, error) {
	sys := &SystemMessage{string(sm.Content)}
	msg := &Message{cmd:MSG_SYSTEM, body:sys}

	msgid, err := SaveMessage(sm.Appid, sm.Uid, 0, msg)
	if err != nil {
		return &pb.Reply{1}, nil
	}

	//推送通知
	PushMessage(sm.Appid, sm.Uid, msg)

	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(sm.Appid, sm.Uid, notify)

	log.Info("post system message success")

	return &pb.Reply{0}, nil
}



func (s *RPCServer) PostRealTimeMessage(ctx context.Context, rm *pb.RealTimeMessage) (*pb.Reply, error) {

	rt := &RTMessage{}
	rt.sender = rm.Sender
	rt.receiver = rm.Receiver
	rt.content = string(rm.Content)

	msg := &Message{cmd:MSG_RT, body:rt}
	SendAppMessage(rm.Appid, rm.Receiver, msg)
	log.Info("post realtime message success")
	return &pb.Reply{0}, nil
}


func (s *RPCServer) PostRoomMessage(ctx context.Context, rm *pb.RoomMessage) (*pb.Reply, error) {

	room_im := &RoomMessage{new(RTMessage)}
	room_im.sender = rm.Uid
	room_im.receiver = rm.RoomId
	room_im.content = string(rm.Content)

	msg := &Message{cmd:MSG_ROOM_IM, body:room_im}
	route := app_route.FindOrAddRoute(rm.Appid)
	clients := route.FindRoomClientSet(rm.RoomId)
	for c, _ := range(clients) {
		c.wt <- msg
	}

	amsg := &AppMessage{appid:rm.Appid, receiver:rm.RoomId, msg:msg}
	channel := GetRoomChannel(rm.RoomId)
	channel.PublishRoom(amsg)

	log.Info("post room message success")
	return &pb.Reply{0}, nil
}


func (s *RPCServer) PostCustomerMessage(ctx context.Context, cm *pb.CustomerMessage) (*pb.Reply, error) {

	c := &CustomerMessage{}
	c.customer_appid = cm.CustomerAppid
	c.customer_id = cm.CustomerId
	c.store_id = cm.StoreId
	c.seller_id = cm.SellerId
	c.content = cm.Content
	c.timestamp = int32(time.Now().Unix())

	m := &Message{cmd:MSG_CUSTOMER, body:c}


	msgid, err := SaveMessage(config.kefu_appid, cm.SellerId, 0, m)
 	if err != nil {
		log.Warning("save message error:", err)
		return &pb.Reply{1}, nil
	}
	msgid2, err := SaveMessage(cm.CustomerAppid, cm.CustomerId, 0, m)
 	if err != nil {
		log.Warning("save message error:", err)
		return &pb.Reply{1}, nil
	}
	
	PushMessage(config.kefu_appid, cm.SellerId, m)
	
	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(config.kefu_appid, cm.SellerId, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	SendAppMessage(cm.CustomerAppid, cm.CustomerId, notify)

	log.Info("post customer message success")
	return &pb.Reply{0}, nil
}


func (s *RPCServer) GetNewCount(ctx context.Context, nc *pb.NewCountRequest) (*pb.NewCount, error) {
	appid := nc.Appid
	uid := nc.Uid

	last_id := GetSyncKey(appid, uid)
	sync_key := SyncHistory{AppID:appid, Uid:uid, LastMsgID:last_id}
	
	dc := GetStorageRPCClient(uid)

	resp, err := dc.Call("GetNewCount", sync_key)

	if err != nil {
		log.Warning("get new count err:", err)
		return &pb.NewCount{0}, nil
	}
	count := resp.(int64)

	log.Infof("get offline appid:%d uid:%d count:%d", appid, uid, count)
	return &pb.NewCount{int32(count)}, nil
}



func (s *RPCServer) LoadLatestMessage(ctx context.Context, r *pb.LoadLatestRequest) (*pb.HistoryMessage, error) {
	appid := r.Appid
	uid := r.Uid
	limit := r.Limit

	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return &pb.HistoryMessage{}, nil
	}
	defer storage_pool.Release(storage)

	messages, err := storage.LoadLatestMessage(appid, uid, int32(limit))
	if err != nil {
		log.Error("load latest message err:", err)
		return &pb.HistoryMessage{}, nil
	}

	if len(messages) > 0 {
		//reverse
		size := len(messages)
		for i := 0; i < size/2; i++ {
			t := messages[i]
			messages[i] = messages[size-i-1]
			messages[size-i-1] = t
		}
	}

	hm := &pb.HistoryMessage{}
	hm.Messages = make([]*pb.Message, 0)

	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_IM {
			im := emsg.msg.body.(*IMMessage)
			p := &pb.PeerMessage{}
			p.Sender = im.sender
			p.Receiver = im.receiver
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_Peer{p}}
			hm.Messages = append(hm.Messages, m)
		} else if emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			p := &pb.GroupMessage{}
			p.Sender = im.sender
			p.GroupId = im.receiver
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_Group{p}}
			hm.Messages = append(hm.Messages, m)
		} else if emsg.msg.cmd == MSG_CUSTOMER {
			im := emsg.msg.body.(*CustomerMessage)
			p := &pb.CustomerMessage{}
			p.CustomerAppid = im.customer_appid
			p.CustomerId = im.customer_id
			p.StoreId = im.store_id
			p.SellerId = im.seller_id
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_Customer{p}}
			hm.Messages = append(hm.Messages, m)
		} else if emsg.msg.cmd == MSG_CUSTOMER_SUPPORT {
			im := emsg.msg.body.(*CustomerMessage)
			p := &pb.CustomerMessage{}
			p.CustomerAppid = im.customer_appid
			p.CustomerId = im.customer_id
			p.StoreId = im.store_id
			p.SellerId = im.seller_id
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_CustomerSupport{p}}
			hm.Messages = append(hm.Messages, m)
		}
	}

	log.Info("load latest message success")
	return hm, nil
}


func (s *RPCServer) LoadHistoryMessage(ctx context.Context, r *pb.LoadHistoryRequest) (*pb.HistoryMessage, error) {
	appid := r.Appid
	uid := r.Uid
	msgid := r.LastId
	
	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return &pb.HistoryMessage{}, nil
	}
	defer storage_pool.Release(storage)

	messages, err := storage.LoadHistoryMessage(appid, uid, msgid)
	if err != nil {
		log.Error("load latest message err:", err)
		return &pb.HistoryMessage{}, nil
	}

	if len(messages) > 0 {
		//reverse
		size := len(messages)
		for i := 0; i < size/2; i++ {
			t := messages[i]
			messages[i] = messages[size-i-1]
			messages[size-i-1] = t
		}
	}

	hm := &pb.HistoryMessage{}
	hm.Messages = make([]*pb.Message, 0)


	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_IM {
			im := emsg.msg.body.(*IMMessage)
			p := &pb.PeerMessage{}
			p.Sender = im.sender
			p.Receiver = im.receiver
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_Peer{p}}
			hm.Messages = append(hm.Messages, m)
		} else if emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			p := &pb.GroupMessage{}
			p.Sender = im.sender
			p.GroupId = im.receiver
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_Group{p}}
			hm.Messages = append(hm.Messages, m)
		} else if emsg.msg.cmd == MSG_CUSTOMER {
			im := emsg.msg.body.(*CustomerMessage)
			p := &pb.CustomerMessage{}
			p.CustomerAppid = im.customer_appid
			p.CustomerId = im.customer_id
			p.StoreId = im.store_id
			p.SellerId = im.seller_id
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_Customer{p}}
			hm.Messages = append(hm.Messages, m)
		} else if emsg.msg.cmd == MSG_CUSTOMER_SUPPORT {
			im := emsg.msg.body.(*CustomerMessage)
			p := &pb.CustomerMessage{}
			p.CustomerAppid = im.customer_appid
			p.CustomerId = im.customer_id
			p.StoreId = im.store_id
			p.SellerId = im.seller_id
			p.Content = im.content
			m := &pb.Message{emsg.msgid, im.timestamp, &pb.Message_CustomerSupport{p}}
			hm.Messages = append(hm.Messages, m)
		}
	}

	log.Info("load latest message success")
	return hm, nil
}



func StartRPCServer(addr string) {
	go func() {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterIMServer(s, &RPCServer{})
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
}
