package main
import "net/http"
import "encoding/json"
import "time"
import "net/url"
import "strconv"
import "sync/atomic"
import log "github.com/golang/glog"
import "io/ioutil"
import "github.com/bitly/go-simplejson"
import pb "github.com/GoBelieveIO/im_service/rpc"
import "golang.org/x/net/context"



func SendGroupNotification(appid int64, gid int64, 
	notification string, members IntSet) {

	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{notification}}

	for member := range(members) {
		msgid, err := SaveMessage(appid, member, 0, msg)
		if err != nil {
			break
		}

		//发送同步的通知消息
		notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
		SendAppMessage(appid, member, notify)
	}
}


func SendGroupIMMessage(im *IMMessage, appid int64) {
	m := &Message{cmd:MSG_GROUP_IM, version:DEFAULT_VERSION, body:im}
	group := group_manager.FindGroup(im.receiver)
	if group == nil {
		log.Warning("can't find group:", im.receiver)
		return
	}
	if group.super {
		msgid, err := SaveGroupMessage(appid, im.receiver, 0, m)
		if err != nil {
			return
		}

		//推送外部通知
		PushGroupMessage(appid, im.receiver, m)

		//发送同步的通知消息
		notify := &Message{cmd:MSG_SYNC_GROUP_NOTIFY, body:&GroupSyncKey{group_id:im.receiver, sync_key:msgid}}
		SendAppGroupMessage(appid, im.receiver, notify)

	} else {
		members := group.Members()
		for member := range members {
			msgid, err := SaveMessage(appid, member, 0, m)
			if err != nil {
				continue
			}

			//推送外部通知
			PushGroupMessage(appid, member, m)

			//发送同步的通知消息
			notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{sync_key:msgid}}
			SendAppMessage(appid, member, notify)
		}
	}
	atomic.AddInt64(&server_summary.in_message_count, 1)
}

func SendIMMessage(im *IMMessage, appid int64) {
	m := &Message{cmd: MSG_IM, version:DEFAULT_VERSION, body: im}
	msgid, err := SaveMessage(appid, im.receiver, 0, m)
	if err != nil {
		return
	}

	//保存到发送者自己的消息队列
	msgid2, err := SaveMessage(appid, im.sender, 0, m)
	if err != nil {
		return
	}
	
	//推送外部通知
	PushGroupMessage(appid, im.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{sync_key:msgid}}
	SendAppMessage(appid, im.receiver, notify)

	//发送同步的通知消息
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{sync_key:msgid2}}
	SendAppMessage(appid, im.sender, notify)

	atomic.AddInt64(&server_summary.in_message_count, 1)
}

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


//http
func PostGroupNotification(w http.ResponseWriter, req *http.Request) {
	log.Info("post group notification")
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	obj, err := simplejson.NewJson(body)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	appid, err := obj.Get("appid").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}
	group_id, err := obj.Get("group_id").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}

	notification, err := obj.Get("notification").String()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}

	members := NewIntSet()

	marray, err := obj.Get("members").Array()
	for _, m := range marray {
		if _, ok := m.(json.Number); ok {
			member, err := m.(json.Number).Int64()
			if err != nil {
				log.Info("error:", err)
				WriteHttpError(400, "invalid json format", w)
				return		
			}
			members.Add(member)
		}
	}

	group := group_manager.FindGroup(group_id)
	if group != nil {
		ms := group.Members()
		for m, _ := range ms {
			members.Add(m)
		}
	}

	if len(members) == 0 {
		WriteHttpError(400, "group no member", w)
		return
	}

	SendGroupNotification(appid, group_id, notification, members)

	log.Info("post group notification success:", members)
	w.WriteHeader(200)
}


func PostIMMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	sender, err := strconv.ParseInt(m.Get("sender"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	var is_group bool
	msg_type := m.Get("class")
	if msg_type == "group" {
		is_group = true
	} else if msg_type == "peer" {
		is_group = false
	} else {
		log.Info("invalid message class")
		WriteHttpError(400, "invalid message class", w)
		return
	}

	obj, err := simplejson.NewJson(body)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	sender2, err := obj.Get("sender").Int64()
	if err == nil && sender == 0 {
		sender = sender2
	}

	receiver, err := obj.Get("receiver").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}
	
	content, err := obj.Get("content").String()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}

	im := &IMMessage{}
	im.sender = sender
	im.receiver = receiver
	im.msgid = 0
	im.timestamp = int32(time.Now().Unix())
	im.content = content

	if is_group {
		SendGroupIMMessage(im, appid)
		log.Info("post group im message success")
 	} else {
		SendIMMessage(im, appid)
		log.Info("post peer im message success")
	}
	w.WriteHeader(200)
}

func LoadLatestMessage(w http.ResponseWriter, req *http.Request) {
	log.Info("load latest message")
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	limit, err := strconv.ParseInt(m.Get("limit"), 10, 32)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	
	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		WriteHttpError(400, "server internal error", w)
		return
	}
	defer storage_pool.Release(storage)

	messages, err := storage.LoadLatestMessage(appid, uid, int32(limit))
	if err != nil {
		WriteHttpError(400, "server internal error", w)
		return
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

	msg_list := make([]map[string]interface{}, 0, len(messages))
	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_IM || 
			emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["command"] = emsg.msg.cmd
			obj["id"] = emsg.msgid
			msg_list = append(msg_list, obj)
			
		} else if emsg.msg.cmd == MSG_CUSTOMER ||
			emsg.msg.cmd == MSG_CUSTOMER_SUPPORT {
			im := emsg.msg.body.(*CustomerMessage)
			
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["customer_appid"] = im.customer_appid
			obj["customer_id"] = im.customer_id
			obj["store_id"] = im.store_id
			obj["seller_id"] = im.seller_id
			obj["command"] = emsg.msg.cmd
			obj["id"] = emsg.msgid
			msg_list = append(msg_list, obj)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = msg_list
	b, _ := json.Marshal(obj)
	w.Write(b)
	log.Info("load latest message success")
}

func LoadHistoryMessage(w http.ResponseWriter, req *http.Request) {
	log.Info("load message")
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	msgid, err := strconv.ParseInt(m.Get("last_id"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	
	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		WriteHttpError(400, "server internal error", w)
		return
	}
	defer storage_pool.Release(storage)

	messages, err := storage.LoadHistoryMessage(appid, uid, msgid)
	if err != nil {
		WriteHttpError(400, "server internal error", w)
		return
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

	msg_list := make([]map[string]interface{}, 0, len(messages))
	for _, emsg := range messages {
		if emsg.msg.cmd == MSG_IM || 
			emsg.msg.cmd == MSG_GROUP_IM {
			im := emsg.msg.body.(*IMMessage)
			
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["command"] = emsg.msg.cmd
			obj["id"] = emsg.msgid
			msg_list = append(msg_list, obj)

		} else if emsg.msg.cmd == MSG_CUSTOMER || 
			emsg.msg.cmd == MSG_CUSTOMER_SUPPORT {
			im := emsg.msg.body.(*CustomerMessage)
			
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["customer_appid"] = im.customer_appid
			obj["customer_id"] = im.customer_id
			obj["store_id"] = im.store_id
			obj["seller_id"] = im.seller_id
			obj["command"] = emsg.msg.cmd
			obj["id"] = emsg.msgid
			msg_list = append(msg_list, obj)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = msg_list
	b, _ := json.Marshal(obj)
	w.Write(b)
	log.Info("load history message success")
}

func GetOfflineCount(w http.ResponseWriter, req *http.Request){
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	last_id := GetSyncKey(appid, uid)
	sync_key := SyncHistory{AppID:appid, Uid:uid, LastMsgID:last_id}
	
	dc := GetStorageRPCClient(uid)

	resp, err := dc.Call("GetNewCount", sync_key)

	if err != nil {
		log.Warning("get new count err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}
	count := resp.(int64)

	log.Infof("get offline appid:%d uid:%d count:%d", appid, uid, count)
	obj := make(map[string]interface{})
	obj["count"] = count
	WriteHttpObj(obj, w)
}

func SendNotification(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	sys := &SystemMessage{string(body)}
	msg := &Message{cmd:MSG_NOTIFICATION, body:sys}
	SendAppMessage(appid, uid, msg)
	
	w.WriteHeader(200)
}


func SendSystemMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	sys := &SystemMessage{string(body)}
	msg := &Message{cmd:MSG_SYSTEM, body:sys}

	msgid, err := SaveMessage(appid, uid, 0, msg)
	if err != nil {
		WriteHttpError(500, "internal server error", w)
		return
	}

	//推送通知
	PushMessage(appid, uid, msg)

	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(appid, uid, notify)
	
	w.WriteHeader(200)
}

func SendRoomMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	room_id, err := strconv.ParseInt(m.Get("room"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	room_im := &RoomMessage{new(RTMessage)}
	room_im.sender = uid
	room_im.receiver = room_id
	room_im.content = string(body)

	msg := &Message{cmd:MSG_ROOM_IM, body:room_im}
	route := app_route.FindOrAddRoute(appid)
	clients := route.FindRoomClientSet(room_id)
	for c, _ := range(clients) {
		c.wt <- msg
	}

	amsg := &AppMessage{appid:appid, receiver:room_id, msg:msg}
	channel := GetRoomChannel(room_id)
	channel.PublishRoom(amsg)

	w.WriteHeader(200)
}

func SendCustomerMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	obj, err := simplejson.NewJson(body)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	customer_appid, err := obj.Get("customer_appid").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}

	customer_id, err := obj.Get("customer_id").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	store_id, err := obj.Get("store_id").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	seller_id, err := obj.Get("seller_id").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}
	
	content, err := obj.Get("content").String()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}


	cm := &CustomerMessage{}
	cm.customer_appid = customer_appid
	cm.customer_id = customer_id
	cm.store_id = store_id
	cm.seller_id = seller_id
	cm.content = content
	cm.timestamp = int32(time.Now().Unix())

	m := &Message{cmd:MSG_CUSTOMER, body:cm}


	msgid, err := SaveMessage(config.kefu_appid, cm.seller_id, 0, m)
 	if err != nil {
		log.Warning("save message error:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}
	msgid2, err := SaveMessage(cm.customer_appid, cm.customer_id, 0, m)
 	if err != nil {
		log.Warning("save message error:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}
	
	PushMessage(config.kefu_appid, cm.seller_id, m)
	
	
	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(config.kefu_appid, cm.seller_id, notify)


	//发送给自己的其它登录点
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	SendAppMessage(cm.customer_appid, cm.customer_id, notify)

	resp := make(map[string]interface{})
	resp["seller_id"] = seller_id
	WriteHttpObj(resp, w)
}


func SendRealtimeMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	sender, err := strconv.ParseInt(m.Get("sender"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	receiver, err := strconv.ParseInt(m.Get("receiver"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	rt := &RTMessage{}
	rt.sender = sender
	rt.receiver = receiver
	rt.content = string(body)

	msg := &Message{cmd:MSG_RT, body:rt}
	SendAppMessage(appid, receiver, msg)
	w.WriteHeader(200)
}


func InitMessageQueue(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
}


func DequeueMessage(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
}
