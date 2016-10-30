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

func SendGroupNotification(appid int64, gid int64, 
	notification string, members IntSet) {

	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{notification}}

	for member := range(members) {
		_, err := SaveMessage(appid, member, 0, msg)
		if err != nil {
			break
		}
	}
}

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

func SendGroupIMMessage(im *IMMessage, appid int64) {
	m := &Message{cmd:MSG_GROUP_IM, version:DEFAULT_VERSION, body:im}
	group := group_manager.FindGroup(im.receiver)
	if group == nil {
		log.Warning("can't find group:", im.receiver)
		return
	}
	if group.super {
		_, err := SaveGroupMessage(appid, im.receiver, 0, m)
		if err != nil {
			return
		}
	} else {
		members := group.Members()
		for member := range members {
			_, err := SaveMessage(appid, member, 0, m)
			if err != nil {
				continue
			}
		}
	}
	atomic.AddInt64(&server_summary.in_message_count, 1)
}

func SendIMMessage(im *IMMessage, appid int64) {
	m := &Message{cmd: MSG_IM, version:DEFAULT_VERSION, body: im}
	_, err := SaveMessage(appid, im.receiver, 0, m)
	if err != nil {
		return
	}

	//保存到发送者自己的消息队列
	SaveMessage(appid, im.sender, 0, m)
	atomic.AddInt64(&server_summary.in_message_count, 1)
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

	var did int64
	device_id := m.Get("device_id")
	platform_id, _ := strconv.ParseInt(m.Get("platform_id"), 10, 64)
	if len(device_id) > 0 && (platform_id == 1 || platform_id == 2) {
		did, err = GetDeviceID(device_id, int(platform_id))
		if err != nil {
			log.Error("get device id err:", err)
			WriteHttpError(500, "server internal error", w)
			return
		}
	}
	
	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}
	defer storage_pool.Release(storage)


	count, err := storage.GetOfflineCount(appid, uid, did)
	if err != nil {
		log.Error("get offline count err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}
	log.Infof("get offline %d %d %d count:%d", appid, uid, did, count)
	obj := make(map[string]interface{})
	obj["count"] = count
	WriteHttpObj(obj, w)
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

	_, err = SaveMessage(appid, uid, 0, msg)
	if err != nil {
		WriteHttpError(500, "internal server error", w)
	} else {
		w.WriteHeader(200)
	}
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

	store, err := customer_service.GetStore(store_id)
	if err != nil {
		log.Warning("get store err:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}

	mode := store.mode
	if (mode == CS_MODE_BROADCAST) {
		cs := &CustomerMessage{}
		cs.customer_appid = customer_appid
		cs.customer_id = customer_id
		cs.store_id = store_id
		cs.seller_id = seller_id
		cs.content = content
		cs.timestamp = int32(time.Now().Unix())

		m := &Message{cmd:MSG_CUSTOMER, body:cs}

		group := group_manager.FindGroup(store.group_id)
		if group == nil {
			log.Warning("can't find group:", store.group_id)
			WriteHttpError(500, "internal server error", w)
			return
		}

		members := group.Members()
		for member := range members {
			_, err := SaveMessage(config.kefu_appid, member, 0, m)
			if err != nil {
				log.Error("save message err:", err)
				WriteHttpError(500, "internal server error", w)
				return
			}
		}
		_, err := SaveMessage(cs.customer_appid, cs.customer_id, 0, m)

		if err != nil {
			log.Error("save message err:", err)
			WriteHttpError(500, "internal server error", w)
			return
		}

		obj := make(map[string]interface{})
		obj["seller_id"] = 0
		WriteHttpObj(obj, w)
		return
	} else if (mode == CS_MODE_ONLINE) {
		if seller_id > 0 {
			is_on := customer_service.IsOnline(store_id, seller_id)
			if !is_on {
				seller_id = customer_service.GetOnlineSellerID(store_id)
				if seller_id == 0 {
					WriteHttpError(400, "no seller online", w)
					return
				}
			}
		} else {
			seller_id = customer_service.GetLastSellerID(customer_appid, customer_id, store_id)
			if seller_id != 0 {
				is_on := customer_service.IsOnline(store_id, seller_id)
				if !is_on {
					seller_id = customer_service.GetOnlineSellerID(store_id)
				}
			} else {
				seller_id = customer_service.GetOnlineSellerID(store_id)
			}
			if seller_id == 0 {
				WriteHttpError(400, "no seller online", w)
				return
			}
		}
	} else if (mode == CS_MODE_FIX) {
		if seller_id == 0 {
			seller_id = customer_service.GetLastSellerID(customer_appid, customer_id, store_id)
			if seller_id == 0 {
				seller_id = customer_service.GetSellerID(store_id)
				if seller_id == 0 {
					WriteHttpError(400, "no seller in store", w)
					return
				}
				customer_service.SetLastSellerID(customer_appid, customer_id, store_id, seller_id)
			}
		}

		is_exist := customer_service.IsExist(store_id, seller_id)
		if !is_exist {
			seller_id = customer_service.GetSellerID(store_id)
			if seller_id == 0 {
				WriteHttpError(400, "no seller in store", w)
				return
			}
			customer_service.SetLastSellerID(customer_appid, customer_id, store_id, seller_id)
		}
	} else if (mode == CS_MODE_ORDER) {
		if seller_id == 0 {
			seller_id = customer_service.GetLastSellerID(customer_appid, customer_id, store_id)
			if seller_id == 0 {
				seller_id = customer_service.GetOrderSellerID(store_id)
				if seller_id == 0 {
					WriteHttpError(400, "no seller in store", w)
					return
				}
				customer_service.SetLastSellerID(customer_appid, customer_id, store_id, seller_id)
			}
		}

		is_exist := customer_service.IsExist(store_id, seller_id)
		if !is_exist {
			seller_id = customer_service.GetOrderSellerID(store_id)
			if seller_id == 0 {
				WriteHttpError(400, "no seller in store", w)
				return
			}
			customer_service.SetLastSellerID(customer_appid, customer_id, store_id, seller_id)
		}
	}


	//fix and online mode
	cs := &CustomerMessage{}
	cs.customer_appid = customer_appid
	cs.customer_id = customer_id
	cs.store_id = store_id
	cs.seller_id = seller_id
	cs.content = content
	cs.timestamp = int32(time.Now().Unix())

	m := &Message{cmd:MSG_CUSTOMER, body:cs}

	SaveMessage(cs.customer_appid, cs.customer_id, 0, m)
	_, err = SaveMessage(config.kefu_appid, cs.seller_id, 0, m)

	if err != nil {
		WriteHttpError(500, "internal server error", w)
	} else {
		obj := make(map[string]interface{})
		obj["seller_id"] = seller_id
		WriteHttpObj(obj, w)
	}
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
	Send0Message(appid, receiver, msg)
	w.WriteHeader(200)
}


func InitMessageQueue(w http.ResponseWriter, req *http.Request) {
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

	uid, err := obj.Get("uid").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	platform_id, err := obj.Get("platform_id").Int()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	device_id, err := obj.Get("device_id").String()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	if platform_id == PLATFORM_WEB || len(device_id) == 0 {
		WriteHttpError(400, "invalid platform/device id", w)
		return
	}
	
	did, err := GetDeviceID(device_id, platform_id)

	if err != nil {
		log.Info("error:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}

	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		WriteHttpError(500, "server internal error", w)
	}
	defer storage_pool.Release(storage)

	err = storage.InitQueue(appid, uid, did)

	if err != nil {
		log.Error("init queue err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}

	groups := group_manager.FindUserGroups(appid, uid)
	for _, group := range groups {
		if !group.super {
			continue
		}

		storage_pool = GetGroupStorageConnPool(group.gid)
		storage, err = storage_pool.Get()
		if err != nil {
			log.Error("connect storage err:", err)
			WriteHttpError(500, "server internal error", w)
			return
		}
		defer storage_pool.Release(storage)

		err = storage.InitGroupQueue(appid, group.gid, uid, did)
		if err != nil {
			log.Error("init group queue err:", err)
			WriteHttpError(500, "server internal error", w)
			return
		}
	}
	w.WriteHeader(200)
}


func DequeueMessage(w http.ResponseWriter, req *http.Request) {
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

	uid, err := obj.Get("uid").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	msgid, err := obj.Get("msgid").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	dq := &DQMessage{msgid:msgid, appid:appid, receiver:uid, device_id:0}

	storage_pool := GetStorageConnPool(uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		WriteHttpError(500, "server internal error", w)
	}
	defer storage_pool.Release(storage)

	err = storage.DequeueMessage(dq)

	if err != nil {
		log.Error("init queue err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}

	w.WriteHeader(200)
}
