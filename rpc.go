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
