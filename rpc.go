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


func GetStorageConnPool(uid int64) *StorageConnPool {
	index := uid%int64(len(storage_pools))
	return storage_pools[index]
}

func GetChannel(uid int64) *Channel{
	index := uid%int64(len(channels))
	return channels[index]
}


func SendGroupNotification(appid int64, gid int64, 
	notification string, members IntSet) {

	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{notification}}

	for member := range(members) {
		sae := &SAEMessage{}
		sae.msg = msg
		sae.receivers = make([]*AppUserID, 1)
		id := &AppUserID{appid:appid, uid:member}
		sae.receivers[0] = id

		storage_pool := GetStorageConnPool(member)

		storage, err := storage_pool.Get()
		if err != nil {
			log.Error("connect storage err:", err)
			return
		}
		defer storage_pool.Release(storage)

		msgid, err := storage.SaveAndEnqueueMessage(sae)
		if err != nil {
			log.Error("saveandequeue message err:", err)
			return
		}

		channel := GetChannel(member)
		amsg := &AppMessage{appid:appid, receiver:member, 
			msgid:msgid, msg:msg}
		channel.Publish(amsg)

		emsg := &EMessage{msgid:msgid, msg:msg}
		route := app_route.FindRoute(appid)
		if route == nil {
			continue
		}
		clients := route.FindClientSet(member)
		if clients != nil {
			for c, _ := range(clients) {
				c.ewt <- emsg
			}
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

func  SendEMessage(appid int64, uid int64, emsg *EMessage) bool {
	channel := GetChannel(uid)
	amsg := &AppMessage{appid:appid, receiver:uid, 
		msgid:emsg.msgid, msg:emsg.msg}
	channel.Publish(amsg)

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warning("can't find app route, msg cmd:", 
			Command(emsg.msg.cmd))
		return false
	}
	clients := route.FindClientSet(uid)
	if clients != nil || clients.Count() > 0 {
		for c, _ := range(clients) {
			c.ewt <- emsg
		}
		return true
	}
	return false
}

func SendIMMessage(im *IMMessage, appid int64) {
	m := &Message{cmd: MSG_IM, version:DEFAULT_VERSION, body: im}

	storage_pool := GetStorageConnPool(im.receiver)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	sae := &SAEMessage{}
	sae.msg = m
	sae.receivers = make([]*AppUserID, 1)
	sae.receivers[0] = &AppUserID{appid:appid, uid:im.receiver}

	msgid, err := storage.SaveAndEnqueueMessage(sae)
	if err != nil {
		log.Error("saveandequeue message err:", err)
		return
	}

	emsg := &EMessage{msgid:msgid, msg:m}
	SendEMessage(appid, im.receiver, emsg)

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d", im.sender, im.receiver)
}

func PostIMMessage(w http.ResponseWriter, req *http.Request) {
	log.Info("post im message")
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

	obj, err := simplejson.NewJson(body)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	receiver, err := obj.Get("receiver").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return		
	}

	id, err := obj.Get("msgid").Int64()
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
	im.msgid = int32(id)
	im.timestamp = int32(time.Now().Unix())
	im.content = content

	SendIMMessage(im, appid)
	log.Info("post im message success")
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

	msg_list := make([]map[string]interface{}, 0, len(messages))
	for _, emsg := range messages {
		if emsg.msg.cmd != MSG_IM && emsg.msg.cmd != MSG_GROUP_IM {
			continue
		}
		im := emsg.msg.body.(*IMMessage)
		
		obj := make(map[string]interface{})
		obj["content"] = im.content
		obj["timestamp"] = im.timestamp
		obj["sender"] = im.sender
		obj["receiver"] = im.receiver
		obj["is_group"] = bool(emsg.msg.cmd == MSG_GROUP_IM)
		msg_list = append(msg_list, obj)
	}

	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = msg_list
	b, _ := json.Marshal(obj)
	w.Write(b)
	log.Info("load latest message success")
}
