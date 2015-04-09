package main

import "net/http"
import "encoding/json"
import "os"
import "runtime/pprof"
import log "github.com/golang/glog"
import "io/ioutil"
import "github.com/bitly/go-simplejson"

type ServerSummary struct {
	nconnections      int64
	nclients          int64
	in_message_count  int64
	out_message_count int64
}

func NewServerSummary() *ServerSummary {
	s := new(ServerSummary)
	return s
}


func Summary(rw http.ResponseWriter, req *http.Request) {
	obj := make(map[string]interface{})
	obj["connection_count"] = server_summary.nconnections
	obj["client_count"] = server_summary.nclients
	obj["in_message_count"] = server_summary.in_message_count
	obj["out_message_count"] = server_summary.out_message_count

	res, err := json.Marshal(obj)
	if err != nil {
		log.Info("json marshal:", err)
		return
	}

	rw.Header().Add("Content-Type", "application/json")
	_, err = rw.Write(res)
	if err != nil {
		log.Info("write err:", err)
	}
	return
}

func Stack(rw http.ResponseWriter, req *http.Request) {
	pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	rw.WriteHeader(200)
}

func WriteHttpError(status int, err string, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	meta := make(map[string]interface{})
	meta["code"] = status
	meta["message"] = err
	obj["meta"] = meta
	b, _ := json.Marshal(obj)
	w.WriteHeader(status)
	w.Write(b)
}

func WriteHttpObj(data map[string]interface{}, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = data
	b, _ := json.Marshal(obj)
	w.Write(b)
}


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

		route := app_route.FindRoute(appid)
		if route == nil {
			continue
		}
		clients := route.FindClientSet(member)
		if clients != nil {
			for c, _ := range(clients) {
				c.wt <- msg
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

func StartHttpServer(addr string) {
	http.HandleFunc("/summary", Summary)
	http.HandleFunc("/stack", Stack)

	//rpc function
	http.HandleFunc("/post_group_notification", PostGroupNotification)

	HTTPService(addr, nil)
}
