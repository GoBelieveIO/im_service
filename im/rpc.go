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
			PushMessage(appid, member, m)

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
	PushMessage(appid, im.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{sync_key:msgid}}
	SendAppMessage(appid, im.receiver, notify)

	//发送同步的通知消息
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{sync_key:msgid2}}
	SendAppMessage(appid, im.sender, notify)

	atomic.AddInt64(&server_summary.in_message_count, 1)
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
	log.Infof("appid:%d uid:%d limit:%d", appid, uid, limit)

	rpc := GetStorageRPCClient(uid)

	s := &HistoryRequest{
		AppID:appid, 
		Uid:uid, 
		Limit:int32(limit),
	}

	resp, err := rpc.Call("GetLatestMessage", s)
	if err != nil {
		log.Warning("get latest message err:", err)
		WriteHttpError(400, "internal error", w)		
		return
	}

	hm := resp.([]*HistoryMessage)
	messages := make([]*EMessage, 0)
	for _, msg := range(hm) {
		m := &Message{cmd:int(msg.Cmd), version:DEFAULT_VERSION}
		m.FromData(msg.Raw)
		e := &EMessage{msgid:msg.MsgID, device_id:msg.DeviceID, msg:m}
		messages = append(messages, e)
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


	rpc := GetStorageRPCClient(uid)

	s := &SyncHistory{
		AppID:appid, 
		Uid:uid, 
		DeviceID:0, 
		LastMsgID:msgid,
	}

	resp, err := rpc.Call("SyncMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}

	ph := resp.(*PeerHistoryMessage)
	messages := ph.Messages

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
		msg := &Message{cmd:int(emsg.Cmd), version:DEFAULT_VERSION}
		msg.FromData(emsg.Raw)
		if msg.cmd == MSG_IM || 
			msg.cmd == MSG_GROUP_IM {
			im := msg.body.(*IMMessage)
			
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["command"] = emsg.Cmd
			obj["id"] = emsg.MsgID
			msg_list = append(msg_list, obj)

		} else if msg.cmd == MSG_CUSTOMER || 
			msg.cmd == MSG_CUSTOMER_SUPPORT {
			im := msg.body.(*CustomerMessage)
			
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["customer_appid"] = im.customer_appid
			obj["customer_id"] = im.customer_id
			obj["store_id"] = im.store_id
			obj["seller_id"] = im.seller_id
			obj["command"] = emsg.Cmd
			obj["id"] = emsg.MsgID
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

	last_id, err := strconv.ParseInt(m.Get("sync_key"), 10, 64)
	if err != nil || last_id == 0 {
		last_id = GetSyncKey(appid, uid)		
	}
	sync_key := SyncHistory{AppID:appid, Uid:uid, LastMsgID:last_id}
	
	dc := GetStorageRPCClient(uid)

	resp, err := dc.Call("GetNewCount", sync_key)

	if err != nil {
		log.Warning("get new count err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}
	count := resp.(int64)

	log.Infof("get offline appid:%d uid:%d sync_key:%d count:%d", appid, uid, last_id, count)
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


func SendCustomerSupportMessage(w http.ResponseWriter, req *http.Request) {
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

	m := &Message{cmd:MSG_CUSTOMER_SUPPORT, body:cm}


	msgid, err := SaveMessage(cm.customer_appid, cm.customer_id, 0, m)
 	if err != nil {
		log.Warning("save message error:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}
	
	msgid2, err := SaveMessage(config.kefu_appid, cm.seller_id, 0, m)
 	if err != nil {
		log.Warning("save message error:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}
	
	PushMessage(cm.customer_appid, cm.customer_id, m)
	
	//发送给自己的其它登录点	
	notify := &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid2}}
	SendAppMessage(config.kefu_appid, cm.seller_id, notify)

	//发送同步的通知消息
	notify = &Message{cmd:MSG_SYNC_NOTIFY, body:&SyncKey{msgid}}
	SendAppMessage(cm.customer_appid, cm.customer_id, notify)

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
