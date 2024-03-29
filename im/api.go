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

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"io/ioutil"

	"github.com/GoBelieveIO/im_service/set"
	"github.com/bitly/go-simplejson"
	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
)

func SendGroupNotification(appid int64, gid int64,
	notification string, members set.IntSet, app *App, rpc_storage *RPCStorage) {

	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{notification}}

	for member := range members {
		msgid, _, err := rpc_storage.SaveMessage(appid, member, 0, msg)
		if err != nil {
			break
		}

		//发送同步的通知消息
		notify := &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncNotify{sync_key: msgid}}
		app.SendAnonymousMessage(appid, member, notify)
	}
}

func SendSuperGroupNotification(appid int64, gid int64,
	notification string, members set.IntSet, app *App, rpc_storage *RPCStorage) {

	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{notification}}

	msgid, _, err := rpc_storage.SaveGroupMessage(appid, gid, 0, msg)

	if err != nil {
		log.Errorf("save group message: %d err:%s", gid, err)
		return
	}

	for member := range members {
		//发送同步的通知消息
		notify := &Message{cmd: MSG_SYNC_GROUP_NOTIFY, body: &GroupSyncNotify{gid, msgid}}
		app.SendAnonymousMessage(appid, member, notify)
	}
}

func SendGroupIMMessage(im *IMMessage, appid int64, app *App, server_summary *ServerSummary, rpc_storage *RPCStorage) {
	m := &Message{cmd: MSG_GROUP_IM, version: DEFAULT_VERSION, body: im}
	loader := app.GetGroupLoader(im.receiver)
	group := loader.LoadGroup(im.receiver)
	if group == nil {
		log.Warning("can't find group:", im.receiver)
		return
	}
	if group.super {
		msgid, _, err := rpc_storage.SaveGroupMessage(appid, im.receiver, 0, m)
		if err != nil {
			return
		}

		//推送外部通知
		app.PushGroupMessage(appid, group, m)

		//发送同步的通知消息
		notify := &Message{cmd: MSG_SYNC_GROUP_NOTIFY, body: &GroupSyncNotify{group_id: im.receiver, sync_key: msgid}}
		app.SendAnonymousGroupMessage(appid, group, notify)
	} else {
		gm := &PendingGroupMessage{}
		gm.appid = appid
		gm.sender = im.sender
		gm.device_ID = 0
		gm.gid = im.receiver
		gm.timestamp = im.timestamp
		members := group.Members()
		gm.members = make([]int64, len(members))
		i := 0
		for uid := range members {
			gm.members[i] = uid
			i += 1
		}

		gm.content = im.content
		deliver := app.GetGroupMessageDeliver(group.gid)
		m := &Message{cmd: MSG_PENDING_GROUP_MESSAGE, body: gm}
		deliver.SaveMessage(m, nil)
	}
	atomic.AddInt64(&server_summary.in_message_count, 1)
}

func SendIMMessage(im *IMMessage, appid int64, app *App, server_summary *ServerSummary, rpc_storage *RPCStorage) {
	m := &Message{cmd: MSG_IM, version: DEFAULT_VERSION, body: im}
	msgid, _, err := rpc_storage.SaveMessage(appid, im.receiver, 0, m)
	if err != nil {
		return
	}

	//保存到发送者自己的消息队列
	msgid2, _, err := rpc_storage.SaveMessage(appid, im.sender, 0, m)
	if err != nil {
		return
	}

	//推送外部通知
	app.PushMessage(appid, im.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncNotify{sync_key: msgid}}
	app.SendAnonymousMessage(appid, im.receiver, notify)

	//发送同步的通知消息
	notify = &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncNotify{sync_key: msgid2}}
	app.SendAnonymousMessage(appid, im.sender, notify)

	atomic.AddInt64(&server_summary.in_message_count, 1)
}

// http
func PostGroupNotification(w http.ResponseWriter, req *http.Request, app *App, rpc_storage *RPCStorage) {
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

	is_super := false
	if super_json, ok := obj.CheckGet("super"); ok {
		is_super, err = super_json.Bool()
		if err != nil {
			log.Info("error:", err)
			WriteHttpError(400, "invalid json format", w)
			return
		}
	}

	members := set.NewIntSet()
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

	loader := app.GetGroupLoader(group_id)
	group := loader.LoadGroup(group_id)
	if group != nil {
		ms := group.Members()
		for m, _ := range ms {
			members.Add(m)
		}
		is_super = group.super
	}

	if len(members) == 0 {
		WriteHttpError(400, "group no member", w)
		return
	}
	if is_super {
		SendSuperGroupNotification(appid, group_id, notification, members, app, rpc_storage)
	} else {
		SendGroupNotification(appid, group_id, notification, members, app, rpc_storage)
	}

	log.Info("post group notification success")
	w.WriteHeader(200)
}

func PostPeerMessage(w http.ResponseWriter, req *http.Request, app *App, server_summary *ServerSummary, rpc_storage *RPCStorage) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}

	sender, err := strconv.ParseInt(m.Get("sender"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}

	receiver, err := strconv.ParseInt(m.Get("receiver"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}

	content := string(body)

	im := &IMMessage{}
	im.sender = sender
	im.receiver = receiver
	im.msgid = 0
	im.timestamp = int32(time.Now().Unix())
	im.content = content

	SendIMMessage(im, appid, app, server_summary, rpc_storage)

	w.WriteHeader(200)
	log.Info("post peer im message success")
}

func PostGroupMessage(w http.ResponseWriter, req *http.Request, app *App, server_summary *ServerSummary, rpc_storage *RPCStorage) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}

	sender, err := strconv.ParseInt(m.Get("sender"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}

	receiver, err := strconv.ParseInt(m.Get("receiver"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}

	content := string(body)

	im := &IMMessage{}
	im.sender = sender
	im.receiver = receiver
	im.msgid = 0
	im.timestamp = int32(time.Now().Unix())
	im.content = content

	SendGroupIMMessage(im, appid, app, server_summary, rpc_storage)

	w.WriteHeader(200)

	log.Info("post group im message success")
}

func LoadLatestMessage(w http.ResponseWriter, req *http.Request, rpc_storage *RPCStorage) {
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

	messages, err := rpc_storage.GetLatestMessage(appid, uid, int32(limit))
	if err != nil {
		log.Warning("get latest message err:", err)
		WriteHttpError(400, "internal error", w)
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

		msg := &Message{cmd: int(emsg.Cmd), version: DEFAULT_VERSION}
		msg.FromData(emsg.Raw)

		if msg.cmd == MSG_IM ||
			msg.cmd == MSG_GROUP_IM {
			im := msg.body.(*IMMessage)

			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["command"] = msg.cmd
			obj["id"] = emsg.MsgID
			msg_list = append(msg_list, obj)

		} else if msg.cmd == MSG_CUSTOMER_V2 {
			im := msg.body.(*CustomerMessageV2)
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["sender_appid"] = im.sender_appid
			obj["receiver_appid"] = im.receiver_appid
			obj["command"] = msg.cmd
			obj["id"] = emsg.MsgID

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

func LoadHistoryMessage(w http.ResponseWriter, req *http.Request, rpc_storage *RPCStorage) {
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

	ph, err := rpc_storage.SyncMessage(appid, uid, 0, msgid)
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
		msg := &Message{cmd: int(emsg.Cmd), version: DEFAULT_VERSION}
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
		} else if msg.cmd == MSG_CUSTOMER_V2 {
			im := msg.body.(*CustomerMessageV2)
			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["sender_appid"] = im.sender_appid
			obj["receiver_appid"] = im.receiver_appid
			obj["command"] = msg.cmd
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

func GetOfflineCount(w http.ResponseWriter, req *http.Request, redis_pool *redis.Pool, rpc_storage *RPCStorage) {
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
		last_id = GetSyncKey(redis_pool, appid, uid)
	}
	count, err := rpc_storage.GetNewCount(appid, uid, last_id)
	if err != nil {
		log.Warning("get new count err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}

	log.Infof("get offline appid:%d uid:%d sync_key:%d count:%d", appid, uid, last_id, count)
	obj := make(map[string]interface{})
	obj["count"] = count
	WriteHttpObj(obj, w)
}

func SendNotification(w http.ResponseWriter, req *http.Request, app *App) {
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
	msg := &Message{cmd: MSG_NOTIFICATION, body: sys}
	app.SendAnonymousMessage(appid, uid, msg)

	w.WriteHeader(200)
}

func SendSystemMessage(w http.ResponseWriter, req *http.Request, app *App, rpc_storage *RPCStorage) {
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

	content, err := obj.Get("content").String()
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

	receivers, err := obj.Get("receivers").Array()
	if err != nil {
		log.Info("get receivers error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}
	for i, _ := range receivers {
		_, err = obj.Get("receivers").GetIndex(i).Int64()
		if err != nil {
			log.Warning("receivers contains non int receiver:", err)
			WriteHttpError(400, "invalid json format", w)
			return
		}
	}
	for i, _ := range receivers {
		uid := obj.Get("receivers").GetIndex(i).MustInt64()
		sys := &SystemMessage{content}
		msg := &Message{cmd: MSG_SYSTEM, body: sys}

		msgid, _, err := rpc_storage.SaveMessage(appid, uid, 0, msg)
		if err != nil {
			WriteHttpError(500, "internal server error", w)
			return
		}

		//推送通知
		app.PushMessage(appid, uid, msg)

		//发送同步的通知消息
		notify := &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncNotify{sync_key: msgid}}
		app.SendAnonymousMessage(appid, uid, notify)
	}

	w.WriteHeader(200)
}

func SendRoomMessage(w http.ResponseWriter, req *http.Request, app *App) {
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

	msg := &Message{cmd: MSG_ROOM_IM, body: room_im}

	app.SendAnonymousRoomMessage(appid, room_id, msg)

	w.WriteHeader(200)
}

func SendCustomerMessage(w http.ResponseWriter, req *http.Request, app *App, rpc_storage *RPCStorage) {
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

	sender_appid, err := obj.Get("sender_appid").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	sender, err := obj.Get("sender").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	receiver_appid, err := obj.Get("receiver_appid").Int64()
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

	content, err := obj.Get("content").String()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	cm := &CustomerMessageV2{}
	cm.sender_appid = sender_appid
	cm.sender = sender
	cm.receiver_appid = receiver_appid
	cm.receiver = receiver
	cm.content = content
	cm.timestamp = int32(time.Now().Unix())

	m := &Message{cmd: MSG_CUSTOMER_V2, body: cm}

	msgid, _, err := rpc_storage.SaveMessage(receiver_appid, receiver, 0, m)
	if err != nil {
		log.Warning("save message error:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}
	msgid2, _, err := rpc_storage.SaveMessage(sender_appid, sender, 0, m)
	if err != nil {
		log.Warning("save message error:", err)
		WriteHttpError(500, "internal server error", w)
		return
	}

	app.PushMessage(receiver_appid, receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncNotify{sync_key: msgid}}
	app.SendAnonymousMessage(receiver_appid, receiver, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd: MSG_SYNC_NOTIFY, body: &SyncNotify{sync_key: msgid2}}
	app.SendAnonymousMessage(sender_appid, sender, notify)

	resp := make(map[string]interface{})
	WriteHttpObj(resp, w)
}

func SendRealtimeMessage(w http.ResponseWriter, req *http.Request, app *App) {
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

	msg := &Message{cmd: MSG_RT, body: rt}
	app.SendAnonymousMessage(appid, receiver, msg)
	w.WriteHeader(200)
}
