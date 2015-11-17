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

import "fmt"
import "net/http"
import "strconv"
import "io/ioutil"
import "encoding/json"
import "github.com/gorilla/mux"
import "github.com/garyburd/redigo/redis"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"
import "strings"
import "time"

type BroadcastMessage struct {
	channel string
	msg     string
}

type GroupServer struct {
	port  int
	c     chan *BroadcastMessage
	redis redis.Conn
}

func NewGroupServer(port int) *GroupServer {
	server := new(GroupServer)
	server.port = port
	server.c = make(chan *BroadcastMessage)
	return server
}


func (group_server *GroupServer) SendGroupNotification(appid int64, gid int64, 
	op map[string]interface{}, members []int64) {

	b, _ := json.Marshal(op)

	v := make(map[string]interface{})
	v["appid"] = appid
	v["group_id"] = gid
	v["notification"] = string(b)

	if len(members) > 0 {
		v["members"] = members
	}

	body, _ := json.Marshal(v)

	url := fmt.Sprintf("%s/post_group_notification", config.im_url)
	resp, err := http.Post(url, "application/json", strings.NewReader(string(body)))
	if err != nil {
		log.Info("post err:", err)
		return
	}

	resp.Body.Close()
	log.Info("send group notification success")
}

func (group_server *GroupServer) PublishMessage(channel string, msg string) {
	group_server.c <- &BroadcastMessage{channel, msg}
}

func (group_server *GroupServer) OpenDB() (*sql.DB, error) {
	db, err := sql.Open("mysql", config.mysqldb_datasource)
	return db, err
}

func (group_server *GroupServer) AuthRequest(r *http.Request) (int64, int64, error) {
	var appid int64
	var uid int64
	var err error

	appid, uid, err = BearerAuthentication(r)
	if err != nil {
		appid, err = BasicAuthorization(r)
	}
	return appid, uid, err
}

func (group_server *GroupServer) CreateGroup(appid int64, gname string,
	master int64, members []int64, super int8) int64 {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return 0
	}
	defer db.Close()
	gid := CreateGroup(db, appid, master, gname, super)
	if gid == 0 {
		return 0
	}
	for _, member := range members {
		AddGroupMember(db, gid, member)
	}

	content := fmt.Sprintf("%d,%d,%d", gid, appid, super)
	group_server.PublishMessage("group_create", content)

	for _, member := range members {
		content = fmt.Sprintf("%d,%d", gid, member)
		group_server.PublishMessage("group_member_add", content)
	}

	v := make(map[string]interface{})
	v["group_id"] = gid
	v["master"] = master
	v["name"] = gname
	v["members"] = members
	v["timestamp"] = int32(time.Now().Unix())
	op := make(map[string]interface{})
	op["create"] = v

	group_server.SendGroupNotification(appid, gid, op, members)

	return gid
}

func (group_server *GroupServer) DisbandGroup(appid int64, gid int64) bool {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	if !DeleteGroup(db, gid) {
		return false
	}

	v := make(map[string]interface{})
	v["group_id"] = gid
	v["timestamp"] = int32(time.Now().Unix())
	op := make(map[string]interface{})
	op["disband"] = v

	group_server.SendGroupNotification(appid, gid, op, nil)

	content := fmt.Sprintf("%d", gid)
	group_server.PublishMessage("group_disband", content)

	return true
}

func (group_server *GroupServer) AddGroupMember(appid int64, gid int64, uid int64) bool {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	if !AddGroupMember(db, gid, uid) {
		return false
	}

	v := make(map[string]interface{})
	v["group_id"] = gid
	v["member_id"] = uid
	v["timestamp"] = int32(time.Now().Unix())
	op := make(map[string]interface{})
	op["add_member"] = v

	group_server.SendGroupNotification(appid, gid, op, []int64{uid})

	content := fmt.Sprintf("%d,%d", gid, uid)
	group_server.PublishMessage("group_member_add", content)

	return true
}

func (group_server *GroupServer) QuitGroup(appid int64, gid int64, uid int64) bool {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	if !RemoveGroupMember(db, gid, uid) {
		return false
	}

	v := make(map[string]interface{})
	v["group_id"] = gid
	v["member_id"] = uid
	v["timestamp"] = int32(time.Now().Unix())
	op := make(map[string]interface{})
	op["quit_group"] = v
	group_server.SendGroupNotification(appid, gid, op, []int64{uid})

	content := fmt.Sprintf("%d,%d", gid, uid)
	group_server.PublishMessage("group_member_remove", content)

	return true
}

func (group_server *GroupServer) HandleCreate(w http.ResponseWriter, r *http.Request) {
	appid, _, err := group_server.AuthRequest(r)
	if err != nil {
		WriteHttpError(403, err.Error(), w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}
	var v map[string]interface{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}
	if v["master"] == nil || v["members"] == nil || v["name"] == nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}
	if _, ok := v["master"].(float64); !ok {
		log.Info("error:", err)
		WriteHttpError(400, "invalid param", w)
		return
	}
	master := int64(v["master"].(float64))
	if _, ok := v["members"].([]interface{}); !ok {
		WriteHttpError(400, "invalid param", w)
		return
	}
	if _, ok := v["name"].(string); !ok {
		WriteHttpError(400, "invalid param", w)
		return
	}
	name := v["name"].(string)
	
	var super int8
	if _, ok := v["super"].(bool); ok {
		s := v["super"].(bool)
		log.Info("super:", s)
		if s {
			super = 1
		} else {
			super = 0
		}
	}

	ms := v["members"].([]interface{})
	members := make([]int64, len(ms))
	for i, m := range ms {
		if _, ok := m.(float64); !ok {
			WriteHttpError(400, "invalid param", w)
			return
		}
		members[i] = int64(m.(float64))
	}
	log.Info("create group master:", master, " members:", members)

	gid := group_server.CreateGroup(appid, name, master, members, super)
	if gid == 0 {
		WriteHttpError(500, "db error", w)
		return
	}
	v = make(map[string]interface{})
	v["group_id"] = gid
	WriteHttpObj(v, w)
}

func (group_server *GroupServer) HandleDisband(w http.ResponseWriter, r *http.Request) {
	appid, _, err := group_server.AuthRequest(r)
	if err != nil {
		WriteHttpError(403, err.Error(), w)
		return
	}

	vars := mux.Vars(r)
	gid, err := strconv.ParseInt(vars["gid"], 10, 64)
	if err != nil {
		WriteHttpError(400, "group id is't integer", w)
		return
	}

	log.Info("disband:", gid)
	res := group_server.DisbandGroup(appid, gid)
	if !res {
		WriteHttpError(500, "db error", w)
	} else {
		w.WriteHeader(200)
	}
}

func (group_server *GroupServer) HandleAddGroupMember(w http.ResponseWriter, r *http.Request) {
	appid, _, err := group_server.AuthRequest(r)
	if err != nil {
		WriteHttpError(403, err.Error(), w)
		return
	}

	vars := mux.Vars(r)
	gid, err := strconv.ParseInt(vars["gid"], 10, 64)
	if err != nil {
		WriteHttpError(400, "group id is't integer", w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	var v map[string]float64
	err = json.Unmarshal(body, &v)
	if err != nil {
		WriteHttpError(400, "invalid json format", w)
		return
	}
	if v["uid"] == 0 {
		WriteHttpError(400, "uid can't be zero", w)
		return
	}
	uid := int64(v["uid"])
	log.Infof("gid:%d add member:%d\n", gid, uid)
	res := group_server.AddGroupMember(appid, gid, uid)
	if !res {
		WriteHttpError(500, "db error", w)
	} else {
		w.WriteHeader(200)
	}
}

func (group_server *GroupServer) HandleQuitGroup(w http.ResponseWriter, r *http.Request) {
	appid, _, err := group_server.AuthRequest(r)
	if err != nil {
		WriteHttpError(403, err.Error(), w)
		return
	}

	vars := mux.Vars(r)
	gid, _ := strconv.ParseInt(vars["gid"], 10, 64)
	mid, _ := strconv.ParseInt(vars["mid"], 10, 64)
	log.Info("quit group", gid, " ", mid)

	res := group_server.QuitGroup(appid, gid, mid)
	if !res {
		WriteHttpError(500, "db error", w)
	} else {
		w.WriteHeader(200)
	}
}

func (group_server *GroupServer) Publish(channel string, msg string) bool {
	if group_server.redis == nil {
		c, err := redis.Dial("tcp", config.redis_address)
		if err != nil {
			log.Info("error:", err)
			return false
		}
		group_server.redis = c
	}
	_, err := group_server.redis.Do("PUBLISH", channel, msg)
	if err != nil {
		log.Info("error:", err)
		group_server.redis = nil
		return false
	}
	log.Info("publish message:", channel, " ", msg)
	return true
}

func (group_server *GroupServer) RunPublish() {
	for {
		m := <-group_server.c
		group_server.Publish(m.channel, m.msg)
	}
}

