package main

import "fmt"
import "net/http"
import "strconv"
import "io/ioutil"
import "errors"
import "encoding/json"
import "github.com/gorilla/mux"
import "github.com/garyburd/redigo/redis"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"

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

func (group_server *GroupServer) SendMessage(receiver int64, msg *Message) {
	//todo send group notification
}

func (group_server *GroupServer) PublishMessage(channel string, msg string) {
	group_server.c <- &BroadcastMessage{channel, msg}
}

func (group_server *GroupServer) OpenDB() (*sql.DB, error) {
	db, err := sql.Open("mysql", config.mysqldb_datasource)
	return db, err
}


func (group_server *GroupServer) AuthToken(token string) (int64, int64, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("tokens_%s", token)

	var uid int64
	var appid int64
	
	reply, err := redis.Values(conn.Do("HMGET", key, "uid", "app_id"))
	if err != nil {
		log.Info("hmget error:", err)
		return 0, 0, err
	}

	_, err = redis.Scan(reply, &uid, &appid)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, 0, err
	}
	return appid, uid, nil
}


func (group_server *GroupServer) AuthRequest(r *http.Request) (int64, int64, error) {
	token := r.Header.Get("Authorization");
	if len(token) <= 7 {
		return 0, 0, errors.New("no authorization header")
	}
	if token[:7] != "Bearer " {
		return 0, 0, errors.New("no authorization header")
	}
	return group_server.AuthToken(token[7:])
}

func (group_server *GroupServer) CreateGroup(appid int64, gname string,
	master int64, members []int64) int64 {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return 0
	}
	defer db.Close()
	gid := CreateGroup(db, appid, master, gname)
	if gid == 0 {
		return 0
	}
	for _, member := range members {
		AddGroupMember(db, gid, member)
	}

	v := make(map[string]interface{})
	v["group_id"] = gid
	v["master"] = master
	v["name"] = gname
	v["members"] = members
	op := make(map[string]interface{})
	op["create"] = v
	b, _ := json.Marshal(op)
	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{string(b)}}
	for _, member := range members {
		group_server.SendMessage(member, msg)
	}
	content := fmt.Sprintf("%d", gid)
	group_server.PublishMessage("group_create", content)
	for _, member := range members {
		content = fmt.Sprintf("%d,%d", gid, member)
		group_server.PublishMessage("group_member_add", content)
	}
	return gid
}

func (group_server *GroupServer) DisbandGroup(gid int64) bool {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	if !DeleteGroup(db, gid) {
		return false
	}
	content := fmt.Sprintf("%d", gid)
	group_server.PublishMessage("group_disband", content)

	group := group_manager.FindGroup(gid)
	if group == nil {
		log.Info("can't find group:", gid)
		return true
	}
	v := make(map[string]interface{})
	v["group_id"] = gid
	op := make(map[string]interface{})
	op["disband"] = v
	b, _ := json.Marshal(op)
	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{string(b)}}
	for member := range group.Members() {
		group_server.SendMessage(member, msg)
	}

	return true
}

func (group_server *GroupServer) AddGroupMember(gid int64, uid int64) bool {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	if !AddGroupMember(db, gid, uid) {
		return false
	}
	content := fmt.Sprintf("%d,%d", gid, uid)
	group_server.PublishMessage("group_member_add", content)

	group := group_manager.FindGroup(gid)
	if group == nil {
		log.Info("can't find group:", gid)
		return true
	}
	v := make(map[string]interface{})
	v["group_id"] = gid
	v["member_id"] = uid
	op := make(map[string]interface{})
	op["add_member"] = v
	b, _ := json.Marshal(op)
	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{string(b)}}
	for member := range group.Members() {
		group_server.SendMessage(member, msg)
	}
	return true
}

func (group_server *GroupServer) QuitGroup(gid int64, uid int64) bool {
	db, err := group_server.OpenDB()
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	if !RemoveGroupMember(db, gid, uid) {
		return false
	}
	content := fmt.Sprintf("%d,%d", gid, uid)
	group_server.PublishMessage("group_member_remove", content)

	group := group_manager.FindGroup(gid)
	if group == nil {
		log.Info("can't find group:", gid)
		return true
	}
	v := make(map[string]interface{})
	v["group_id"] = gid
	v["member_id"] = uid
	op := make(map[string]interface{})
	op["quit_group"] = v
	b, _ := json.Marshal(op)
	msg := &Message{cmd: MSG_GROUP_NOTIFICATION, body: &GroupNotification{string(b)}}
	for member := range group.Members() {
		group_server.SendMessage(member, msg)
	}
	return true
}

func (group_server *GroupServer) HandleCreate(w http.ResponseWriter, r *http.Request) {
	appid, _, err := group_server.AuthRequest(r)
	if err != nil {
		w.WriteHeader(403)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	var v map[string]interface{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Info("error:", err)
		w.WriteHeader(400)
		return
	}
	if v["master"] == nil || v["members"] == nil || v["name"] == nil {
		log.Info("error:", err)
		w.WriteHeader(400)
		return
	}
	if _, ok := v["master"].(float64); !ok {
		log.Info("error:", err)
		w.WriteHeader(400)
		return
	}
	master := int64(v["master"].(float64))
	if _, ok := v["members"].([]interface{}); !ok {
		w.WriteHeader(400)
		return
	}
	if _, ok := v["name"].(string); !ok {
		w.WriteHeader(400)
		return
	}
	name := v["name"].(string)

	ms := v["members"].([]interface{})
	members := make([]int64, len(ms))
	for i, m := range ms {
		if _, ok := m.(float64); !ok {
			w.WriteHeader(400)
			return
		}
		members[i] = int64(m.(float64))
	}
	log.Info("create group master:", master, " members:", members)

	gid := group_server.CreateGroup(appid, name, master, members)
	if gid == 0 {
		w.WriteHeader(500)
		return
	}
	v = make(map[string]interface{})
	v["group_id"] = gid
	b, _ := json.Marshal(v)
	w.Write(b)
}

func (group_server *GroupServer) HandleDisband(w http.ResponseWriter, r *http.Request) {
	_, _, err := group_server.AuthRequest(r)
	if err != nil {
		w.WriteHeader(403)
		return
	}

	vars := mux.Vars(r)
	gid, err := strconv.ParseInt(vars["gid"], 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	log.Info("disband", gid)
	res := group_server.DisbandGroup(gid)
	if !res {
		w.WriteHeader(500)
	} else {
		w.WriteHeader(200)
	}
}

func (group_server *GroupServer) HandleAddGroupMember(w http.ResponseWriter, r *http.Request) {
	_, _, err := group_server.AuthRequest(r)
	if err != nil {
		w.WriteHeader(403)
		return
	}

	vars := mux.Vars(r)
	gid, err := strconv.ParseInt(vars["gid"], 10, 64)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	var v map[string]float64
	err = json.Unmarshal(body, &v)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	if v["uid"] == 0 {
		w.WriteHeader(400)
		return
	}
	uid := int64(v["uid"])
	log.Infof("gid:%d add member:%d\n", gid, uid)
	res := group_server.AddGroupMember(gid, uid)
	if !res {
		w.WriteHeader(500)
	} else {
		w.WriteHeader(200)
	}
}

func (group_server *GroupServer) HandleQuitGroup(w http.ResponseWriter, r *http.Request) {
	_, _, err := group_server.AuthRequest(r)
	if err != nil {
		w.WriteHeader(403)
		return
	}

	vars := mux.Vars(r)
	gid, _ := strconv.ParseInt(vars["gid"], 10, 64)
	mid, _ := strconv.ParseInt(vars["mid"], 10, 64)
	log.Info("quit group", gid, " ", mid)

	res := group_server.QuitGroup(gid, mid)
	if !res {
		w.WriteHeader(500)
	} else {
		w.WriteHeader(200)
	}
}

func (group_server *GroupServer) Run() {
	r := mux.NewRouter()
	r.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleCreate(w, r)
	}).Methods("POST")

	r.HandleFunc("/groups/{gid}", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleDisband(w, r)
	}).Methods("DELETE")

	r.HandleFunc("/groups/{gid}/members", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleAddGroupMember(w, r)
	}).Methods("POST")

	r.HandleFunc("/groups/{gid}/members/{mid}", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleQuitGroup(w, r)
	}).Methods("DELETE")

	http.Handle("/", r)

	var PORT = group_server.port
	var BIND_ADDR = ""
	addr := fmt.Sprintf("%s:%d", BIND_ADDR, PORT)
	http.ListenAndServe(addr, nil)
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
func (group_server *GroupServer) Start() {
	go group_server.RunPublish()
	go group_server.Run()
}
