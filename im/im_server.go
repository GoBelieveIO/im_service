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
import "flag"
import "time"
import "runtime"
import "math/rand"
import "net/http"
import "path"

import "github.com/gomodule/redigo/redis"
import log "github.com/golang/glog"
import "github.com/valyala/gorpc"
import "github.com/importcjj/sensitive"
import "github.com/bitly/go-simplejson"

var (
    VERSION    string
    BUILD_TIME string
    GO_VERSION string
	GIT_COMMIT_ID string
	GIT_BRANCH string
)

//storage server,  peer, group, customer message
var rpc_clients []*gorpc.DispatcherClient

//super group storage server
var group_rpc_clients []*gorpc.DispatcherClient

//route server
var route_channels []*Channel

//super group route server
var group_route_channels []*Channel

var app_route *AppRoute

var group_manager *GroupManager
var redis_pool *redis.Pool

var config *Config
var server_summary *ServerSummary

var sync_c chan *SyncHistory
var group_sync_c chan *SyncGroupHistory

var relationship_pool *RelationshipPool

//round-robin
var current_deliver_index uint64
var group_message_delivers []*GroupMessageDeliver
var filter *sensitive.Filter

func init() {
	app_route = NewAppRoute()
	server_summary = NewServerSummary()
	sync_c = make(chan *SyncHistory, 100)
	group_sync_c = make(chan *SyncGroupHistory, 100)
}



func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2)*time.Second
			c, err := redis.DialTimeout("tcp", server, timeout, 0, 0)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if db > 0 && db < 16 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}

//过滤敏感词
func FilterDirtyWord(msg *IMMessage) {
	if filter == nil {
		log.Info("filter is null")
		return
	}

	obj, err := simplejson.NewJson([]byte(msg.content))
	if err != nil {
		log.Info("filter dirty word, can't decode json")
		return
	}

	text, err := obj.Get("text").String()
	if err != nil {
		log.Info("filter dirty word, can't get text")
		return
	}

	if exist,  _ := filter.FindIn(text); exist {
		t := filter.RemoveNoise(text)
		replacedText := filter.Replace(t, '*')

		obj.Set("text", replacedText)
		c, err := obj.Encode()
		if err != nil {
			log.Errorf("json encode err:%s", err)
			return
		}
		msg.content = string(c)
		log.Infof("filter dirty word, replace text %s with %s", text, replacedText)
	}
}


type loggingHandler struct {
	handler http.Handler
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Infof("http request:%s %s %s", r.RemoteAddr, r.Method, r.URL)
	h.handler.ServeHTTP(w, r)
}

func StartHttpServer(addr string) {
	http.HandleFunc("/summary", Summary)
	http.HandleFunc("/stack", Stack)

	//rpc function
	http.HandleFunc("/post_group_notification", PostGroupNotification)
	http.HandleFunc("/post_im_message", PostIMMessage)
	http.HandleFunc("/load_latest_message", LoadLatestMessage)
	http.HandleFunc("/load_history_message", LoadHistoryMessage)
	http.HandleFunc("/post_system_message", SendSystemMessage)
	http.HandleFunc("/post_notification", SendNotification)
	http.HandleFunc("/post_room_message", SendRoomMessage)
	http.HandleFunc("/post_customer_message", SendCustomerMessage)
	http.HandleFunc("/post_customer_support_message", SendCustomerSupportMessage)
	http.HandleFunc("/post_realtime_message", SendRealtimeMessage)
	http.HandleFunc("/init_message_queue", InitMessageQueue)
	http.HandleFunc("/get_offline_count", GetOfflineCount)
	http.HandleFunc("/dequeue_message", DequeueMessage)

	handler := loggingHandler{http.DefaultServeMux}
	
	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}

func SyncKeyService() {
	for {
		select {
		case s := <- sync_c:
			origin := GetSyncKey(s.AppID, s.Uid)
			if s.LastMsgID > origin {
				log.Infof("save sync key:%d %d %d", s.AppID, s.Uid, s.LastMsgID)
				SaveSyncKey(s.AppID, s.Uid, s.LastMsgID)
			}
			break
		case s := <- group_sync_c:
			origin := GetGroupSyncKey(s.AppID, s.Uid, s.GroupID)
			if s.LastMsgID > origin {
				log.Infof("save group sync key:%d %d %d %d", 
					s.AppID, s.Uid, s.GroupID, s.LastMsgID)
				SaveGroupSyncKey(s.AppID, s.Uid, s.GroupID, s.LastMsgID)
			}
			break
		}
	}
}

func main() {
	fmt.Printf("Version:     %s\nBuilt:       %s\nGo version:  %s\nGit branch:  %s\nGit commit:  %s\n", VERSION, BUILD_TIME, GO_VERSION, GIT_BRANCH, GIT_COMMIT_ID)
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}
	
	config = read_cfg(flag.Args()[0])
	log.Infof("port:%d\n", config.port)

	log.Infof("redis address:%s password:%s db:%d\n", 
		config.redis_address, config.redis_password, config.redis_db)

	log.Info("storage addresses:", config.storage_rpc_addrs)
	log.Info("route addressed:", config.route_addrs)
	log.Info("group route addressed:", config.group_route_addrs)	
	log.Info("kefu appid:", config.kefu_appid)
	log.Info("pending root:", config.pending_root)
	
	log.Infof("socket io address:%s tls_address:%s cert file:%s key file:%s",
		config.socket_io_address, config.tls_address, config.cert_file, config.key_file)
	log.Infof("ws address:%s wss address:%s", config.ws_address, config.wss_address)
	log.Info("group deliver count:", config.group_deliver_count)
	log.Infof("friend permission:%t enable blacklist:%t", config.friend_permission, config.enable_blacklist)
	
	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
		config.redis_db)

	rpc_clients = make([]*gorpc.DispatcherClient, 0)
	for _, addr := range(config.storage_rpc_addrs) {
		c := &gorpc.Client{
			Conns: 4,
			Addr: addr,
		}
		c.Start()

		dispatcher := gorpc.NewDispatcher()
		dispatcher.AddFunc("SyncMessage", SyncMessageInterface)
		dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessageInterface)
		dispatcher.AddFunc("SavePeerMessage", SavePeerMessageInterface)
		dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessageInterface)
		dispatcher.AddFunc("GetLatestMessage", GetLatestMessageInterface)

		dc := dispatcher.NewFuncClient(c)

		rpc_clients = append(rpc_clients, dc)
	}

	if len(config.group_storage_rpc_addrs) > 0 {
		group_rpc_clients = make([]*gorpc.DispatcherClient, 0)
		for _, addr := range(config.group_storage_rpc_addrs) {
			c := &gorpc.Client{
				Conns: 4,
				Addr: addr,
			}
			c.Start()

			dispatcher := gorpc.NewDispatcher()
			dispatcher.AddFunc("SyncMessage", SyncMessageInterface)
			dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessageInterface)
			dispatcher.AddFunc("SavePeerMessage", SavePeerMessageInterface)
			dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessageInterface)

			dc := dispatcher.NewFuncClient(c)

			group_rpc_clients = append(group_rpc_clients, dc)
		}
	} else {
		group_rpc_clients = rpc_clients
	}

	route_channels = make([]*Channel, 0)
	for _, addr := range(config.route_addrs) {
		channel := NewChannel(addr, DispatchAppMessage, DispatchGroupMessage, DispatchRoomMessage)
		channel.Start()
		route_channels = append(route_channels, channel)
	}

	if len(config.group_route_addrs) > 0 {
		group_route_channels = make([]*Channel, 0)
		for _, addr := range(config.group_route_addrs) {
			channel := NewChannel(addr, DispatchAppMessage, DispatchGroupMessage, DispatchRoomMessage)
			channel.Start()
			group_route_channels = append(group_route_channels, channel)
		}
	} else {
		group_route_channels = route_channels
	}

	if len(config.word_file) > 0 {
		filter = sensitive.New()
		filter.LoadWordDict(config.word_file)
	}
	
	group_manager = NewGroupManager()
	group_manager.Start()

	group_message_delivers = make([]*GroupMessageDeliver, config.group_deliver_count)
	for i := 0; i < config.group_deliver_count; i++ {
		q := fmt.Sprintf("q%d", i)
		r := path.Join(config.pending_root, q)
		deliver := NewGroupMessageDeliver(r)
		deliver.Start()
		group_message_delivers[i] = deliver
	}
	
	go ListenRedis()
	go SyncKeyService()

	if config.friend_permission || config.enable_blacklist {
		relationship_pool = NewRelationshipPool()
		relationship_pool.Start()
	}
	
	go StartHttpServer(config.http_listen_address)
	StartRPCServer(config.rpc_listen_address)

	if len(config.socket_io_address) > 0 {
		go StartSocketIO(config.socket_io_address, config.tls_address, 
			config.cert_file, config.key_file)
	}
	if len(config.ws_address) > 0 {
		go StartWSServer(config.ws_address)
	}
	if len(config.wss_address) > 0 && len(config.cert_file) > 0 && len(config.key_file) > 0 {
		go StartWSSServer(config.wss_address, config.cert_file, config.key_file)
	}
	
	if config.ssl_port > 0 && len(config.cert_file) > 0 && len(config.key_file) > 0 {
		go ListenSSL(config.ssl_port, config.cert_file, config.key_file)
	}
	ListenClient()
	log.Infof("exit")
}
