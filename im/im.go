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
import "net"
import "fmt"
import "flag"
import "time"
import "runtime"
import "net/http"
import "github.com/garyburd/redigo/redis"
import log "github.com/golang/glog"
import "github.com/valyala/gorpc"

var storage_pools []*StorageConnPool

//group storage server
var rpc_clients []*gorpc.DispatcherClient

//route server
var route_channels []*Channel

var app_route *AppRoute
var group_manager *GroupManager
var redis_pool *redis.Pool

var config *Config
var server_summary *ServerSummary

var sync_c chan *SyncHistory
var group_sync_c chan *SyncGroupHistory


func init() {
	app_route = NewAppRoute()
	server_summary = NewServerSummary()
	sync_c = make(chan *SyncHistory, 100)
	group_sync_c = make(chan *SyncGroupHistory, 100)
}

func handle_client(conn net.Conn) {
	log.Infoln("handle_client")
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(net.Conn), port int) {
	TCPService(fmt.Sprintf("0.0.0.0:%d", port), f)

}
func ListenClient() {
	Listen(handle_client, config.port)
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

//个人消息／普通群消息／客服消息
func GetStorageRPCClient(uid int64) *gorpc.DispatcherClient {
	index := uid%int64(len(rpc_clients))
	return rpc_clients[index]
}

//超级群消息
func GetGroupStorageRPCClient(group_id int64) *gorpc.DispatcherClient {
	index := group_id%int64(len(rpc_clients))
	return rpc_clients[index]
}

func GetStorageConnPool(uid int64) *StorageConnPool {
	index := uid%int64(len(storage_pools))
	return storage_pools[index]
}

func GetGroupStorageConnPool(gid int64) *StorageConnPool {
	index := gid%int64(len(storage_pools))
	return storage_pools[index]
}


func GetChannel(uid int64) *Channel{
	index := uid%int64(len(route_channels))
	return route_channels[index]
}

func GetGroupChannel(group_id int64) *Channel{
	index := group_id%int64(len(route_channels))
	return route_channels[index]
}

func GetRoomChannel(room_id int64) *Channel {
	index := room_id%int64(len(route_channels))
	return route_channels[index]
}

func SaveGroupMessage(appid int64, gid int64, device_id int64, msg *Message) (int64, error) {
	dc := GetGroupStorageRPCClient(gid)
	
	gm := &GroupMessage{
		AppID:appid,
		GroupID:gid,
		DeviceID:device_id,
		Cmd:int32(msg.cmd),
		Raw:msg.ToData(),
	}
	resp, err := dc.Call("SaveGroupMessage", gm)
	if err != nil {
		log.Warning("save group message err:", err)
		return 0, err
	}
	msgid := resp.(int64)
	log.Infof("save group message:%d %d %d\n", appid, gid, msgid)
	return msgid, nil
}

func SaveMessage(appid int64, uid int64, device_id int64, m *Message) (int64, error) {
	dc := GetStorageRPCClient(uid)
	
	pm := &PeerMessage{
		AppID:appid,
		Uid:uid,
		DeviceID:device_id,
		Cmd:int32(m.cmd),
		Raw:m.ToData(),
	}

	resp, err := dc.Call("SavePeerMessage", pm)
	if err != nil {
		log.Error("save peer message err:", err)
		return 0, err
	}

	msgid := resp.(int64)
	log.Infof("save peer message:%d %d %d %d\n", appid, uid, device_id, msgid)
	return msgid, nil
}

func PushGroupMessage(appid int64, group_id int64, m *Message) {
	amsg := &AppMessage{appid:appid, receiver:group_id, msgid:0, msg:m}
	channel := GetGroupChannel(group_id)
	channel.PublishGroup(amsg)
}

func PushMessage(appid int64, uid int64, m *Message) {
	amsg := &AppMessage{appid:appid, receiver:uid, msgid:0, msg:m}
	channel := GetChannel(uid)
	channel.Publish(amsg)
}


func SendAppGroupMessage(appid int64, group_id int64, msg *Message) {
	amsg := &AppMessage{appid:appid, receiver:group_id, msgid:0, msg:msg}
	channel := GetGroupChannel(group_id)
	channel.PublishGroup(amsg)
	DispatchGroupMessage(amsg)
}

func SendAppMessage(appid int64, uid int64, msg *Message) {
	amsg := &AppMessage{appid:appid, receiver:uid, msgid:0, msg:msg}
	channel := GetChannel(uid)
	channel.Publish(amsg)
	DispatchAppMessage(amsg)
}

func DispatchAppMessage(amsg *AppMessage) {
	log.Info("dispatch app message:", Command(amsg.msg.cmd))

	route := app_route.FindRoute(amsg.appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
		return
	}
	clients := route.FindClientSet(amsg.receiver)
	if len(clients) == 0 {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
		return
	}
	for c, _ := range(clients) {
		c.EnqueueMessage(amsg.msg)
	}
}

func DispatchRoomMessage(amsg *AppMessage) {
	log.Info("dispatch room message", Command(amsg.msg.cmd))
	room_id := amsg.receiver
	route := app_route.FindOrAddRoute(amsg.appid)
	clients := route.FindRoomClientSet(room_id)

	if len(clients) == 0 {
		log.Warningf("can't dispatch room message, appid:%d room id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
		return
	}
	for c, _ := range(clients) {
		c.EnqueueMessage(amsg.msg)
	}	
}

func DispatchGroupMessage(amsg *AppMessage) {
	log.Info("dispatch group message:", Command(amsg.msg.cmd))
	group := group_manager.FindGroup(amsg.receiver)
	if group == nil {
		log.Warningf("can't dispatch group message, appid:%d group id:%d", amsg.appid, amsg.receiver)
		return
	}

	route := app_route.FindRoute(amsg.appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
		return
	}

	members := group.Members()
	for member := range members {
	    clients := route.FindClientSet(member)
		if len(clients) == 0 {
			continue
		}

		for c, _ := range(clients) {
			c.EnqueueMessage(amsg.msg)
		}
	}
}

func DialStorageFun(addr string) func()(*StorageConn, error) {
	f := func() (*StorageConn, error){
		storage := NewStorageConn()
		err := storage.Dial(addr)
		if err != nil {
			log.Error("connect storage err:", err)
			return nil, err
		}
		return storage, nil
	}
	return f
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
	http.HandleFunc("/post_room_message", SendRoomMessage)
	http.HandleFunc("/post_customer_message", SendCustomerMessage)
	http.HandleFunc("/post_realtime_message", SendRealtimeMessage)
	http.HandleFunc("/init_message_queue", InitMessageQueue)
	http.HandleFunc("/get_offline_count", GetOfflineCount)
	http.HandleFunc("/dequeue_message", DequeueMessage)

	handler := loggingHandler{http.DefaultServeMux}
	HTTPService(addr, handler)
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
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config = read_cfg(flag.Args()[0])
	log.Infof("port:%d redis address:%s\n",
		config.port,  config.redis_address)

	log.Infof("redis address:%s password:%s db:%d\n", 
		config.redis_address, config.redis_password, config.redis_db)

	log.Info("storage addresses:", config.storage_addrs)
	log.Info("route addressed:", config.route_addrs)
	log.Info("kefu appid:", config.kefu_appid)

	log.Infof("socket io address:%s tls_address:%s cert file:%s key file:%s",
		config.socket_io_address, config.tls_address, config.cert_file, config.key_file)
	
	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
		config.redis_db)

	storage_pools = make([]*StorageConnPool, 0)
	for _, addr := range(config.storage_addrs) {
		f := DialStorageFun(addr)
		pool := NewStorageConnPool(100, 500, 600 * time.Second, f) 
		storage_pools = append(storage_pools, pool)
	}


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

		dc := dispatcher.NewFuncClient(c)

		rpc_clients = append(rpc_clients, dc)
	}


	route_channels = make([]*Channel, 0)
	for _, addr := range(config.route_addrs) {
		channel := NewChannel(addr, DispatchAppMessage, DispatchGroupMessage, DispatchRoomMessage)
		channel.Start()
		route_channels = append(route_channels, channel)
	}
	
	group_manager = NewGroupManager()
	group_manager.Start()

	go ListenRedis()

	StartHttpServer(config.http_listen_address)

	go StartSocketIO(config.socket_io_address, config.tls_address, 
		config.cert_file, config.key_file)
	go SyncKeyService()

	ListenClient()
	Wait()
}
