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
import "io/ioutil"
import "strconv"
import "strings"
import "os"
import "os/exec"
import "sync/atomic"

import "github.com/gomodule/redigo/redis"
import "gopkg.in/natefinch/lumberjack.v2"
import log "github.com/sirupsen/logrus"
import "github.com/importcjj/sensitive"
import "github.com/bitly/go-simplejson"
import "github.com/GoBelieveIO/im_service/storage"


var (
    VERSION    string
    BUILD_TIME string
    GO_VERSION string
	GIT_COMMIT_ID string
	GIT_BRANCH string
)

var auth Auth;

var rpc_storage *RPCStorage

//route server
var route_channels []*Channel

//super group route server
var group_route_channels []*Channel

var app_route *AppRoute

var group_manager *GroupManager
var redis_pool *redis.Pool

var config *Config
var server_summary *ServerSummary

var sync_c chan *storage.SyncHistory
var group_sync_c chan *storage.SyncGroupHistory

var relationship_pool *RelationshipPool

//round-robin
var current_deliver_index uint64
var group_message_delivers []*GroupMessageDeliver
var filter *sensitive.Filter

var low_memory int32//低内存状态


func init() {
	app_route = NewAppRoute()
	server_summary = NewServerSummary()
	sync_c = make(chan *storage.SyncHistory, 100)
	group_sync_c = make(chan *storage.SyncGroupHistory, 100)
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
	http.HandleFunc("/post_peer_message", PostPeerMessage)		
	http.HandleFunc("/post_group_message", PostGroupMessage)	
	http.HandleFunc("/load_latest_message", LoadLatestMessage)
	http.HandleFunc("/load_history_message", LoadHistoryMessage)
	http.HandleFunc("/post_system_message", SendSystemMessage)
	http.HandleFunc("/post_notification", SendNotification)
	http.HandleFunc("/post_room_message", SendRoomMessage)
	http.HandleFunc("/post_customer_message", SendCustomerMessage)
	http.HandleFunc("/post_customer_support_message", SendCustomerSupportMessage)
	http.HandleFunc("/post_realtime_message", SendRealtimeMessage)
	http.HandleFunc("/get_offline_count", GetOfflineCount)


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

func formatStdOut(stdout []byte, userfulIndex int) []string {
	eol := "\n"
	infoArr := strings.Split(string(stdout), eol)[userfulIndex]
	ret := strings.Fields(infoArr)
	return ret
}

func ReadRSSDarwin(pid int) int64 {
	args := "-o rss -p"
	stdout, _ := exec.Command("ps", args, strconv.Itoa(pid)).Output()
	ret := formatStdOut(stdout, 1)
	if len(ret) == 0 {
		log.Warning("can't find process")
		return 0
	}

	rss, _ := strconv.ParseInt(ret[0], 10, 64)
	return rss*1024
}

func ReadRSSLinux(pid int, pagesize int) int64 {
	//http://man7.org/linux/man-pages/man5/proc.5.html
	procStatFileBytes, err := ioutil.ReadFile(path.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		log.Warning("read file err:", err)
		return 0
	}
	
	splitAfter := strings.SplitAfter(string(procStatFileBytes), ")")

	if len(splitAfter) == 0 || len(splitAfter) == 1 {
		log.Warning("Can't find process ")
		return 0
	}
	
	infos := strings.Split(splitAfter[1], " ")
	if len(infos) < 23 {
		//impossible
		return 0
	}

	rss, _ := strconv.ParseInt(infos[22], 10, 64)
	return rss*int64(pagesize)
}

func ReadRSS(platform string, pid int, pagesize int) int64 {
	if platform == "linux" {
		return ReadRSSLinux(pid, pagesize)
	} else if platform == "darwin" {
		return ReadRSSDarwin(pid)
	} else {
		return 0
	}
}

func MemStatService() {
	platform := runtime.GOOS
	pagesize := os.Getpagesize();
	pid := os.Getpid()	
	//3 min
	ticker := time.NewTicker(time.Second * 60 * 3)
	for range ticker.C {
		rss := ReadRSS(platform, pid, pagesize)
		if rss > config.memory_limit {
			atomic.StoreInt32(&low_memory, 1)
		} else {
			atomic.StoreInt32(&low_memory, 0)
		}
		log.Infof("process rss:%dk low memory:%d", rss/1024, low_memory)
	}
}

func initLog() {
	if config.log_filename != "" {
		writer := &lumberjack.Logger{
			Filename:   config.log_filename,
			MaxSize:    1024, // megabytes
			MaxBackups: config.log_backup,
			MaxAge:     config.log_age, //days
			Compress:   false,
		}
		log.SetOutput(writer)
		log.StandardLogger().SetNoLock()
	}

	log.SetReportCaller(config.log_caller)

	level := config.log_level
	if level == "debug" {
		log.SetLevel(log.DebugLevel)
	} else if level == "info" {
		log.SetLevel(log.InfoLevel)
	} else if level == "warn" {
		log.SetLevel(log.WarnLevel)
	} else if level == "fatal" {
		log.SetLevel(log.FatalLevel)
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

	initLog()

	log.Info("startup...")
	log.Infof("port:%d\n", config.port)

	log.Infof("redis address:%s password:%s db:%d\n", 
		config.redis_address, config.redis_password, config.redis_db)

	log.Info("storage addresses:", config.storage_rpc_addrs)
	log.Info("route addressed:", config.route_addrs)
	log.Info("group route addressed:", config.group_route_addrs)	
	log.Info("kefu appid:", config.kefu_appid)
	log.Info("pending root:", config.pending_root)

	log.Infof("ws address:%s wss address:%s", config.ws_address, config.wss_address)	
	log.Infof("cert file:%s key file:%s", config.cert_file, config.key_file)
	
	log.Info("group deliver count:", config.group_deliver_count)
	log.Infof("friend permission:%t enable blacklist:%t", config.friend_permission, config.enable_blacklist)
	log.Infof("memory limit:%d", config.memory_limit)

	log.Infof("auth method:%s", config.auth_method)
	log.Infof("jwt sign key:%s", string(config.jwt_signing_key))
	
	log.Infof("log filename:%s level:%s backup:%d age:%d caller:%t",
		config.log_filename, config.log_level, config.log_backup, config.log_age, config.log_caller)
	
	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
		config.redis_db)

	auth = NewAuth(config.auth_method)

	rpc_storage = NewRPCStorage(config.storage_rpc_addrs, config.group_storage_rpc_addrs)
	
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

	if len(config.mysqldb_datasource) > 0 {
		group_manager = NewGroupManager()
		group_manager.Start()
	}

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

	if config.memory_limit > 0 {
		go MemStatService()
	}

	if config.friend_permission || config.enable_blacklist {
		relationship_pool = NewRelationshipPool()
		relationship_pool.Start()
	}
	
	go StartHttpServer(config.http_listen_address)

	if len(config.ws_address) > 0 {
		go StartWSServer(config.ws_address)
	}
	if len(config.wss_address) > 0 && len(config.cert_file) > 0 && len(config.key_file) > 0 {
		go StartWSSServer(config.wss_address, config.cert_file, config.key_file)
	}
	
	if config.ssl_port > 0 && len(config.cert_file) > 0 && len(config.key_file) > 0 {
		go ListenSSL(config.ssl_port, config.cert_file, config.key_file)
	}
	ListenClient(config.port)
	log.Infof("exit")
}
