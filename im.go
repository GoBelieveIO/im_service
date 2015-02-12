package main

import "net"
import "fmt"
import "flag"
import "time"
import "runtime"
import "github.com/garyburd/redigo/redis"
import log "github.com/golang/glog"

var channels []*Channel
var app_route *AppRoute
var group_manager *GroupManager
var redis_pool *redis.Pool
var storage_pools []*StorageConnPool
var config *Config
var server_summary *ServerSummary

func init() {
	app_route = NewAppRoute()
	server_summary = NewServerSummary()
}

func handle_client(conn net.Conn) {
	log.Infoln("handle_client")
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(net.Conn), port int) {
	SocketService(fmt.Sprintf("0.0.0.0:%d", port), f)

}
func ListenClient() {
	Listen(handle_client, config.port)
}

func NewRedisPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}

func SendAppMessage(amsg *AppMessage, uid int64) {
	route := app_route.FindRoute(amsg.appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d", amsg.appid, amsg.receiver)
		return
	}
	clients := route.FindClientSet(uid)
	if len(clients) == 0 {
		log.Warningf("can't dispatch app message, appid:%d uid:%d", amsg.appid, amsg.receiver)
		return
	}
	if clients != nil {
		for c, _ := range(clients) {
			if amsg.msgid > 0 {
				c.ewt <- &EMessage{msgid:amsg.msgid, msg:amsg.msg}
			} else {
				c.wt <- amsg.msg
			}
		}
	}	
}

func DispatchAppMessage(amsg *AppMessage) {
	log.Info("dispatch app message:", Command(amsg.msg.cmd))
	SendAppMessage(amsg, amsg.receiver)
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

	log.Info("storage addresses:", config.storage_addrs)
	log.Info("route addressed:", config.route_addrs)
	
	redis_pool = NewRedisPool(config.redis_address, "")



	storage_pools = make([]*StorageConnPool, 0)
	for _, addr := range(config.storage_addrs) {
		f := DialStorageFun(addr)
		pool := NewStorageConnPool(100, 500, 600 * time.Second, f) 
		storage_pools = append(storage_pools, pool)
	}

	channels = make([]*Channel, 0)
	for _, addr := range(config.route_addrs) {
		channel := NewChannel(addr, DispatchAppMessage, nil)
		channel.Start()
		channels = append(channels, channel)
	}
	
	group_manager = NewGroupManager()
	group_manager.Start()

	StartHttpServer(config.http_listen_address)

	go StartSocketIO(config.socket_io_address)
	ListenClient()
	Wait()
}
