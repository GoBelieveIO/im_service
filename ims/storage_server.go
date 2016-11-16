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
import "time"
import "sync"
import "runtime"
import "flag"
import log "github.com/golang/glog"
import "os"
import "os/signal"
import "syscall"
import "github.com/garyburd/redigo/redis"
import "github.com/valyala/gorpc"

const GROUP_OFFLINE_LIMIT = 100
const GROUP_C_COUNT = 10


var storage *Storage
var config *StorageConfig
var master *Master
var group_manager *GroupManager
var clients ClientSet
var mutex   sync.Mutex
var redis_pool *redis.Pool

var group_c []chan func()

func init() {
	clients = NewClientSet()
	group_c = make([]chan func(), GROUP_C_COUNT)
	for i := 0; i < GROUP_C_COUNT; i++ {
		group_c[i] = make(chan func())
	}
}

func GetGroupChan(gid int64) chan func() {
	index := gid%GROUP_C_COUNT
	return group_c[index]
}

func GetUserChan(uid int64) chan func() {
	index := uid%GROUP_C_COUNT
	return group_c[index]
}

//clone when write, lockless when read
func AddClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()
	
	if clients.IsMember(client) {
		return
	}
	c := clients.Clone()
	c.Add(client)
	clients = c
}

func RemoveClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	if !clients.IsMember(client) {
		return
	}
	c := clients.Clone()
	c.Remove(client)
	clients = c
}

//group im
func FindGroupClientSet(appid int64, gid int64) ClientSet {
	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppGroupID(appid, gid) {
			s.Add(c)
		}
	}
	return s
}

func IsGroupUserOnline(appid int64, gid int64, uid int64) bool {
	for c := range(clients) {
		if c.ContainGroupUserID(appid, gid, uid) {
			return true
		}
	}
	return false
}

//peer im
func FindClientSet(id *AppUserID) ClientSet {
	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppUserID(id) {
			s.Add(c)
		}
	}
	return s
}

func IsUserOnline(appid int64, uid int64) bool {
	id := &AppUserID{appid:appid, uid:uid}
	for c := range(clients) {
		if c.ContainAppUserID(id) {
			return true
		}
	}
	return false
}


func handle_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(*net.TCPConn), listen_addr string) {
	listen, err := net.Listen("tcp", listen_addr)
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	tcp_listener, ok := listen.(*net.TCPListener)
	if !ok {
		fmt.Println("listen error")
		return
	}

	for {
		client, err := tcp_listener.AcceptTCP()
		if err != nil {
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handle_client, config.listen)
}

func handle_sync_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)
}

func GroupLoop(c chan func()) {
	for {
		f := <- c
		f()
	}
}


// Signal handler
func waitSignal() error {
    ch := make(chan os.Signal, 1)
    signal.Notify(
    ch,
    syscall.SIGINT,
    syscall.SIGTERM,
    )
    for {
        sig := <-ch
        fmt.Println("singal:", sig.String())
        switch sig {
            case syscall.SIGTERM, syscall.SIGINT:
			storage.FlushReceived()
			os.Exit(0)
        }
    }
    return nil // It'll never get here.
}

func FlushLoop() {
	for {
		time.Sleep(1*time.Second)
		storage.FlushReceived()
	}
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



func ListenRPCClient() {
	dispatcher := gorpc.NewDispatcher()
	dispatcher.AddFunc("SyncMessage", SyncMessage)
	dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessage)
	dispatcher.AddFunc("SavePeerMessage", SavePeerMessage)
	dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessage)
	dispatcher.AddFunc("GetNewCount", GetNewCount)
	
	s := &gorpc.Server{
		Addr: config.rpc_listen,
		Handler: dispatcher.NewHandlerFunc(),
	}

	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot start rpc server: %s", err)
	}

}
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: ims config")
		return
	}

	config = read_storage_cfg(flag.Args()[0])
	log.Infof("listen:%s rpc listen:%s storage root:%s sync listen:%s master address:%s is push system:%d\n", 
		config.listen, config.rpc_listen, config.storage_root, config.sync_listen, config.master_address, config.is_push_system)

	log.Infof("redis address:%s password:%s db:%d\n", 
		config.redis_address, config.redis_password, config.redis_db)

	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
		config.redis_db)
	storage = NewStorage(config.storage_root)
	
	master = NewMaster()
	master.Start()
	if len(config.master_address) > 0 {
		slaver := NewSlaver(config.master_address)
		slaver.Start()
	}

	group_manager = NewGroupManager()
	group_manager.Start()

	for i := 0; i < GROUP_C_COUNT; i++ {
		go GroupLoop(group_c[i])
	}

	//刷新storage缓存的ack
	go FlushLoop()
	go waitSignal()

	go ListenSyncClient()
	go ListenRPCClient()

	ListenClient()
}
