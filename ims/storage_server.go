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
import "math/rand"
import log "github.com/golang/glog"
import "os"
import "os/signal"
import "syscall"
import "github.com/gomodule/redigo/redis"
import "github.com/valyala/gorpc"


var storage *Storage
var config *StorageConfig
var master *Master
var mutex   sync.Mutex

func init() {
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


func handle_sync_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)
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
			storage.Flush()
			storage.SaveIndexFileAndExit()
        }
    }
    return nil // It'll never get here.
}

//flush storage file
func FlushLoop() {
	ticker := time.NewTicker(time.Millisecond * 1000)
	for range ticker.C {
		storage.Flush()
	}
}

//flush message index
func FlushIndexLoop() {
	//5 min
	ticker := time.NewTicker(time.Second * 60 * 5)
	for range ticker.C {
		storage.FlushIndex()
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
	dispatcher.AddFunc("GetLatestMessage", GetLatestMessage)
	
	s := &gorpc.Server{
		Addr: config.rpc_listen,
		Handler: dispatcher.NewHandlerFunc(),
	}

	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot start rpc server: %s", err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: ims config")
		return
	}

	config = read_storage_cfg(flag.Args()[0])
	log.Infof("rpc listen:%s storage root:%s sync listen:%s master address:%s is push system:%t offline message limit:%d\n", 
		config.rpc_listen, config.storage_root, config.sync_listen,
		config.master_address, config.is_push_system, config.limit)

	storage = NewStorage(config.storage_root)
	
	master = NewMaster()
	master.Start()
	if len(config.master_address) > 0 {
		slaver := NewSlaver(config.master_address)
		slaver.Start()
	}

	//刷新storage file
	go FlushLoop()
	go FlushIndexLoop()
	go waitSignal()

	go ListenSyncClient()
	ListenRPCClient()
}
