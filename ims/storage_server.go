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
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/gomodule/redigo/redis"
	"gopkg.in/natefinch/lumberjack.v2"

	st "github.com/GoBelieveIO/im_service/storage"
	log "github.com/sirupsen/logrus"
)

var (
	VERSION       string
	BUILD_TIME    string
	GO_VERSION    string
	GIT_COMMIT_ID string
	GIT_BRANCH    string
)

var storage *st.Storage
var config *StorageConfig
var master *st.Master
var server_summary *ServerSummary

func init() {
	server_summary = NewServerSummary()
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
	client := st.NewSyncClient(conn, storage, master)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)
}

// Signal handler
func waitSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	for {
		sig := <-ch
		fmt.Println("singal:", sig.String())
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			storage.Flush()
			storage.SaveIndexFileAndExit()
		}
	}
}

// flush storage file
func FlushLoop() {
	ticker := time.NewTicker(time.Millisecond * 1000)
	for range ticker.C {
		storage.Flush()
	}
}

// flush message index
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
			timeout := time.Duration(2) * time.Second
			c, err := redis.Dial("tcp", server, redis.DialConnectTimeout(timeout))
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

	handler := loggingHandler{http.DefaultServeMux}

	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}

func ListenRPCClient() {
	rpc_s := new(RPCStorage)
	rpc.Register(rpc_s)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", config.rpc_listen)
	if err != nil {
		log.Fatalf("can't start rpc server:%s", err)
	}

	http.Serve(l, nil)

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

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: ims config")
		return
	}

	config = read_storage_cfg(flag.Args()[0])

	initLog()

	log.Info("startup...")

	log.Infof("rpc listen:%s storage root:%s sync listen:%s master address:%s is push system:%t group limit:%d offline message limit:%d hard limit:%d\n",
		config.rpc_listen, config.storage_root, config.sync_listen,
		config.master_address, config.is_push_system, config.group_limit,
		config.limit, config.hard_limit)
	log.Infof("http listen address:%s", config.http_listen_address)

	if config.limit == 0 {
		log.Error("config limit is 0")
		return
	}
	if config.hard_limit > 0 && config.hard_limit/config.limit < 2 {
		log.Errorf("config limit:%d, hard limit:%d invalid, hard limit/limit must gte 2", config.limit, config.hard_limit)
		return
	}

	log.Infof("log filename:%s level:%s backup:%d age:%d caller:%t",
		config.log_filename, config.log_level, config.log_backup, config.log_age, config.log_caller)

	storage = st.NewStorage(config.storage_root, master.Channel())

	master = st.NewMaster()
	master.Start()
	if len(config.master_address) > 0 {
		slaver := st.NewSlaver(config.master_address, storage)
		slaver.Start()
	}

	//刷新storage file
	go FlushLoop()
	go FlushIndexLoop()
	go waitSignal()

	if len(config.http_listen_address) > 0 {
		go StartHttpServer(config.http_listen_address)
	}

	go ListenSyncClient()
	ListenRPCClient()
}
