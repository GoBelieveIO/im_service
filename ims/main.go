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
	Listen(handle_sync_client, config.SyncListen)
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

	l, err := net.Listen("tcp", config.RpcListen)
	if err != nil {
		log.Fatalf("can't start rpc server:%s", err)
	}

	http.Serve(l, nil)

}

func initLog() {
	if config.Log.Filename != "" {
		writer := &lumberjack.Logger{
			Filename:   config.Log.Filename,
			MaxSize:    1024, // megabytes
			MaxBackups: config.Log.Backup,
			MaxAge:     config.Log.Age, //days
			Compress:   false,
		}
		log.SetOutput(writer)
		log.StandardLogger().SetNoLock()
	}

	log.SetReportCaller(config.Log.Caller)

	level := config.Log.Level
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

	log.Infof("rpc listen:%s storage root:%s sync listen:%s master address:%s group limit:%d offline message limit:%d hard limit:%d\n",
		config.RpcListen, config.StorageRoot, config.SyncListen,
		config.MasterAddress, config.GroupLimit,
		config.Limit, config.HardLimit)
	log.Infof("http listen address:%s", config.HttpListenAddress)

	if config.Limit == 0 {
		log.Error("config limit is 0")
		return
	}
	if config.HardLimit > 0 && config.HardLimit/config.Limit < 2 {
		log.Errorf("config limit:%d, hard limit:%d invalid, hard limit/limit must gte 2", config.Limit, config.HardLimit)
		return
	}

	log.Infof("log filename:%s level:%s backup:%d age:%d caller:%t",
		config.Log.Filename, config.Log.Level, config.Log.Backup, config.Log.Age, config.Log.Caller)

	storage = st.NewStorage(config.StorageRoot, master.Channel())

	master = st.NewMaster()
	master.Start()
	if config.MasterAddress != "" {
		slaver := st.NewSlaver(config.MasterAddress, storage)
		slaver.Start()
	}

	//刷新storage file
	go FlushLoop()
	go FlushIndexLoop()
	go waitSignal()

	if len(config.HttpListenAddress) > 0 {
		go StartHttpServer(config.HttpListenAddress)
	}

	go ListenSyncClient()
	ListenRPCClient()
}
