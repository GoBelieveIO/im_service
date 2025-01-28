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
	"runtime"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/GoBelieveIO/im_service/router"
	"github.com/gomodule/redigo/redis"
	log "github.com/sirupsen/logrus"
)

var (
	VERSION       string
	BUILD_TIME    string
	GO_VERSION    string
	GIT_COMMIT_ID string
	GIT_BRANCH    string
)

var config *RouteConfig
var server *router.Server

func handle_client(conn *net.TCPConn, server *router.Server) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := router.NewClient(conn, server)
	log.Info("new client:", conn.RemoteAddr())
	server.AddClient(client)
	client.Run()
}

func Listen(f func(*net.TCPConn, *router.Server), listen_addr string, server *router.Server) {
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
		f(client, server)
	}
}

func ListenClient(server *router.Server) {
	Listen(handle_client, config.Listen, server)
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
	http.HandleFunc("/online", func(w http.ResponseWriter, r *http.Request) {
		router.GetOnlineStatus(w, r, server)
	})
	http.HandleFunc("/all_online", func(w http.ResponseWriter, r *http.Request) {
		router.GetOnlineClients(w, r, server)
	})

	handler := loggingHandler{http.DefaultServeMux}

	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
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
		fmt.Println("usage: im config")
		return
	}

	config = read_route_cfg(flag.Args()[0])

	initLog()

	log.Info("startup...")

	log.Infof("listen:%s\n", config.Listen)

	log.Infof("redis address:%s password:%s db:%d\n",
		config.Redis.Address, config.Redis.Password, config.Redis.Db)

	log.Infof("push disabled:%t", config.PushDisabled)

	log.Infof("log filename:%s level:%s backup:%d age:%d caller:%t",
		config.Log.Filename, config.Log.Level, config.Log.Backup, config.Log.Age, config.Log.Caller)

	redis_pool := NewRedisPool(config.Redis.Address, config.Redis.Password,
		config.Redis.Db)

	if config.HttpListenAddress != "" {
		go StartHttpServer(config.HttpListenAddress)
	}
	server = router.NewServer(redis_pool, config.PushDisabled)
	server.RunPushService()
	ListenClient(server)
}
