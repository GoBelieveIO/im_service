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
import "sync"
import "runtime"
import "flag"
import "fmt"
import "time"
import "net/http"
import "math/rand"
import log "github.com/golang/glog"
import "github.com/gomodule/redigo/redis"

var config *RouteConfig
var clients ClientSet
var mutex   sync.Mutex
var redis_pool *redis.Pool
var group_manager *GroupManager

func init() {
	clients = NewClientSet()
}

func AddClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()
	
	clients.Add(client)
}

func RemoveClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	clients.Remove(client)
}

//clone clients
func GetClientSet() ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	s := NewClientSet()

	for c := range(clients) {
		s.Add(c)
	}
	return s
}

func FindClientSet(id *AppUserID) ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppUserID(id) {
			s.Add(c)
		}
	}
	return s
}


func FindRoomClientSet(id *AppRoomID) ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	s := NewClientSet()

	for c := range(clients) {
		if c.ContainAppRoomID(id) {
			s.Add(c)
		}
	}
	return s
}

func IsUserOnline(appid, uid int64) bool {
	mutex.Lock()
	defer mutex.Unlock()

	id := &AppUserID{appid:appid, uid:uid}

	for c := range(clients) {
		if c.IsAppUserOnline(id) {
			return true
		}
	}
	return false
}

func handle_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewClient(conn)
	log.Info("new client:", conn.RemoteAddr())
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

type loggingHandler struct {
	handler http.Handler
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Infof("http request:%s %s %s", r.RemoteAddr, r.Method, r.URL)
	h.handler.ServeHTTP(w, r)
}

func StartHttpServer(addr string) {
	http.HandleFunc("/online", GetOnlineStatus)
	http.HandleFunc("/all_online", GetOnlineClients)
	http.HandleFunc("/get_online_status", GetOnlineStatusX)

	handler := loggingHandler{http.DefaultServeMux}
	
	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}


func main() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config = read_route_cfg(flag.Args()[0])
	log.Infof("listen:%s\n", config.listen)

	log.Infof("redis address:%s password:%s db:%d\n", 
		config.redis_address, config.redis_password, config.redis_db)

	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
		config.redis_db)

	group_manager = NewGroupManager()
	group_manager.Start()

	if len(config.http_listen_address) > 0 {
		go StartHttpServer(config.http_listen_address)
	}
	ListenClient()
}
