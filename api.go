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
import "runtime"
import "flag"
import "fmt"
import "time"
import "net/http"
import "math/rand"
import "os"
import "net/url"
import "strconv"
import "strings"
import "io/ioutil"
import "errors"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "github.com/gorilla/mux"
import "github.com/gorilla/handlers"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

var config *APIConfig
var group_server *GroupServer
var redis_pool *redis.Pool
var db *sql.DB

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


func SendIMMessage(body string, appid int64, msg_type string, sender int64) error {
	url := fmt.Sprintf("%s/post_im_message?appid=%d&class=%s&sender=%d", config.im_url, appid, msg_type, sender)
	resp, err := http.Post(url, "application/json", strings.NewReader(string(body)))
	if err != nil {
		log.Info("post err:", err)
		return err
	}

	resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("server internal error")
	}
	log.Info("send im message success")
	return nil
}

func PostIMMessage(w http.ResponseWriter, r *http.Request) {
	var appid int64
	var uid int64
	var err error

	appid, err = BasicAuthorization(r)
	if err != nil {
		appid, uid, err = BearerAuthentication(r)
		if err != nil {
			WriteHttpError(403, err.Error(), w)
			return
		}
	}
	
	vars := mux.Vars(r)
	class, ok := vars["class"]
	if !ok {
		WriteHttpError(400, "message class is empty", w)
		return
	}

	var msg_type string
	if class == "groups" {
		msg_type = "group"
	} else if class == "peers" {
		msg_type = "peer"
	} else {
		WriteHttpError(400, "invalid message class", w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	err = SendIMMessage(string(body), appid, msg_type, uid)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	w.WriteHeader(200)
}

func LoadLatestMessage(w http.ResponseWriter, r *http.Request) {
	var appid int64
	var uid int64
	var err error

	appid, uid, err = BearerAuthentication(r)
	if err != nil {
		WriteHttpError(403, err.Error(), w)
		return
	}
	m, _ := url.ParseQuery(r.URL.RawQuery)

	limit := int64(1024)
	if len(m.Get("limit")) > 0 {
		limit, err = strconv.ParseInt(m.Get("limit"), 10, 32)
		if err != nil {
			log.Info("error:", err)
			WriteHttpError(400, "invalid json format", w)
			return
		}
	}

	url := fmt.Sprintf("%s/load_latest_message?appid=%d&uid=%d&limit=%d", config.im_url, appid, uid, limit)
	resp, err := http.Get(url)
	if err != nil {
		log.Info("post err:", err)
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil || resp.StatusCode != 200 {
		WriteHttpError(400, "server internal error", w)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func RunAPI() {
	r := mux.NewRouter()
	r.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleCreate(w, r)
	}).Methods("POST")

	r.HandleFunc("/groups/{gid}", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleDisband(w, r)
	}).Methods("DELETE")

	r.HandleFunc("/groups/{gid}/members", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleAddGroupMember(w, r)
	}).Methods("POST")

	r.HandleFunc("/groups/{gid}/members/{mid}", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleQuitGroup(w, r)
	}).Methods("DELETE")

	r.HandleFunc("/device/bind", BindToken).Methods("POST")
	r.HandleFunc("/device/unbind", UnbindToken).Methods("POST")
	r.HandleFunc("/notification/groups/{gid}", SetGroupQuiet).Methods("POST")
	r.HandleFunc("/auth/grant", AuthGrant).Methods("POST")
	r.HandleFunc("/messages/{class}", PostIMMessage).Methods("POST")
	r.HandleFunc("/messages", LoadLatestMessage).Methods("GET")

	http.Handle("/", handlers.LoggingHandler(os.Stdout, r))

	var PORT = config.port
	var BIND_ADDR = ""
	addr := fmt.Sprintf("%s:%d", BIND_ADDR, PORT)
	SingleHTTPService(addr, nil)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im_api config")
		return
	}

	config = read_api_cfg(flag.Args()[0])
	log.Infof("port:%d \n",	config.port)

	var err error
	db, err = sql.Open("mysql", config.appdb_datasource)
	if err != nil {
		log.Info("mysql open err:", err)
	} else {
		//use short connection
		db.SetMaxOpenConns(100)
		db.SetMaxIdleConns(0)
	}

	redis_pool = NewRedisPool(config.redis_address, "")

	group_server = NewGroupServer(config.port)
	go group_server.RunPublish()

	RunAPI()
}
