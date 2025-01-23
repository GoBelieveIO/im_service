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
	"math/rand"
	"path"
	"runtime"
	"time"

	"github.com/gomodule/redigo/redis"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/GoBelieveIO/im_service/storage"
	"github.com/bitly/go-simplejson"
	"github.com/importcjj/sensitive"
	log "github.com/sirupsen/logrus"
)

var (
	VERSION       string
	BUILD_TIME    string
	GO_VERSION    string
	GIT_COMMIT_ID string
	GIT_BRANCH    string
)

func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2) * time.Second
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

// 过滤敏感词
func FilterDirtyWord(filter *sensitive.Filter, msg *IMMessage) {
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

	if exist, _ := filter.FindIn(text); exist {
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

func SyncKeyService(redis_pool *redis.Pool,
	sync_c chan *storage.SyncHistory,
	group_sync_c chan *storage.SyncGroupHistory) {
	for {
		select {
		case s := <-sync_c:
			origin := GetSyncKey(redis_pool, s.AppID, s.Uid)
			if s.LastMsgID > origin {
				log.Infof("save sync key:%d %d %d", s.AppID, s.Uid, s.LastMsgID)
				SaveSyncKey(redis_pool, s.AppID, s.Uid, s.LastMsgID)
			}
			break
		case s := <-group_sync_c:
			origin := GetGroupSyncKey(redis_pool, s.AppID, s.Uid, s.GroupID)
			if s.LastMsgID > origin {
				log.Infof("save group sync key:%d %d %d %d",
					s.AppID, s.Uid, s.GroupID, s.LastMsgID)
				SaveGroupSyncKey(redis_pool, s.AppID, s.Uid, s.GroupID, s.LastMsgID)
			}
			break
		}
	}
}

func initLog(config *Config) {
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

	config := read_cfg(flag.Args()[0])

	initLog(config)

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

	var low_memory int32 //低内存状态
	sync_c := make(chan *storage.SyncHistory, 100)
	group_sync_c := make(chan *storage.SyncGroupHistory, 100)
	server_summary := NewServerSummary()

	redis_pool := NewRedisPool(config.redis_address, config.redis_password,
		config.redis_db)

	auth := NewAuth(config.auth_method)

	rpc_storage := NewRPCStorage(config.storage_rpc_addrs, config.group_storage_rpc_addrs)

	var group_manager *GroupManager
	if len(config.mysqldb_datasource) > 0 {
		group_manager = NewGroupManager(redis_pool, config.mysqldb_datasource, config.redis_config())
		group_manager.Start()
	}

	app_route := NewAppRoute()
	app := &App{}
	dispatch_app_message := func(m *RouteMessage) {
		DispatchMessage(app_route, m)
	}
	dispatch_room_message := func(m *RouteMessage) {
		DispatchRoomMessage(app_route, m)
	}
	dispatch_group_message := func(m *RouteMessage) {
		DispatchGroupMessage(app, m)
	}
	route_channels := make([]*Channel, 0)
	for _, addr := range config.route_addrs {
		channel := NewChannel(addr, dispatch_app_message, dispatch_group_message, dispatch_room_message)
		channel.Start()
		route_channels = append(route_channels, channel)
	}

	var group_route_channels []*Channel
	if len(config.group_route_addrs) > 0 {
		group_route_channels = make([]*Channel, 0)
		for _, addr := range config.group_route_addrs {
			channel := NewChannel(addr, dispatch_app_message, dispatch_group_message, dispatch_room_message)
			channel.Start()
			group_route_channels = append(group_route_channels, channel)
		}
	} else {
		group_route_channels = route_channels
	}

	group_message_delivers := make([]*GroupMessageDeliver, config.group_deliver_count)
	for i := 0; i < config.group_deliver_count; i++ {
		q := fmt.Sprintf("q%d", i)
		r := path.Join(config.pending_root, q)
		deliver := NewGroupMessageDeliver(r, group_manager, app, rpc_storage)
		deliver.Start()
		group_message_delivers[i] = deliver
	}

	group_loaders := make([]*GroupLoader, config.group_deliver_count)
	for i := 0; i < config.group_deliver_count; i++ {
		loader := NewGroupLoader(group_manager, app_route)
		loader.Start()
		group_loaders[i] = loader
	}

	app.route_channels = route_channels
	app.group_route_channels = group_route_channels
	app.group_message_delivers = group_message_delivers
	app.group_loaders = group_loaders

	var filter *sensitive.Filter
	if len(config.word_file) > 0 {
		filter = sensitive.New()
		filter.LoadWordDict(config.word_file)
	}

	go ListenRedis(app_route, config.redis_config())
	go SyncKeyService(redis_pool, sync_c, group_sync_c)

	if config.memory_limit > 0 {
		go MemStatService(&low_memory, config)
	}

	var relationship_pool *RelationshipPool
	if config.friend_permission || config.enable_blacklist {
		relationship_pool = NewRelationshipPool(config, redis_pool)
		relationship_pool.Start()
	}

	go StartHttpServer(config.http_listen_address, app_route, app, redis_pool, server_summary, rpc_storage)

	listener := &Listener{
		group_manager:     group_manager,
		filter:            filter,
		redis_pool:        redis_pool,
		server_summary:    server_summary,
		relationship_pool: relationship_pool,
		auth:              auth,
		rpc_storage:       rpc_storage,
		sync_c:            sync_c,
		group_sync_c:      group_sync_c,
		low_memory:        &low_memory,
		app_route:         app_route,
		app:               app,
		config:            config,
		server:            NewServer(),
	}
	if len(config.ws_address) > 0 {
		go StartWSServer(config.ws_address, listener)
	}
	if len(config.wss_address) > 0 && len(config.cert_file) > 0 && len(config.key_file) > 0 {
		go StartWSSServer(config.wss_address, config.cert_file, config.key_file, listener)
	}

	if config.ssl_port > 0 && len(config.cert_file) > 0 && len(config.key_file) > 0 {
		go ListenSSL(config.ssl_port, config.cert_file, config.key_file, listener)
	}
	ListenClient(config.port, listener)
	log.Infof("exit")
}
