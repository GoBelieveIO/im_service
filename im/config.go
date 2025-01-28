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
	"log"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/GoBelieveIO/im_service/server"
)

const DEFAULT_GROUP_DELIVER_COUNT = 4

type RedisConfig struct {
	Address  string `toml:"host"`
	Password string `toml:"password"`
	Db       int    `toml:"db"`
}

type LogConfig struct {
	Filename string `toml:"filename"`
	Level    string `toml:"level"`
	Backup   int    `toml:"backup"` //log files
	Age      int    `toml:"age"`    //days
	Caller   bool   `toml:"caller"`
}

type Config struct {
	Port            int    `toml:"port"`
	SslPort         int    `toml:"ssl_port"`
	MySqlDataSource string `toml:"mysqldb_datasource"`
	PendingRoot     string `toml:"pending_root"`

	KefuAppId int64 `toml:"kefu_appid"`

	Redis RedisConfig `toml:"redis"`

	HttpListenAddress string `toml:"http_listen_address"`

	//websocket listen address
	WsAddress string `toml:"ws_address"`

	WssAddress string `toml:"wss_address"`
	CertFile   string `toml:"cert_file"`
	KeyFile    string `toml:"key_file"`

	StorageRpcAddrs     []string `toml:"storage_rpc_addrs"`
	GroupStorageRpcAdrs []string `toml:"group_storage_rpc_addrs"`
	RouteAddrs          []string `toml:"route_addrs"`
	GroupRouteAddrs     []string `toml:"group_route_addrs"` //可选配置项， 超群群的route server

	GroupDeliverCount int    `toml:"group_deliver_count"` //群组消息投递并发数量,默认4
	WordFile          string `toml:"word_file"`           //关键词字典文件
	EnableFriendship  bool   `toml:"enable_friendship"`   //验证好友关系
	EnableBlacklist   bool   `toml:"enable_blacklist"`    //验证是否在对方的黑名单中

	MemoryLimit string `toml:"memory_limit"` //rss超过limit，不接受新的链接

	memory_limit int64

	Log LogConfig `toml:"log"`

	AuthMethod    string `toml:"auth_method"` //jwt or redis
	JwtSigningKey string `toml:"jwt_signing_key"`

	jwt_signing_key []byte
}

func (config *Config) redis_config() *server.RedisConfig {
	return server.NewRedisConfig(config.Redis.Address, config.Redis.Password, config.Redis.Db)
}

func read_cfg(cfg_path string) *Config {
	var conf Config
	if _, err := toml.DecodeFile(cfg_path, &conf); err != nil {
		// handle error
		log.Fatal("Decode cfg file fail:", err)
	}

	mem_limit := strings.TrimSpace(conf.MemoryLimit)
	if mem_limit != "" {
		if strings.HasSuffix(mem_limit, "M") {
			mem_limit = mem_limit[0 : len(mem_limit)-1]
			n, _ := strconv.ParseInt(mem_limit, 10, 64)
			conf.memory_limit = n * 1024 * 1024
		} else if strings.HasSuffix(mem_limit, "G") {
			mem_limit = mem_limit[0 : len(mem_limit)-1]
			n, _ := strconv.ParseInt(mem_limit, 10, 64)
			conf.memory_limit = n * 1024 * 1024 * 1024
		}
	}

	if conf.AuthMethod == "" {
		conf.AuthMethod = "redis"
	}

	conf.jwt_signing_key = []byte(conf.JwtSigningKey)
	return &conf
}

/*
func read_cfg(cfg_path string) *Config {
	config := new(Config)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.port = get_int(app_cfg, "port")
	config.ssl_port = int(get_opt_int(app_cfg, "ssl_port"))
	config.http_listen_address = get_string(app_cfg, "http_listen_address")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.redis_password = get_opt_string(app_cfg, "redis_password")
	db := get_opt_int(app_cfg, "redis_db")
	config.redis_db = int(db)

	config.pending_root = get_string(app_cfg, "pending_root")
	config.mysqldb_datasource = get_opt_string(app_cfg, "mysqldb_source")

	config.ws_address = get_opt_string(app_cfg, "ws_address")

	config.wss_address = get_opt_string(app_cfg, "wss_address")
	config.cert_file = get_opt_string(app_cfg, "cert_file")
	config.key_file = get_opt_string(app_cfg, "key_file")

	config.kefu_appid = get_opt_int(app_cfg, "kefu_appid")

	str := get_string(app_cfg, "storage_rpc_pool")
	array := strings.Split(str, " ")
	config.storage_rpc_addrs = array
	if len(config.storage_rpc_addrs) == 0 {
		log.Fatal("storage pool config")
	}

	str = get_opt_string(app_cfg, "group_storage_rpc_pool")
	if str != "" {
		array = strings.Split(str, " ")
		config.group_storage_rpc_addrs = array
		//check repeat
		for _, addr := range config.group_storage_rpc_addrs {
			for _, addr2 := range config.storage_rpc_addrs {
				if addr == addr2 {
					log.Fatal("stroage and group storage address repeat")
				}
			}
		}
	}

	str = get_string(app_cfg, "route_pool")
	array = strings.Split(str, " ")
	config.route_addrs = array
	if len(config.route_addrs) == 0 {
		log.Fatal("route pool config")
	}

	str = get_opt_string(app_cfg, "group_route_pool")
	if str != "" {
		array = strings.Split(str, " ")
		config.group_route_addrs = array

		//check repeat group_route_addrs and route_addrs
		for _, addr := range config.group_route_addrs {
			for _, addr2 := range config.route_addrs {
				if addr == addr2 {
					log.Fatal("route and group route repeat")
				}
			}
		}
	}

	config.group_deliver_count = int(get_opt_int(app_cfg, "group_deliver_count"))
	if config.group_deliver_count == 0 {
		config.group_deliver_count = DEFAULT_GROUP_DELIVER_COUNT
	}

	config.word_file = get_opt_string(app_cfg, "word_file")
	config.enable_friendship = get_opt_int(app_cfg, "enable_friendship") != 0
	config.enable_blacklist = get_opt_int(app_cfg, "enable_blacklist") != 0

	config.auth_method = get_opt_string(app_cfg, "auth")
	if config.auth_method == "" {
		config.auth_method = "redis"
	}

	if config.auth_method == "jwt" {
		signing_key := get_string(app_cfg, "jwt_signing_key")
		config.jwt_signing_key = []byte(signing_key)
	}

	config.log_filename = get_opt_string(app_cfg, "log_filename")
	config.log_level = get_opt_string(app_cfg, "log_level")
	config.log_backup = int(get_opt_int(app_cfg, "log_backup"))
	config.log_age = int(get_opt_int(app_cfg, "log_age"))
	config.log_caller = get_opt_int(app_cfg, "log_caller") != 0

	mem_limit := get_opt_string(app_cfg, "memory_limit")
	mem_limit = strings.TrimSpace(mem_limit)
	if mem_limit != "" {
		if strings.HasSuffix(mem_limit, "M") {
			mem_limit = mem_limit[0 : len(mem_limit)-1]
			n, _ := strconv.ParseInt(mem_limit, 10, 64)
			config.memory_limit = n * 1024 * 1024
		} else if strings.HasSuffix(mem_limit, "G") {
			mem_limit = mem_limit[0 : len(mem_limit)-1]
			n, _ := strconv.ParseInt(mem_limit, 10, 64)
			config.memory_limit = n * 1024 * 1024 * 1024
		}
	}
	return config
}
*/
