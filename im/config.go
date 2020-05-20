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

import "strconv"
import "log"
import "strings"
import "github.com/richmonkey/cfg"

const DEFAULT_GROUP_DELIVER_COUNT = 4

type Config struct {
	port                int
	ssl_port            int
	mysqldb_datasource  string
	pending_root        string
	
	kefu_appid          int64

	redis_address       string
	redis_password      string
	redis_db            int

	http_listen_address string

	//websocket listen address
	ws_address          string
	
	wss_address         string
	cert_file           string
	key_file            string

	storage_rpc_addrs   []string
	group_storage_rpc_addrs   []string	
	route_addrs         []string
	group_route_addrs   []string //可选配置项， 超群群的route server

	group_deliver_count int //群组消息投递并发数量,默认4
	word_file           string //关键词字典文件
	friend_permission   bool //验证好友关系
	enable_blacklist    bool //验证是否在对方的黑名单中

	memory_limit        int64  //rss超过limit，不接受新的链接
}

func get_int(app_cfg map[string]string, key string) int {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.ParseInt(concurrency, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return int(n)
}

func get_opt_int(app_cfg map[string]string, key string) int64 {
	concurrency, present := app_cfg[key]
	if !present {
		return 0
	}
	n, err := strconv.ParseInt(concurrency, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
}


func get_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	return concurrency
}

func get_opt_string(app_cfg map[string]string, key string) string {
	concurrency, present := app_cfg[key]
	if !present {
		return ""
	}
	return concurrency
}

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
	config.friend_permission = get_opt_int(app_cfg, "friend_permission") != 0
	config.enable_blacklist = get_opt_int(app_cfg, "enable_blacklist") != 0
	mem_limit := get_opt_string(app_cfg, "memory_limit")
	mem_limit = strings.TrimSpace(mem_limit)
	if mem_limit != "" {
		if strings.HasSuffix(mem_limit, "M") {
			mem_limit = mem_limit[0:len(mem_limit)-1]
			n, _ := strconv.ParseInt(mem_limit, 10, 64)
			config.memory_limit = n*1024*1024
		} else if strings.HasSuffix(mem_limit, "G") {
			mem_limit = mem_limit[0:len(mem_limit)-1]
			n, _ := strconv.ParseInt(mem_limit, 10, 64)
			config.memory_limit = n*1024*1024*1024
		}
	}
	return config
}
