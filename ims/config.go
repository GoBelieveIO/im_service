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
import "github.com/richmonkey/cfg"

type StorageConfig struct {
	listen              string
	rpc_listen          string
	storage_root        string
	kefu_appid          int64

	sync_listen         string
	master_address      string
	is_push_system      bool
}

func get_int(app_cfg map[string]string, key string) int64 {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.ParseInt(concurrency, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
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

func read_storage_cfg(cfg_path string) *StorageConfig {
	config := new(StorageConfig)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = get_string(app_cfg, "listen")
	config.rpc_listen = get_string(app_cfg, "rpc_listen")
	config.storage_root = get_string(app_cfg, "storage_root")
	config.kefu_appid = get_int(app_cfg, "kefu_appid")
	config.sync_listen = get_string(app_cfg, "sync_listen")
	config.master_address = get_opt_string(app_cfg, "master_address")
	config.is_push_system = get_opt_int(app_cfg, "is_push_system") == 1
	return config
}

