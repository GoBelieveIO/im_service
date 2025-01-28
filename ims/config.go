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

	"github.com/BurntSushi/toml"
)

// 超级群离线消息数量限制,超过的部分会被丢弃
const GROUP_OFFLINE_LIMIT = 100

// 离线消息返回的数量限制
const OFFLINE_DEFAULT_LIMIT = 3000

const GROUP_OFFLINE_DEFAULT_LIMIT = 0

// unlimit
const OFFLINE_DEFAULT_HARD_LIMIT = 0

type LogConfig struct {
	Filename string `toml:"filename"`
	Level    string `toml:"level"`
	Backup   int    `toml:"backup"` //log files
	Age      int    `toml:"age"`    //days
	Caller   bool   `toml:"caller"`
}

type Config struct {
	RpcListen         string `toml:"rpc_listen"`
	StorageRoot       string `toml:"storage_root"`
	HttpListenAddress string `toml:"http_listen_address"`

	SyncListen    string `toml:"sync_listen"`
	MasterAddress string `toml:"master_address"`
	GroupLimit    int    `toml:"group_limit"` //普通群离线消息的数量限制
	Limit         int    `toml:"limit"`       //单次离线消息的数量限制
	HardLimit     int    `toml:"hard_limit"`  //离线消息总的数量限制

	Log LogConfig `toml:"log"`
}

func read_storage_cfg(cfg_path string) *Config {
	var conf Config
	if _, err := toml.DecodeFile(cfg_path, &conf); err != nil {
		// handle error
		log.Fatal("Decode cfg file fail:", err)
	}
	if conf.Limit == 0 {
		conf.Limit = OFFLINE_DEFAULT_LIMIT
	}
	if conf.GroupLimit == 0 {
		conf.GroupLimit = GROUP_OFFLINE_DEFAULT_LIMIT
	}
	if conf.HardLimit == 0 {
		conf.HardLimit = OFFLINE_DEFAULT_HARD_LIMIT
	}
	return &conf
}
