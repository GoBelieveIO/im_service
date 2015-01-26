package main

import "strconv"
import "log"
import "strings"
import "github.com/jimlawless/cfg"

type Config struct {
	port                int
	mysqldb_datasource  string
	redis_address       string
	http_listen_address string
	socket_io_address   string

	storage_addrs       []string
	route_addrs         []string
}

type StorageConfig struct {
	listen              string
	storage_root        string
	sync_listen         string
	master_address      string
}

type RouteConfig struct {
	listen string
}

type APIConfig struct {
	port  int
	redis_address       string
	mysqldb_datasource  string

	storage_addrs       []string
	route_addrs         []string
}

func get_int(app_cfg map[string]string, key string) int {
	concurrency, present := app_cfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.Atoi(concurrency)
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
	config.http_listen_address = get_string(app_cfg, "http_listen_address")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")
	config.socket_io_address = get_string(app_cfg, "socket_io_address")

	str := get_string(app_cfg, "storage_pool")
    array := strings.Split(str, " ")
	config.storage_addrs = array
	if len(config.storage_addrs) == 0 {
		log.Fatal("storage pool config")
	}

	str = get_string(app_cfg, "route_pool")
    array = strings.Split(str, " ")
	config.route_addrs = array
	if len(config.route_addrs) == 0 {
		log.Fatal("route pool config")
	}

	return config
}

func read_storage_cfg(cfg_path string) *StorageConfig {
	config := new(StorageConfig)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = get_string(app_cfg, "listen")
	config.storage_root = get_string(app_cfg, "storage_root")
	config.sync_listen = get_string(app_cfg, "sync_listen")
	config.master_address = get_opt_string(app_cfg, "master_address")
	return config
}

func read_route_cfg(cfg_path string) *RouteConfig {
	config := new(RouteConfig)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = get_string(app_cfg, "listen")
	return config
}

func read_api_cfg(cfg_path string) *APIConfig {
	config := new(APIConfig)
	app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

	config.port = get_int(app_cfg, "port")
	config.redis_address = get_string(app_cfg, "redis_address")
	config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")

	str := get_string(app_cfg, "storage_pool")
    array := strings.Split(str, " ")
	config.storage_addrs = array
	if len(config.storage_addrs) == 0 {
		log.Fatal("storage pool config")
	}

	str = get_string(app_cfg, "route_pool")
    array = strings.Split(str, " ")
	config.route_addrs = array
	if len(config.route_addrs) == 0 {
		log.Fatal("route pool config")
	}

	return config
}
