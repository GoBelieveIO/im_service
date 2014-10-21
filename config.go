package main

import "strconv"
import "strings"
import "log"
import "github.com/jimlawless/cfg"
import "net"

type Config struct {
    port int
    storage_root string
    mysqldb_datasource string
    redis_address string
    http_listen_address string
    peer_addrs []*net.TCPAddr
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

func read_cfg(cfg_path string) *Config{
    config := new(Config)
    app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

    config.port = get_int(app_cfg, "port")
    config.storage_root = get_string(app_cfg, "storage_root")
    config.http_listen_address = get_string(app_cfg, "http_listen_address")
    config.redis_address = get_string(app_cfg, "redis_address")
    config.mysqldb_datasource = get_string(app_cfg, "mysqldb_source")

    config.peer_addrs = make([]*net.TCPAddr, 0)
    peers := get_opt_string(app_cfg, "peers")
    if len(peers) > 0 {
        arr := strings.Split(peers, ",")
        for _, item := range arr {
            t := strings.Split(item, ":")
            host := t[0]
            port, _ := strconv.Atoi(t[1])
            ip := net.ParseIP(host)
            addr := &net.TCPAddr{ip, port, ""}
            config.peer_addrs = append(config.peer_addrs, addr)
        }
    }
    return config
}
