package main

import "strconv"
import "strings"
import "log"
import "net"
import "fmt"
import "os"
import "github.com/jimlawless/cfg"
import "runtime"

var route *Route
var cluster *Cluster 
var storage *Storage
var group_manager *GroupManager
var group_server *GroupServer
var state_center *StateCenter

var STORAGE_ROOT = "/tmp"
var PORT = 23000
var MYSQLDB_DATASOURCE = ""
var REDIS_ADDRESS = ""
var PEER_ADDRS []*net.TCPAddr

func init() {
    route = NewRoute()
    state_center = NewStateCenter()
    PEER_ADDRS = make([]*net.TCPAddr, 0)
}

func handle_client(conn *net.TCPConn) {
    client := NewClient(conn)
    client.Run()
}

func handle_peer_client(conn *net.TCPConn) {
    client := NewPeerClient(conn)
    client.Run()
}

func read_cfg(cfg_path string) {
    app_cfg := make(map[string]string)
	err := cfg.Load(cfg_path, app_cfg)
	if err != nil {
		log.Fatal(err)
	}

    port, present := app_cfg["port"]
    if !present {
        fmt.Println("need config listen port")
        os.Exit(1)
    }
    nport, err := strconv.Atoi(port)
    if err != nil {
        fmt.Println("need config listen port")
        os.Exit(1)
    }
    PORT = nport
    fmt.Println("port:", PORT)
    if db_src, present := app_cfg["mysqldb_source"]; present {
        MYSQLDB_DATASOURCE = db_src
    } else {
        os.Exit(1)
    }

    if addr, present := app_cfg["redis_address"]; present {
        REDIS_ADDRESS = addr
    } else {
        os.Exit(1)
    }
    
    root, present := app_cfg["storage_root"]
    if present {
        STORAGE_ROOT = root
    }
    fmt.Println("storage root:", STORAGE_ROOT)
    peers, present := app_cfg["peers"]
    if !present {
        return
    }
    arr := strings.Split(peers, ",")
    for _, item := range arr {
        t := strings.Split(item, ":")
        host := t[0]
        port, _ := strconv.Atoi(t[1])
        ip := net.ParseIP(host)
        addr := &net.TCPAddr{ip, port, ""}
        PEER_ADDRS = append(PEER_ADDRS, addr)
    }

    fmt.Println("addrs:", PEER_ADDRS)
}

func Listen(f func(*net.TCPConn), port int) {
	ip := net.ParseIP("0.0.0.0")
	addr := net.TCPAddr{ip, port, ""}

	listen, err := net.ListenTCP("tcp", &addr);
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	for {
		client, err := listen.AcceptTCP();
		if err != nil {
			return
		}
		f(client)
	}
    
}
func ListenClient() {
    Listen(handle_client, PORT)
}

func ListenPeerClient() {
    Listen(handle_peer_client, PORT + 1)
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile|log.LstdFlags)
    if len(os.Args) < 2 {
        fmt.Println("usage: im config")
        return
    }
    read_cfg(os.Args[1])
    cluster = NewCluster(PEER_ADDRS)
    cluster.Start()
    storage = NewStorage(STORAGE_ROOT)
    storage.Start()
    group_server = NewGroupServer(PORT+2)
    group_server.Start()
    group_manager = NewGroupManager()
    group_manager.Start()

    go ListenPeerClient()
    ListenClient()
}
