package main

import "fmt"
import "sync"
import "net/http"
import "encoding/json"
import "os"
import "runtime/pprof"
import log "github.com/golang/glog"

type ServerSummary struct {
    nconnections int64
    nclients int64
    in_message_count int64
    out_message_count int64

    peer_connected map[string]bool
    mutex sync.Mutex
}

func NewServerSummary() *ServerSummary {
    s := new(ServerSummary)
    s.peer_connected = make(map[string]bool)
    return s
}


func (s *ServerSummary) SetPeerConnected(peer string, connected bool) {
    s.mutex.Lock()
    s.mutex.Unlock()
    
    k := fmt.Sprintf("peer_%s_connected", peer)
    s.peer_connected[k] = connected
}

func Summary(rw http.ResponseWriter, req *http.Request) {
    obj := make(map[string]interface{})
    obj["connection_count"] = server_summary.nconnections
    obj["client_count"] = server_summary.nclients
    obj["in_message_count"] = server_summary.in_message_count
    obj["out_message_count"] = server_summary.out_message_count

    server_summary.mutex.Lock()
    for k, v := range server_summary.peer_connected {
        obj[k] = v
    }
    server_summary.mutex.Unlock()

    res, err := json.Marshal(obj)
    if err != nil {
        log.Info("json marshal:", err)
        return
    }

    rw.Header().Add("Content-Type", "application/json")
    _, err = rw.Write(res)
    if err != nil {
        log.Info("write err:", err)
    }
	return
}

func Stack(rw http.ResponseWriter, req *http.Request) {
    pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
    rw.WriteHeader(200)
}

func StartHttpServer(addr string) {
    go func () {
        http.HandleFunc("/summary", Summary)
        http.HandleFunc("/stack", Stack)
        err := http.ListenAndServe(addr, nil)
        log.Info("http server err:", err)
    }()
}
