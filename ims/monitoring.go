package main

import "net/http"
import "encoding/json"
import "os"
import "runtime"
import "runtime/pprof"
import log "github.com/sirupsen/logrus"

type ServerSummary struct {
	nrequests         int64
	peer_message_count int64
	group_message_count  int64
}

func NewServerSummary() *ServerSummary {
	s := new(ServerSummary)
	return s
}


func Summary(rw http.ResponseWriter, req *http.Request) {
	obj := make(map[string]interface{})
	obj["goroutine_count"] = runtime.NumGoroutine()
	obj["request_count"] = server_summary.nrequests
	obj["peer_message_count"] = server_summary.peer_message_count
	obj["group_message_count"] = server_summary.group_message_count	

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

func WriteHttpError(status int, err string, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	meta := make(map[string]interface{})
	meta["code"] = status
	meta["message"] = err
	obj["meta"] = meta
	b, _ := json.Marshal(obj)
	w.WriteHeader(status)
	w.Write(b)
}

func WriteHttpObj(data map[string]interface{}, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = data
	b, _ := json.Marshal(obj)
	w.Write(b)
}
