package main

import "net/http"
import "encoding/json"
import "os"
import "fmt"
import "net/url"
import "strconv"
import "runtime"
import "runtime/pprof"
import log "github.com/sirupsen/logrus"

type ServerSummary struct {
	nconnections      int64
	nclients          int64
	clientset_count   int64 //重复uid的client对象不计数
	in_message_count  int64
	out_message_count int64
}

func NewServerSummary() *ServerSummary {
	s := new(ServerSummary)
	return s
}


func Summary(rw http.ResponseWriter, req *http.Request) {
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, _ := strconv.ParseInt(m.Get("appid"), 10, 64)

	
	obj := make(map[string]interface{})
	obj["goroutine_count"] = runtime.NumGoroutine()
	obj["connection_count"] = server_summary.nconnections
	obj["client_count"] = server_summary.nclients
	obj["clientset_count"] = server_summary.clientset_count
	obj["in_message_count"] = server_summary.in_message_count
	obj["out_message_count"] = server_summary.out_message_count

	if appid != 0 {
		route := app_route.FindOrAddRoute(appid)
		clientset_count, client_count := route.GetClientCount()

		room_count, room_client_count, room_stat := route.GetRoomCount(1000)
		
		app_obj := make(map[string]interface{})
		app_obj["clientset_count"] = clientset_count
		app_obj["client_count"] = client_count
		app_obj["room_count"] = room_count
		app_obj["room_client_count"] = room_client_count
		app_obj["room_stat"] = room_stat
		k := fmt.Sprintf("app_%d", appid)
		obj[k] = app_obj
	}
	
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
