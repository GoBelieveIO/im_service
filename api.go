package main
import "runtime"
import "flag"
import "fmt"
import "time"
import "net/http"
import "math/rand"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "github.com/gorilla/mux"

var config *APIConfig
var group_server *GroupServer
var redis_pool *redis.Pool


func NewRedisPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}


func RunAPI() {
	r := mux.NewRouter()
	r.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleCreate(w, r)
	}).Methods("POST")

	r.HandleFunc("/groups/{gid}", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleDisband(w, r)
	}).Methods("DELETE")

	r.HandleFunc("/groups/{gid}/members", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleAddGroupMember(w, r)
	}).Methods("POST")

	r.HandleFunc("/groups/{gid}/members/{mid}", func(w http.ResponseWriter, r *http.Request) {
		group_server.HandleQuitGroup(w, r)
	}).Methods("DELETE")

	r.HandleFunc("/device/bind", BindToken).Methods("POST")
	r.HandleFunc("/device/unbind", UnbindToken).Methods("POST")
	r.HandleFunc("/auth/grant", AuthGrant).Methods("POST")
	http.Handle("/", r)

	var PORT = config.port
	var BIND_ADDR = ""
	addr := fmt.Sprintf("%s:%d", BIND_ADDR, PORT)
	SingleHTTPService(addr, nil)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im_api config")
		return
	}

	config = read_api_cfg(flag.Args()[0])
	log.Infof("port:%d \n",	config.port)

	redis_pool = NewRedisPool(config.redis_address, "")

	group_server = NewGroupServer(config.port)
	go group_server.RunPublish()

	RunAPI()
}
