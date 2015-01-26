package main
import "runtime"
import "flag"
import "fmt"
import "time"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"

var channels []*Channel
var config *APIConfig
var group_server *GroupServer
var group_manager *GroupManager
var redis_pool *redis.Pool
var storage_pool *StorageConnPool

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

func DialStorageFun(addr string) func()(*StorageConn, error) {
	f := func() (*StorageConn, error){
		storage := NewStorageConn()
		err := storage.Dial(config.storage_address)
		if err != nil {
			log.Error("connect storage err:", err)
			return nil, err
		}
		return storage, nil
	}
	return f
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im_api config")
		return
	}

	config = read_api_cfg(flag.Args()[0])
	log.Infof("port:%d \n",	config.port)

	redis_pool = NewRedisPool(config.redis_address, "")

	channels = make([]*Channel, 1)
	channels[0] = NewChannel("127.0.0.1:4444", nil)
	channels[0].Start()

	f := DialStorageFun(config.storage_address)
	storage_pool = NewStorageConnPool(100, 500, 600 * time.Second, f) 


	group_manager = NewGroupManager()
	group_manager.Start()

	group_server = NewGroupServer(config.port)
	go group_server.RunPublish()
	group_server.Run()
}
