package main

import "net"
import "log"
import "runtime"
import "time"
import "flag"
import "math/rand"
import "fmt"
import cryptorand "crypto/rand"
import "encoding/base64"
import "github.com/gomodule/redigo/redis"

const APPID = 7
const REDIS_HOST = "127.0.0.1:6379"
const REDIS_PASSWORD = ""
const REDIS_DB = 0


var first int64
var last int64
var local_ip string
var host string
var port int

var redis_pool *redis.Pool

func init() {
	flag.Int64Var(&first, "first", 0, "first uid")
	flag.Int64Var(&last, "last", 0, "last uid")
	flag.StringVar(&local_ip, "local_ip", "0.0.0.0", "local ip")
	flag.StringVar(&host, "host", "127.0.0.1", "host")
	flag.IntVar(&port, "port", 23000, "port")
}


func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2)*time.Second
			c, err := redis.DialTimeout("tcp", server, timeout, 0, 0)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if db > 0 && db < 16 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}



func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := cryptorand.Read(b)
    // Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func GenerateRandomString(s int) (string, error) {
	b, err := GenerateRandomBytes(s)
	return base64.URLEncoding.EncodeToString(b), err
}



func SaveUserAccessToken(token string, appid int64, uid int64) (error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("access_token_%s", token)

	_, err := conn.Do("HMSET", key, "user_id", uid, "app_id", appid)
	if err != nil {
		return err		
	}

	_, err = conn.Do("EXPIRE", key, 60*60)
	return err
}


func receive(uid int64) {
	ip := net.ParseIP(host)
	addr := net.TCPAddr{ip, port, ""}

	lip := net.ParseIP(local_ip)
	laddr := net.TCPAddr{lip, 0, ""}

	conn, err := net.DialTCP("tcp4", &laddr, &addr)
	if err != nil {
		log.Println("connect error")

		return
	}


	token, err := GenerateRandomString(32)
	if err != nil {
		log.Panicln("err:", err)
	}
	
	err = SaveUserAccessToken(token, APPID, uid)
	if err != nil {
		log.Panicln("redis err:", err)
	}
	
	seq := 1
	SendMessage(conn, &Message{MSG_AUTH_TOKEN, seq, DEFAULT_VERSION, 0, &AuthenticationToken{token: token, platform_id:PLATFORM_WEB, device_id:"1"}})
	ReceiveMessage(conn)

	q := make(chan bool, 10)
	wt := make(chan *Message, 10)

	const HEARTBEAT_TIMEOUT = 3 * 60
	go func() {
		msgid := 0
		ticker := time.NewTicker(HEARTBEAT_TIMEOUT * time.Second)
		ticker2 := time.NewTicker(150 * time.Second)
		for {
			select {
			case <-ticker2.C:
				seq++
				msgid++
				receiver := first + rand.Int63()%(last-first)
				im := &IMMessage{uid, receiver, 0, int32(msgid), "test"}
				SendMessage(conn, &Message{MSG_IM, seq, DEFAULT_VERSION, 0, im})
			case <-ticker.C:
				seq++
				SendMessage(conn, &Message{MSG_PING, seq, DEFAULT_VERSION, 0, nil})
			case m := <-wt:
				if m == nil {
					q <- true
					return
				}
				seq++
				m.seq = seq
				SendMessage(conn, m)
			}
		}
	}()

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				wt <- nil
				q <- true
				return
			}

			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				ack := &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
				wt <- ack
			}
		}
	}()

	<-q
	<-q
	conn.Close()
}

func receive_loop(uid int64) {
	for {
		receive(uid)
		n := rand.Int()
		n = n % 20
		time.Sleep(time.Duration(n) * time.Second)
	}
}

func main() {
	runtime.GOMAXPROCS(4)
	rand.Seed(time.Now().Unix())

	flag.Parse()
	log.Printf("first:%d last:%d local ip:%s host:%s port:%d\n",
		first, last, local_ip, host, port)

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	redis_pool = NewRedisPool(REDIS_HOST, REDIS_PASSWORD, REDIS_DB)
	
	c := make(chan bool, 100)
	var i int64
	var j int64

	for i = first; i < last; i += 1000 {
		for j = i; j < i+1000 && j < last; j++ {
			go receive_loop(j)
		}
		time.Sleep(2 * time.Second)
	}

	for i = first; i < last; i++ {
		<-c
	}
}
