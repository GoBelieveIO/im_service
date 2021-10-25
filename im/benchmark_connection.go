package main

import "net"
import "log"
import "runtime"
import "time"
import "flag"
import "math/rand"
import "github.com/go-redis/redis/v8"

const APPID = 7
const REDIS_HOST = "127.0.0.1:6379"
const REDIS_PASSWORD = ""
const REDIS_DB = 0


var first int64
var last int64
var local_ip string
var host string
var port int

var seededRand *rand.Rand
var redis_client *redis.Client

func init() {
	flag.Int64Var(&first, "first", 0, "first uid")
	flag.Int64Var(&last, "last", 0, "last uid")
	flag.StringVar(&local_ip, "local_ip", "0.0.0.0", "local ip")
	flag.StringVar(&host, "host", "127.0.0.1", "host")
	flag.IntVar(&port, "port", 23000, "port")
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

	token, err := login(uid)
	
	seq := 1
	SendMessage(conn, &Message{cmd:MSG_AUTH_TOKEN, seq:seq, version:DEFAULT_VERSION, flag:0, body:&AuthenticationToken{token: token, platform_id:PLATFORM_WEB, device_id:"1"}})
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
				SendMessage(conn, &Message{cmd:MSG_IM, seq:seq, version:DEFAULT_VERSION, flag:0, body:im})
			case <-ticker.C:
				seq++
				SendMessage(conn, &Message{cmd:MSG_PING, seq:seq, version:DEFAULT_VERSION, flag:0, body:nil})
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
				ack := &Message{cmd: MSG_ACK, body: &MessageACK{seq:int32(msg.seq)}}
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

	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	redis_client = NewRedisClient(REDIS_HOST, REDIS_PASSWORD, REDIS_DB)
	
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
