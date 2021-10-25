package main

import "fmt"
import "net"
import "log"
import "runtime"
import "time"
import "flag"
import "math/rand"
import "github.com/go-redis/redis/v8"


const HOST = "127.0.0.1"
const PORT = 23000
const redis_address = "127.0.0.1"
const redis_password = ""
const redis_db = 0

var redis_client *redis.Client
var seededRand *rand.Rand
var concurrent int
var count int
var c chan bool

func init() {
	flag.IntVar(&concurrent, "c", 10, "concurrent number")
	flag.IntVar(&count, "n", 100000, "request number")
}



func send(uid int64, receiver int64, sem chan int) {
	ip := net.ParseIP(HOST)
	addr := net.TCPAddr{ip, PORT, ""}

	token, err := login(uid)
	if err != nil {
		panic(err)
	}

	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error")
		return
	}
	seq := 1
	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{cmd:MSG_AUTH_TOKEN, seq:seq, version:DEFAULT_VERSION, body:auth})
	ReceiveMessage(conn)

	send_count := 0
	for i := 0; i < count; i++ {
		content := fmt.Sprintf("test....%d", i)
		seq++
		msg := &Message{cmd:MSG_IM, seq:seq, version:DEFAULT_VERSION, flag:0,
			body:&IMMessage{uid, receiver, 0, int32(i), content}}


		select {
		case <- sem:
			break
		case <- time.After(1*time.Second):
			log.Println("wait send sem timeout")			
		}
		
		SendMessage(conn, msg)

		var ack *Message
		for {
			mm := ReceiveMessage(conn)
			if mm == nil {

				break
			}
			if mm.cmd == MSG_ACK {
				ack = mm
				break
			}
		}

		if ack != nil {
			send_count++
		} else {
			log.Println("recv ack error")
			break
		}
	}
	conn.Close()
	c <- true
	log.Printf("%d send complete:%d", uid, send_count)
}

func receive(uid int64, limit int,  sem chan int) {
	sync_key := int64(0)
	
	ip := net.ParseIP(HOST)
	addr := net.TCPAddr{ip, PORT, ""}

	token, err := login(uid)

	if err != nil {
		panic(err)
	}

	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error")
		return
	}
	seq := 1
	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{cmd:MSG_AUTH_TOKEN, seq:seq, version:DEFAULT_VERSION, flag:0, body:auth})
	ReceiveMessage(conn)

	seq++
	ss := &Message{cmd:MSG_SYNC, seq:seq, version:DEFAULT_VERSION, flag:0, body:&SyncKey{sync_key}}
	SendMessage(conn, ss)

	//一次同步的取到的消息数目
	sync_count := 0
	
	recv_count := 0
	syncing := false
	pending_sync := false
	for  {
		if limit > 0 {
			conn.SetDeadline(time.Now().Add(40 * time.Second))
		} else {
			conn.SetDeadline(time.Now().Add(400 * time.Second))			
		}
		
		msg := ReceiveMessage(conn)
		if msg == nil {
			log.Println("receive nill message")
			break
		}

		if msg.cmd == MSG_SYNC_NOTIFY {
			if !syncing {
				seq++
				s := &Message{cmd:MSG_SYNC, seq:seq, version:DEFAULT_VERSION, flag:0, body:&SyncKey{sync_key}}
				SendMessage(conn, s)
				syncing = true
			} else {
				pending_sync = true
			}
		} else if msg.cmd == MSG_IM {
			//m := msg.body.(*IMMessage)
			//log.Printf("sender:%d receiver:%d content:%s", m.sender, m.receiver, m.content)
			
			recv_count += 1
			if limit > 0 && recv_count <= limit {
				select {
				case sem <- 1:
					break
				case <- time.After(10*time.Millisecond):
					log.Println("increment timeout")
				}
			}

			sync_count++
			
			seq++
			ack := &Message{cmd:MSG_ACK, seq:seq, version:DEFAULT_VERSION, flag:0, body:&MessageACK{seq:int32(msg.seq)}}
			SendMessage(conn, ack)			
		} else if msg.cmd == MSG_SYNC_BEGIN {
			sync_count = 0
			//log.Println("sync begin:", recv_count)
		} else if msg.cmd == MSG_SYNC_END {
			syncing = false			
			s := msg.body.(*SyncKey)
			//log.Println("sync end:", recv_count, s.sync_key, sync_key)			
			if s.sync_key > sync_key {
				sync_key = s.sync_key
				//log.Println("sync key:", sync_key)
				seq++
				sk := &Message{cmd:MSG_SYNC_KEY, seq:seq, version:DEFAULT_VERSION, flag:0, body:&SyncKey{sync_key}}
				SendMessage(conn, sk)
			}
			
			if limit < 0 && sync_count == 0 {
				break
			}
			
			if limit > 0 && recv_count >= limit {
				break
			}

			

			if pending_sync {
				seq++
				s := &Message{cmd:MSG_SYNC, seq:seq, version:DEFAULT_VERSION, flag:0, body:&SyncKey{sync_key}}
				SendMessage(conn, s)
				syncing = true
				pending_sync = false
			}
			
		} else {
			log.Println("mmmmmm:", Command(msg.cmd))		
		}
	}
	conn.Close()
	c <- true

	log.Printf("%d received:%d", uid, recv_count)
}

func main() {
	runtime.GOMAXPROCS(4)
	flag.Parse()

	fmt.Printf("concurrent:%d, request:%d\n", concurrent, count)

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	redis_client = NewRedisClient(redis_address, redis_password, redis_db)
	
	c = make(chan bool, 100)
	u := int64(13635273140)

	sems := make([]chan int, concurrent)

	for i := 0; i < concurrent; i++ {
		sems[i] = make(chan int, 2000)
		for j := 0; j < 1000; j++ {
			sems[i] <- 1
		}
	}

	//接受历史离线消息
	for i := 0; i < concurrent; i++ {
		go receive(u + int64(concurrent+i), -1, sems[i])
	}

	for i := 0; i < concurrent; i++ {
		<-c
	}

	time.Sleep(1 * time.Second)

	
	//启动接受者
	for i := 0; i < concurrent; i++ {
		go receive(u + int64(concurrent+i), count, sems[i])
	}
	
	time.Sleep(2 * time.Second)	

	begin := time.Now().UnixNano()
	log.Println("begin test:", begin)
	
	for i := 0; i < concurrent; i++ {
		go send(u+int64(i), u+int64(i+concurrent), sems[i])
	}
	for i := 0; i < 2*concurrent; i++ {
		<-c
	}

	end := time.Now().UnixNano()

	var tps int64 = 0
	if end-begin > 0 {
		tps = int64(1000*1000*1000*concurrent*count) / (end - begin)
	}
	fmt.Println("tps:", tps)
}
