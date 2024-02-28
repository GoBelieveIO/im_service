//go:build exclude

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"runtime"
	"time"

	"github.com/gomodule/redigo/redis"
)

const HOST = "127.0.0.1"
const PORT = 23000
const redis_address = "192.168.33.10:6379"
const redis_password = "123456"
const redis_db = 1

type Param struct {
	concurrent int
	count      int
	recv_count int
	room_id    int
	wait       int
}

var param Param

var send_c chan bool
var recv_c chan bool

var redis_pool *redis.Pool
var seededRand *rand.Rand

func init() {
	flag.IntVar(&param.concurrent, "c", 10, "concurrent number")
	flag.IntVar(&param.recv_count, "r", 20, "recv number")
	flag.IntVar(&param.count, "n", 5000, "request number")
	flag.IntVar(&param.room_id, "i", 10, "room id")
	flag.IntVar(&param.wait, "w", 0, "wait before send")
}

func recv(uid int64, room_id int64, conn *net.TCPConn, count int, concurrent int) {
	seq := 2

	pingTs := time.Now()
	n := count * (concurrent)
	received_num := 0

	for received_num < n {
		now := time.Now()
		d := now.Sub(pingTs)
		if d > time.Duration(3*60)*time.Second {
			seq++
			p := &Message{cmd: MSG_PING, seq: seq, version: DEFAULT_VERSION, flag: 0}
			err := SendMessage(conn, p)
			if err != nil {
				log.Println("send ping err:", err)
			}
			pingTs = now
		}

		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		msg, err := ReceiveServerMessage(conn)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				log.Println("receive message timeout err:", err)
				continue
			}
			log.Println("receive message err:", err)
			break
		}

		if msg.cmd == MSG_ROOM_IM {
			received_num++
			if received_num%10000 == 0 {
				log.Printf("%d received:%d", uid, received_num)
			}
		}
	}
	log.Printf("%d received:%d", uid, received_num)
	recv_c <- true
}

func send(uid int64, room_id int64, conn *net.TCPConn, count int, concurrent int) {
	ack_c := make(chan int, 100)
	close_c := make(chan bool)

	seq := 2

	go func() {
		n := count * (concurrent - 1)
		received_num := 0
		ack_num := 0
		for received_num < n || ack_num < count {
			conn.SetDeadline(time.Now().Add(90 * time.Second))
			msg := ReceiveMessage(conn)
			if msg == nil {
				log.Println("receive nil message")
				break
			}
			if msg.cmd == MSG_ACK {
				ack_num++
				if ack_num%1000 == 0 {
					log.Printf("%d ack:%d", uid, ack_num)
				}
				ack_c <- 0
				continue
			}

			if msg.cmd == MSG_ROOM_IM {
				received_num++
				if received_num%10000 == 0 {
					log.Printf("%d received:%d", uid, received_num)
				}
			}
		}
		log.Printf("%d ack:%d received:%d", uid, ack_num, received_num)
		close(close_c)
	}()

	for i := 0; i < count; i++ {
		begin := time.Now()
		content := fmt.Sprintf("test....%d", i)
		seq++
		msg := &Message{cmd: MSG_ROOM_IM, seq: seq, version: DEFAULT_VERSION, flag: 0,
			body: &RoomMessage{&RTMessage{uid, room_id, content}}}
		SendMessage(conn, msg)

		var e bool
		select {
		case <-ack_c:
			break
		case <-close_c:
			e = true
			break
		}
		if e {
			break
		}

		end := time.Now()
		d := end.Sub(begin)
		if i%100 == 0 {
			log.Printf("send message duration:%d", d)
		}
		if param.wait != 0 {
			if d >= time.Duration(param.wait)*time.Second {
				continue
			} else {
				time.Sleep(time.Duration(param.wait)*time.Second - d)
			}
		}
	}

	<-close_c

	conn.Close()
	log.Printf("%d send complete", uid)
	send_c <- true
}

func ConnectServer(uid int64) *net.TCPConn {
	token, err := login(uid)
	if err != nil {
		panic(err)
	}
	log.Println("login success:", token)

	ip := net.ParseIP(HOST)
	addr := net.TCPAddr{ip, PORT, ""}
	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error:", err)
		return nil
	}
	seq := 1
	auth := &AuthenticationToken{token: token, platform_id: 1, device_id: "00000000"}
	SendMessage(conn, &Message{cmd: MSG_AUTH_TOKEN, seq: seq, version: DEFAULT_VERSION, flag: 0, body: auth})
	ReceiveMessage(conn)

	log.Printf("uid:%d connected\n", uid)
	return conn

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	log.Printf("concurrent:%d, recv:%d, request:%d room id:%d wait:%d\n",
		param.concurrent, param.recv_count, param.count, param.room_id, param.wait)

	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	redis_pool = NewRedisPool(redis_address, redis_password, redis_db)

	send_c = make(chan bool, 100)
	recv_c = make(chan bool, 10000)
	u := int64(1000)

	concurrent := param.concurrent
	recv_count := param.recv_count
	count := param.count
	room_id := int64(param.room_id)

	conns := make([]*net.TCPConn, 0, 1000)
	for i := 0; i < recv_count+concurrent; i++ {
		conn := ConnectServer(u + int64(i))

		var room_body Room = Room(room_id)
		m := &Message{cmd: MSG_ENTER_ROOM, seq: 2, version: DEFAULT_VERSION, flag: 0, body: &room_body}
		SendMessage(conn, m)

		if i < recv_count {
			uid := u + int64(i)
			go recv(uid, room_id, conn, param.count, param.concurrent)
		}

		conns = append(conns, conn)
		if i%500 == 0 && i > 0 {
			time.Sleep(time.Second * 1)
		}
	}

	time.Sleep(time.Second * 2)

	fmt.Println("connected")

	begin := time.Now().UnixNano()
	for i := 0; i < concurrent; i++ {
		uid := u + int64(i+recv_count)
		go send(uid, room_id, conns[i+recv_count], param.count, param.concurrent)
	}

	for i := 0; i < concurrent; i++ {
		<-send_c
	}

	end := time.Now().UnixNano()

	var tps int64 = 0
	if end-begin > 0 {
		tps = int64(1000*1000*1000*concurrent*count) / (end - begin)
	}
	fmt.Println("tps:", tps)

	fmt.Println("waiting recv completed...")
	for i := 0; i < recv_count; i++ {
		<-recv_c
	}
	fmt.Println("recv completed")
}
