package main

import "fmt"
import "net"
import "log"
import "runtime"
import "flag"
import "math/rand"

var first int64
var last int64
var host string
var port int

func init() {
	flag.Int64Var(&first, "first", 0, "first uid")
	flag.Int64Var(&last, "last", 0, "last uid")

	flag.StringVar(&host, "host", "127.0.0.1", "host")
	flag.IntVar(&port, "port", 23000, "port")
}

func send(uid int64) {
	ip := net.ParseIP(host)
	addr := net.TCPAddr{ip, port, ""}

	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error")
		return
	}
	seq := 1

	SendMessage(conn, &Message{MSG_AUTH, seq, &Authentication{uid: uid}})
	ReceiveMessage(conn)

	for i := 0; i < 18000; i++ {
		r := rand.Int63()
		receiver := r%(last-first) + first
		log.Println("receiver:", receiver)
		content := fmt.Sprintf("test....%d", i)
		seq++
		msg := &Message{MSG_IM, seq, &IMMessage{uid, receiver, 0, int32(i), content}}
		SendMessage(conn, msg)
		for {
			ack := ReceiveMessage(conn)
			if ack.cmd == MSG_ACK {
				break
			}
		}
	}
	conn.Close()
	log.Printf("%d send complete", uid)
}

func main() {
	runtime.GOMAXPROCS(4)
	flag.Parse()
	fmt.Printf("first:%d, last:%d\n", first, last)
	if last <= first {
		return
	}
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	send(1)
}
