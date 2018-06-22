package main

import "fmt"
import "net"
import "log"
import "runtime"
import "time"
import "flag"
import "strings"
import "io/ioutil"
import "net/http"
import "encoding/base64"
import "crypto/md5"
import "encoding/json"
import "github.com/bitly/go-simplejson"

const HOST = "127.0.0.1"
const PORT = 23000

const APP_ID = 7
const APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
const APP_SECRET = "0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1"
const URL = "http://127.0.0.1:23002"


var concurrent int
var count int
var recv_count int
var c chan bool

func init() {
	flag.IntVar(&concurrent, "c", 10, "concurrent number")
	flag.IntVar(&recv_count, "r", 20, "recv number")
	flag.IntVar(&count, "n", 5000, "request number")
}


func login(uid int64) (string, error) {
	url := URL + "/auth/grant"
	secret := fmt.Sprintf("%x", md5.Sum([]byte(APP_SECRET)))
	s := fmt.Sprintf("%d:%s", APP_ID, secret)
	basic := base64.StdEncoding.EncodeToString([]byte(s))

	v := make(map[string]interface{})
	v["uid"] = uid

	body, _ := json.Marshal(v)

	client := &http.Client{}
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Basic " + basic)
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	res, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	obj, err := simplejson.NewJson(b)
	token, _ := obj.Get("data").Get("token").String()
	return token, nil
}

func recv(uid int64, gid int64, conn *net.TCPConn) {
	seq := 1

	n := count*(concurrent)
	total := n
	for i := 0; i < n; i++ {
		conn.SetDeadline(time.Now().Add(40 * time.Second))
		msg := ReceiveMessage(conn)
		if msg == nil {
			log.Println("receive nill message")
			total = i
			break
		}
	
		if msg.cmd != MSG_GROUP_IM {
			log.Println("mmmmmm:", Command(msg.cmd))
			i--
		}
	
		if msg.cmd == MSG_GROUP_IM {
			//m := msg.body.(*IMMessage)
			//log.Printf("sender:%d receiver:%d content:%s", m.sender, m.receiver, m.content)
		}
		seq++
		ack := &Message{MSG_ACK, seq, DEFAULT_VERSION, 0, &MessageACK{int32(msg.seq)}}
		SendMessage(conn, ack)
	}
	log.Printf("%d received:%d", uid, total)
	c <- true
}

func send(uid int64, gid int64, conn *net.TCPConn) {
	ack_c := make(chan int, 100)
	close_c := make(chan bool)

	seq := 1

	go func() {
		c := count*(concurrent-1)
		total := c
		for i := 0; i < c; i++ {
			conn.SetDeadline(time.Now().Add(40 * time.Second))
			msg := ReceiveMessage(conn)
			if msg == nil {
				log.Println("receive nill message")
				total = i
				break
			}
	 
			if msg.cmd == MSG_ACK {
				i--
				ack_c <- 0
				continue
			}
	 
			if msg.cmd != MSG_GROUP_IM {
				log.Println("mmmmmm:", Command(msg.cmd))
				i--
			}
	 
			if msg.cmd == MSG_GROUP_IM {
				//m := msg.body.(*IMMessage)
				//log.Printf("sender:%d receiver:%d content:%s", m.sender, m.receiver, m.content)
			}
			seq++
			ack := &Message{MSG_ACK, seq, DEFAULT_VERSION, 0, &MessageACK{int32(msg.seq)}}
			SendMessage(conn, ack)
		}
		log.Printf("%d received:%d", uid, total)
		close(close_c)
	}()



	for i := 0; i < count; i++ {
		content := fmt.Sprintf("test....%d", i)
		seq++
		msg := &Message{MSG_GROUP_IM, seq, DEFAULT_VERSION, 0,
			&IMMessage{uid, gid, 0, int32(i), content}}
		SendMessage(conn, msg)
		var e bool
		select {
		case <- ack_c:
		case <- close_c:
			for {
				ack := ReceiveMessage(conn)
				if ack == nil {
					e = true
					break
				}
				if ack.cmd == MSG_ACK {
					break
				}			
			}			
		}
		if e {
			break
		}
	}

	<- close_c

	conn.Close()
	log.Printf("%d send complete", uid)
	c <- true
}


func ConnectServer(uid int64) *net.TCPConn {
	var token string
	for i := 0; i < 2; i++ {
		var err error
		token, err = login(uid)
		if err != nil {
			log.Println("login err:", err)
			continue
		}
	}

	if token == "" {
		panic("")
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
	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{MSG_AUTH_TOKEN, seq, DEFAULT_VERSION, 0, auth})
	ReceiveMessage(conn)

	log.Printf("uid:%d connected\n", uid)
	return conn

}


func main() {
	runtime.GOMAXPROCS(4)
	flag.Parse()

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	log.Printf("concurrent:%d, recv:%d, request:%d\n", concurrent, recv_count, count)

	c = make(chan bool, 100)
	u := int64(13635273140)

	//171(5000)  173(2000)
	gid := int64(171)

	//test_send(u, gid)
	//return




	conns := make([]*net.TCPConn, 0, 1000)

	for i := 0; i < concurrent + recv_count; i++ {

		conn := ConnectServer(u + int64(i))
		conns = append(conns, conn)
		if i%100 == 0 && i > 0 {
			time.Sleep(time.Second*1)
		}
	}
	time.Sleep(time.Second*1)

	fmt.Println("connected")

	begin := time.Now().UnixNano()
	for i := 0; i < concurrent; i++ {
		go send(u+int64(i), gid, conns[i])
	}
	for i := 0; i < recv_count; i++ {
		go recv(u+ int64(concurrent+i) , gid, conns[i+concurrent])
	}

	for i := 0; i < concurrent + recv_count; i++ {
		<-c
	}

	end := time.Now().UnixNano()

	var tps int64 = 0
	if end-begin > 0 {
		tps = int64(1000*1000*1000*concurrent*count) / (end - begin)
	}
	fmt.Println("tps:", tps)
}
