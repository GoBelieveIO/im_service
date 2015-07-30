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
var c chan bool

func init() {
	flag.IntVar(&concurrent, "c", 10, "concurrent number")
	flag.IntVar(&count, "n", 100000, "request number")
}


func login(uid int64) string {
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
		return ""
	}
	defer res.Body.Close()
	
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return ""
	}
	obj, err := simplejson.NewJson(b)
	token, _ := obj.Get("data").Get("token").String()
	return token
}

func send(uid int64, receiver int64) {
	ip := net.ParseIP(HOST)
	addr := net.TCPAddr{ip, PORT, ""}

	token := login(uid)

	if token == "" {
		panic("")
	}

	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error")
		return
	}
	seq := 1
	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{MSG_AUTH_TOKEN, seq, DEFAULT_VERSION, auth})
	ReceiveMessage(conn)

	for i := 0; i < count; i++ {
		content := fmt.Sprintf("test....%d", i)
		seq++
		msg := &Message{MSG_IM, seq, DEFAULT_VERSION, &IMMessage{uid, receiver, 0, int32(i), content}}
		SendMessage(conn, msg)
		for {
			ack := ReceiveMessage(conn)
			if ack.cmd == MSG_ACK {
				break
			} else if ack.cmd == MSG_PEER_ACK {
				seq++
				m := &Message{MSG_ACK, seq, DEFAULT_VERSION, &MessageACK{int32(ack.seq)}}
				SendMessage(conn, m)
			}
		}
	}
	conn.Close()
	c <- true
	log.Printf("%d send complete", uid)
}

func receive(uid int64) {
	ip := net.ParseIP(HOST)
	addr := net.TCPAddr{ip, PORT, ""}

	token := login(uid)

	if token == "" {
		panic("")
	}

	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error")
		return
	}
	seq := 1
	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{MSG_AUTH_TOKEN, seq, DEFAULT_VERSION, auth})
	ReceiveMessage(conn)

	total := count
	for i := 0; i < count; i++ {
		conn.SetDeadline(time.Now().Add(40 * time.Second))
		msg := ReceiveMessage(conn)
		if msg == nil {
			log.Println("receive nill message")
			total = i
			break
		}
		if msg.cmd != MSG_IM {
			log.Println("mmmmmm:", Command(msg.cmd))
			i--
		} else {
			//m := msg.body.(*IMMessage)
			//log.Printf("sender:%d receiver:%d content:%s", m.sender, m.receiver, m.content)
		}
		seq++
		ack := &Message{MSG_ACK, seq, DEFAULT_VERSION, &MessageACK{int32(msg.seq)}}
		SendMessage(conn, ack)
	}
	conn.Close()
	c <- true

	log.Printf("%d received:%d", uid, total)
}

func main() {
	runtime.GOMAXPROCS(4)
	flag.Parse()

	fmt.Printf("concurrent:%d, request:%d\n", concurrent, count)

	log.SetFlags(log.Lshortfile | log.LstdFlags)

	c = make(chan bool, 100)
	u := int64(13635273140)

	begin := time.Now().UnixNano()
	for i := 0; i < concurrent; i++ {
		go receive(u + int64(concurrent+i))
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < concurrent; i++ {
		go send(u+int64(i), u+int64(i+concurrent))
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
