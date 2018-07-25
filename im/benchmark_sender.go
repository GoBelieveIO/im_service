package main

import "fmt"
import "net"
import "log"
import "runtime"
import "flag"
import "math/rand"
import "net/http"
import "encoding/base64"
import "crypto/md5"
import "strings"
import "encoding/json"
import "github.com/bitly/go-simplejson"
import "io/ioutil"

var first int64
var last int64
var host string
var port int



const APP_ID = 7
const APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
const APP_SECRET = "0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1"
const URL = "http://192.168.33.10:5000"


func init() {
	flag.Int64Var(&first, "first", 0, "first uid")
	flag.Int64Var(&last, "last", 0, "last uid")

	flag.StringVar(&host, "host", "127.0.0.1", "host")
	flag.IntVar(&port, "port", 23000, "port")
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


func send(uid int64) {
	ip := net.ParseIP(host)
	addr := net.TCPAddr{ip, port, ""}

	token := login(uid)
	
	conn, err := net.DialTCP("tcp4", nil, &addr)
	if err != nil {
		log.Println("connect error")
		return
	}
	seq := 1

	auth := &AuthenticationToken{token:token, platform_id:1, device_id:"00000000"}
	SendMessage(conn, &Message{cmd:MSG_AUTH_TOKEN, seq:seq, version:DEFAULT_VERSION, body:auth})	
	ReceiveMessage(conn)

	for i := 0; i < 18000; i++ {
		r := rand.Int63()
		receiver := r%(last-first) + first
		log.Println("receiver:", receiver)
		content := fmt.Sprintf("test....%d", i)
		seq++
		msg := &Message{MSG_IM, seq, DEFAULT_VERSION, 0, &IMMessage{uid, receiver, 0, int32(i), content}}
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
