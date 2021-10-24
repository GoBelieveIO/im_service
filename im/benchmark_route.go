package main

import "time"
import "flag"
import "log"
import "bytes"

var route_addr string = "127.0.0.1:4444"
var appid int64 = 8

func Dispatch(amsg *RouteMessage) {
	log.Printf("amsg appid:%d receiver:%d", amsg.appid, amsg.receiver)
}
func main() {
	flag.Parse()

	channel1 := NewChannel(route_addr, Dispatch, nil, nil)
	channel1.Start()

	channel1.Subscribe(appid, 1000, true)

	time.Sleep(1*time.Second)

	channel2 := NewChannel(route_addr, Dispatch, nil, nil)
	channel2.Start()

	im := &IMMessage{}
	im.sender = 1
	im.receiver = 1000
	im.content = "test"
	msg := &Message{cmd:MSG_IM, body:im}

	
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, msg)
	msg_buf := mbuffer.Bytes()

	amsg := &RouteMessage{}
	amsg.appid = appid
	amsg.receiver = 1000
	amsg.msg = msg_buf
	channel2.Publish(amsg)

	time.Sleep(3*time.Second)

	channel1.Unsubscribe(appid, 1000, true)

	time.Sleep(1*time.Second)
}
