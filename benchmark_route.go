package main

import "time"
import "flag"
import log "github.com/golang/glog"

var route_addr string = "127.0.0.1:4444"
var appid int64 = 8

func Dispatch(amsg *AppMessage) {
	log.Infof("amsg appid:%d receiver:%d cmd:%d", amsg.appid, amsg.receiver, amsg.msg.cmd)
}
func main() {
	flag.Parse()

	channel1 := NewChannel(route_addr, Dispatch, nil)
	channel1.Start()

	channel1.Subscribe(appid, 1000)

	time.Sleep(1*time.Second)

	channel2 := NewChannel(route_addr, Dispatch, nil)
	channel2.Start()

	im := &IMMessage{}
	im.sender = 1
	im.receiver = 1000
	im.content = "test"
	msg := &Message{cmd:MSG_IM, body:im}

	amsg := &AppMessage{}
	amsg.appid = appid
	amsg.receiver = 1000
	amsg.msg = msg
	channel2.Publish(amsg)

	time.Sleep(3*time.Second)

	channel1.Unsubscribe(appid, 1000)

	time.Sleep(1*time.Second)
}
