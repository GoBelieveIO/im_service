package main

import "testing"
import "log"

var storage = NewStorage("/tmp")
var appid int64 = 0
var device_id int64 = 0
var master *Master
var config *StorageConfig

func init() {
	master = NewMaster()
	master.Start()

	config = &StorageConfig{}
}

func Test_Storage(t *testing.T) {
	im := &IMMessage{sender:1, receiver:2, content:"test"}
	msg := &Message{cmd:MSG_IM, body:im}
	msgid := storage.SaveMessage(msg)
	msg2 := storage.LoadMessage(msgid)
	if msg2 != nil {
		log.Println("msg2 cmd:", msg2.cmd)
	} else {
		log.Println("can't load msg:", msgid)
	}
}
 

func Test_LoadLatest(t *testing.T) {
	im := &IMMessage{sender:1, receiver:2, content:"test"}
	msg := &Message{cmd:MSG_IM, body:im}
	storage.SavePeerMessage(appid, im.receiver, device_id, msg)
	
	im = &IMMessage{sender:1, receiver:2, content:"test2"}
	msg = &Message{cmd:MSG_IM, body:im}
	storage.SavePeerMessage(appid, im.receiver, device_id, msg)

	messages := storage.LoadLatestMessages(appid, im.receiver, 2)
	latest := messages[0]
	im2 := latest.msg.body.(*IMMessage)
	log.Println("sender:", im2.sender, " receiver:", im2.receiver, " content:", string(im2.content))


	latest = messages[1]
	im2 = latest.msg.body.(*IMMessage)
	log.Println("sender:", im2.sender, " receiver:", im2.receiver, " content:", string(im2.content))

}
