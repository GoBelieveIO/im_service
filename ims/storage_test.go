package main

import "testing"
import "log"
import "fmt"
import "os"
import "flag"

var storage *Storage
var appid int64 = 0
var device_id int64 = 0
var master *Master
var config *StorageConfig

func init() {
	master = NewMaster()
	master.Start()

	config = &StorageConfig{}
}

func TestMain(m *testing.M) {
	flag.Parse()	
	storage = NewStorage("/tmp")	
	os.Exit(m.Run())
}

func Test_Storage(t *testing.T) {
	im := &IMMessage{sender:1, receiver:2, content:"test"}
	msg := &Message{cmd:MSG_IM, body:im}
	msgid := storage.SaveMessage(msg)
	msg2 := storage.LoadMessage(msgid)
	if msg2 != nil {
		log.Println("msg2 cmd:", Command(msg2.cmd))
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

func Test_Sync(t *testing.T) {
	last_id, _ := storage.GetLastMessageID(appid, 2)
	
	im := &IMMessage{sender:1, receiver:2, content:"test"}
	msg := &Message{cmd:MSG_IM, body:im}
	storage.SavePeerMessage(appid, im.receiver, device_id, msg)
	
	im = &IMMessage{sender:1, receiver:2, content:"test2"}
	msg = &Message{cmd:MSG_IM, body:im}
	storage.SavePeerMessage(appid, im.receiver, device_id, msg)
	
	messages, _, _ := storage.LoadHistoryMessagesV3(appid, im.receiver, last_id, 0, 0)
	latest := messages[0]
	im2 := latest.msg.body.(*IMMessage)
	log.Println("sender:", im2.sender, " receiver:", im2.receiver, " content:", string(im2.content))


	latest = messages[1]
	im2 = latest.msg.body.(*IMMessage)
	log.Println("sender:", im2.sender, " receiver:", im2.receiver, " content:", string(im2.content))
}

func Test_SyncBatch(t *testing.T) {
	receiver := int64(2)
	last_id, _ := storage.GetLastMessageID(appid, receiver)

	for i := 0; i < 5000; i++ {
		content := fmt.Sprintf("test:%d", i)		
		im := &IMMessage{sender:1, receiver:receiver, content:content}
		msg := &Message{cmd:MSG_IM, body:im}
		storage.SavePeerMessage(appid, im.receiver, device_id, msg)
	}

	hasMore := true
	loop := 0
	for hasMore {
		messages, last_msgid, m := storage.LoadHistoryMessagesV3(appid, receiver, last_id, 1000, 4000)
		latest := messages[0]
		im2 := latest.msg.body.(*IMMessage)
		log.Println("loop:", loop, "sender:", im2.sender, " receiver:", im2.receiver, " content:", string(im2.content))
		
		loop++
		last_id = last_msgid
		hasMore = m
	}
}

func Test_PeerIndex(t *testing.T) {
	storage.flushIndex()
}


func Test_NewCount(t *testing.T) {
	receiver := int64(2)
	
	content := "test"
	im := &IMMessage{sender:1, receiver:receiver, content:content}
	msg := &Message{cmd:MSG_IM, body:im}	
	last_id, _ := storage.SavePeerMessage(appid, im.receiver, device_id, msg)
	
	for i := 0; i < 5000; i++ {
		content = fmt.Sprintf("test:%d", i)		
		im = &IMMessage{sender:1, receiver:receiver, content:content}
		msg = &Message{cmd:MSG_IM, body:im}
		storage.SavePeerMessage(appid, im.receiver, device_id, msg)
	}

	count := storage.GetNewCount(appid, receiver, last_id)
	if count != 5000 {
        t.Errorf("new count = %d; expected %d", count, 5000)
 	} else {
		log.Println("last id:", last_id, " new count:", count)
	}

	count = storage.GetNewCount(appid, receiver, 0)
	log.Println("last id:", 0, " new count:", count)	
}
