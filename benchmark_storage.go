package main
import "log"

var storage_address string = "127.0.0.1:3333"
var appid int64 = 8

func Test_SetAndEnqueue() {
	storage := NewStorageConn()
	err := storage.Dial(storage_address)
	if err != nil {
		log.Println("connect storage err:", err)
		return
	}
	defer storage.Close()
	
	im := &IMMessage{sender:1, receiver:1000, content:"1111"}
	m := &Message{cmd:MSG_IM, body:im}
	sae := &SAEMessage{}
	sae.msg = m
	sae.receivers = make([]*AppUserID, 1)
	sae.receivers[0] = &AppUserID{appid:appid, uid:1000}

	msgid, err := storage.SaveAndEnqueueMessage(sae)
	if err != nil {
		log.Println("saveandequeue message err:", err)
		return
	}
	log.Println("msgid:", msgid)

	messages, err := storage.LoadOfflineMessage(appid, 1000)
	if err != nil {
		log.Println("load offline message err:", err)
		return
	}
	for _, emsg := range(messages) {
		im := emsg.msg.body.(*IMMessage)
		log.Printf("message id:%d sender:%d receiver:%d content:%s\n",
			emsg.msgid, im.sender, im.receiver, string(im.content))
	}

	dq := &DQMessage{msgid:msgid, appid:appid, receiver:1000}
	err = storage.DequeueMessage(dq)
	if err != nil {
		log.Println("dequeue err:", err)
	} else {
		log.Printf("dequeue msgid:%d success\n", msgid)
	}
}


func main() {
	Test_SetAndEnqueue()
}
