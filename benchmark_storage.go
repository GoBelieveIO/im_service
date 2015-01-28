package main
import "log"
import "flag"
import "runtime"
import "time"
import "fmt"

var storage_address string = "127.0.0.1:3333"
var appid int64 = 8


var concurrent int
var count int

var c chan bool

func init() {
	flag.IntVar(&concurrent, "c", 10, "concurrent number")
	flag.IntVar(&count, "n", 100000, "request number")
}


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

func benchmark() {
	storage := NewStorageConn()
	err := storage.Dial(storage_address)
	if err != nil {
		log.Println("connect storage err:", err)
		return
	}
	defer storage.Close()

	for i := 0; i < count; i++ {
		im := &IMMessage{sender:1, receiver:1000, content:"1111"}
		m := &Message{cmd:MSG_IM, body:im}
		sae := &SAEMessage{}
		sae.msg = m
		sae.receivers = make([]*AppUserID, 1)
		sae.receivers[0] = &AppUserID{appid:appid, uid:1000}

		_, err := storage.SaveAndEnqueueMessage(sae)
		if err != nil {
			log.Println("saveandequeue message err:", err)
			return
		}
		//log.Println("msgid:", msgid)
	}

	c <- true

}


func main() {
	runtime.GOMAXPROCS(4)
	flag.Parse()

	fmt.Printf("concurrent:%d, request:%d\n", concurrent, count)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	c = make(chan bool, 100)


	begin := time.Now().UnixNano()

	for i := 0; i < concurrent; i++ {
		go benchmark()
	}

	for i := 0; i < concurrent; i++ {
		<- c
	}
	end := time.Now().UnixNano()

	var tps int64 = 0
	if end-begin > 0 {
		tps = int64(1000*1000*1000*concurrent*count) / (end - begin)
	}
	fmt.Println("tps:", tps)
}
