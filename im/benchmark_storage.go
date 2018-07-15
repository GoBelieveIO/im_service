package main
import "log"
import "flag"
import "runtime"
import "time"
import "fmt"
import "github.com/valyala/gorpc"

var storage_address string = "127.0.0.1:13333"
var appid int64 = 8
var device_id int64 = 1

var concurrent int
var count int

var c chan bool

func init() {
	flag.IntVar(&concurrent, "c", 10, "concurrent number")
	flag.IntVar(&count, "n", 100000, "request number")
}

func newRPCClient(addr string) *gorpc.DispatcherClient {
	c := &gorpc.Client{
		Conns: 4,
		Addr: addr,
	}
	c.Start()

	dispatcher := gorpc.NewDispatcher()
	dispatcher.AddFunc("SyncMessage", SyncMessageInterface)
	dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessageInterface)
	dispatcher.AddFunc("SavePeerMessage", SavePeerMessageInterface)
	dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessageInterface)

	dc := dispatcher.NewFuncClient(c)
	return dc
}

func Test_SetAndEnqueue() {

	dc := newRPCClient(storage_address)

	im := &IMMessage{sender:1, receiver:1000, content:"1111"}
	m := &Message{cmd:MSG_IM, body:im}
	
	pm := &PeerMessage{
		AppID:appid,
		Uid:1000,
		DeviceID:device_id,
		Cmd:MSG_IM,
		Raw:m.ToData(),
	}
	
	resp, err := dc.Call("SavePeerMessage", pm)
	if err != nil {
		log.Println("save peer message err:", err)
		return
	}

	msgid := resp.(int64)

	log.Println("insert msgid:%d", msgid)
}

func benchmark() {
	dc := newRPCClient(storage_address)
	
	for i := 0; i < count; i++ {
		im := &IMMessage{sender:1, receiver:1000, content:"1111"}
		m := &Message{cmd:MSG_IM, body:im}

		pm := &PeerMessage{
			AppID:appid,
			Uid:1000,
			DeviceID:device_id,
			Cmd:MSG_IM,
			Raw:m.ToData(),
		}
		
		_, err := dc.Call("SavePeerMessage", pm)
		if err != nil {
			fmt.Println("save peer message err:", err)
			return
		}

		//msgid := resp.(int64)
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
