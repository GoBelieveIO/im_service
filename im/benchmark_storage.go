package main
import "log"
import "flag"
import "runtime"
import "time"
import "fmt"

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

func newRPCClient(addr string) *RPCStorage {
	rpc_storage := NewRPCStorage([]string{addr}, nil)
	return rpc_storage
}

func Test_SetAndEnqueue() {
	dc := newRPCClient(storage_address)
	im := &IMMessage{sender:1, receiver:1000, content:"1111"}
	m := &Message{cmd:MSG_IM, body:im}
	msgid, _, err := dc.SaveMessage(appid, 1000, device_id, m)

	if err != nil {
		log.Println("save peer message err:", err)
		return
	}
	log.Println("insert msgid:%d", msgid)
}

func benchmark() {
	dc := newRPCClient(storage_address)
	
	for i := 0; i < count; i++ {
		im := &IMMessage{sender:1, receiver:1000, content:"1111"}
		m := &Message{cmd:MSG_IM, body:im}

		_, _, err := dc.SaveMessage(appid, 1000, device_id, m)
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
