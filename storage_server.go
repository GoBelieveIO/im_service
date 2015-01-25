package main
import "net"
import "fmt"
import "bytes"
import "time"
import "runtime"
import "flag"
import "encoding/binary"
import log "github.com/golang/glog"

var storage *Storage
var config *StorageConfig
var master *Master

type Client struct {
	conn   *net.TCPConn
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn 
	return client
}

func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) HandleSaveAndEnqueue(sae *SAEMessage) {
	if sae.msg == nil {
		log.Error("sae msg is nil")
		return
	}
	msgid := storage.SaveMessage(sae.msg)
	for _, r := range(sae.receivers) {
		storage.EnqueueOffline(msgid, r.appid, r.uid)
	}
	result := &MessageResult{}
	result.status = 0
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, msgid)
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleDQMessage(dq *DQMessage) {
	storage.DequeueOffline(dq.msgid, dq.appid, dq.receiver)
	result := &MessageResult{status:0}
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) WriteEMessage(emsg *EMessage) []byte{
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	SendMessage(buffer, emsg.msg)
	return buffer.Bytes()
}

func (client *Client) HandleLoadOffline(app_user_id *AppUserID) {
	messages := storage.LoadOfflineMessage(app_user_id.appid, app_user_id.uid)
	result := &MessageResult{status:0}
	buffer := new(bytes.Buffer)
	var count int16
	count = int16(len(messages))
	binary.Write(buffer, binary.BigEndian, count)
	for _, emsg := range(messages) {
		ebuf := client.WriteEMessage(emsg)
		var size int16 = int16(len(ebuf))
		binary.Write(buffer, binary.BigEndian, size)
		buffer.Write(ebuf)
	}
	result.content = buffer.Bytes()
	msg := &Message{cmd:MSG_RESULT, body:result}
	SendMessage(client.conn, msg)
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", msg.cmd)
	switch msg.cmd {
	case MSG_LOAD_OFFLINE:
		client.HandleLoadOffline(msg.body.(*AppUserID))
	case MSG_SAVE_AND_ENQUEUE:
		client.HandleSaveAndEnqueue(msg.body.(*SAEMessage))
	case MSG_DEQUEUE:
		client.HandleDQMessage((*DQMessage)(msg.body.(*OfflineMessage)))
	default:
		log.Warning("unknown msg:", msg.cmd)
	}
}

func (client *Client) Run() {
	go client.Read()
}

func (client *Client) read() *Message {
	return ReceiveMessage(client.conn)
}

func (client *Client) send(msg *Message) {
	SendMessage(client.conn, msg)
}

func (client *Client) close() {
	client.conn.Close()
}

func handle_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(*net.TCPConn), listen_addr string) {
	listen, err := net.Listen("tcp", listen_addr)
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	tcp_listener, ok := listen.(*net.TCPListener)
	if !ok {
		fmt.Println("listen error")
		return
	}

	for {
		client, err := tcp_listener.AcceptTCP()
		if err != nil {
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handle_client, config.listen)
}

func handle_sync_client(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handle_sync_client, config.sync_listen)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config = read_storage_cfg(flag.Args()[0])
	log.Infof("listen:%s storage root:%s sync listen:%s master address:%s\n", 
		config.listen, config.storage_root, config.sync_listen, config.master_address)
	storage = NewStorage(config.storage_root)
	
	master = NewMaster()
	master.Start()
	if len(config.master_address) > 0 {
		slaver := NewSlaver(config.master_address)
		slaver.Start()
	}

	go ListenSyncClient()
	ListenClient()
}
