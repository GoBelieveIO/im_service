package main

import "net"
import "time"
import "fmt"
import "sync"
import "sync/atomic"
import "encoding/json"
import log "github.com/golang/glog"
import "github.com/garyburd/redigo/redis"
import "github.com/googollee/go-engine.io"

const CLIENT_TIMEOUT = (60 * 6)

type Client struct {
	tm     time.Time
	wt     chan *Message
	ewt    chan *EMessage

	appid  int64
	uid    int64
	device_id string
	platform_id int8
	conn   interface{}

	unackMessages map[int]*EMessage
	unacks map[int]int64
	mutex  sync.Mutex
}

func NewClient(conn interface{}) *Client {
	client := new(Client)
	client.conn = conn // conn is *net.TCPConn or engineio.Conn
	client.wt = make(chan *Message, 10)
	client.ewt = make(chan *EMessage, 10)

	client.unacks = make(map[int]int64)
	client.unackMessages = make(map[int]*EMessage)
	atomic.AddInt64(&server_summary.nconnections, 1)
	return client
}

func (client *Client) Read() {
	for {
		msg := client.read()
		if msg == nil {
			client.HandleRemoveClient()
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) HandleRemoveClient() {
	client.wt <- nil
	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return
	}
	r := route.RemoveClient(client)
	if client.uid > 0 && r {

	}
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg:", msg.cmd)
	switch msg.cmd {
	case MSG_AUTH:
		client.HandleAuth(msg.body.(*Authentication))
	case MSG_AUTH_TOKEN:
		client.HandleAuthToken(msg.body.(*AuthenticationToken))
	case MSG_IM:
		client.HandleIMMessage(msg.body.(*IMMessage), msg.seq)
	case MSG_GROUP_IM:
		client.HandleGroupIMMessage(msg.body.(*IMMessage), msg.seq)
	case MSG_ACK:
		client.HandleACK(msg.body.(MessageACK))
	case MSG_HEARTBEAT:
		// nothing to do
	case MSG_PING:
		client.HandlePing()
	case MSG_INPUTING:
		client.HandleInputing(msg.body.(*MessageInputing))
	case MSG_SUBSCRIBE_ONLINE_STATE:
		client.HandleSubsribe(msg.body.(*MessageSubsribeState))
	default:
		log.Info("unknown msg:", msg.cmd)
	}
}

func (client *Client) SendOfflineMessage() {
	offline_messages := storage.LoadOfflineMessage(client.uid)
	for _, emsg := range offline_messages {
		client.ewt <- emsg
	}
}

func (client *Client) SendEMessage(uid int64, emsg *EMessage) bool {
	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return false
	}
	clients := route.FindClientSet(uid)
	if clients != nil || clients.Count() > 0 {
		for c, _ := range(clients) {
			c.ewt <- emsg
		}
		return true
	}
	return false
}

func (client *Client) SendMessage(uid int64, msg *Message) bool {
	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return false
	}
	clients := route.FindClientSet(uid)
	if clients != nil {
		for c, _ := range(clients) {
			c.wt <- msg
		}
		return true
	}
	return false
}

func (client *Client) PlatformString(platform_id int8) string {
	var platform string
	if platform_id == PLATFORM_IOS {
		platform = "ios"
	} else if platform_id == PLATFORM_ANDROID {
		platform = "android"
	} else if platform_id == PLATFORM_WEB {
		platform = "web"
	} else {
		platform = "unknown"
	}
	return platform
}

func (client *Client) SaveLoginInfo() {
	conn := redis_pool.Get()
	defer conn.Close()

	var platform_id int8 = client.platform_id
	var device_id string = client.device_id

	platform := client.PlatformString(platform_id)

	key := fmt.Sprintf("user_logins_%d_%d", client.appid, client.uid)
	value := fmt.Sprintf("%s_%s", platform, device_id)
	_, err := conn.Do("SADD", key, value)
	if err != nil {
		log.Warning("sadd err:", err)
	}
	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)

	key = fmt.Sprintf("user_logins_%d_%d_%s_%s", client.appid, client.uid, platform, device_id)
	_, err = conn.Do("HMSET", key, "up_timestamp", client.tm.Unix(), "platform", platform, "device_id", device_id)
	if err != nil {
		log.Warning("hset err:", err)
	}

	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)
}

func (client *Client) RefreshLoginInfo() {
	conn := redis_pool.Get()
	defer conn.Close()

	platform := client.PlatformString(client.platform_id)

	key := fmt.Sprintf("user_logins_%d_%d", client.appid, client.uid)

	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)

	key = fmt.Sprintf("user_logins_%d_%d_%s_%s", client.appid, client.uid, platform, client.device_id)

	conn.Do("EXPIRE", key, CLIENT_TIMEOUT*2)	
}

func (client *Client) AuthToken(login *AuthenticationToken) (int64, int64, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("tokens_%s", login.token)

	var uid int64
	var appid int64
	
	reply, err := redis.Values(conn.Do("HMGET", key, "uid", "app_id"))
	if err != nil {
		log.Info("hmget error:", err)
		return 0, 0, err
	}

	_, err = redis.Scan(reply, &uid, &appid)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, 0, err
	}
	return appid, uid, nil
}

func (client *Client) HandleAuthToken(login *AuthenticationToken) {
	var err error
	client.appid, client.uid, err = client.AuthToken(login)
	if err != nil {
		log.Info("auth token err:", err)
		msg := &Message{cmd: MSG_AUTH_STATUS, body: &AuthenticationStatus{1}}
		client.wt <- msg
		return
	}
	client.device_id = login.device_id
	client.platform_id = login.platform_id
	client.tm = time.Now()
	log.Infof("auth token:%s appid:%d uid:%d", 
		login.token, client.appid, client.uid)

	client.SaveLoginInfo()
	msg := &Message{cmd: MSG_AUTH_STATUS, body: &AuthenticationStatus{0}}
	client.wt <- msg

	client.AddClient()
	client.SendOfflineMessage()

	atomic.AddInt64(&server_summary.nclients, 1)
}

func (client *Client) HandleAuth(login *Authentication) {
	client.appid = 0
	client.uid = login.uid
	client.device_id = "00000000"
	client.platform_id = PLATFORM_IOS
	client.tm = time.Now()
	log.Info("auth:", login.uid)

	client.SaveLoginInfo()
	msg := &Message{cmd: MSG_AUTH_STATUS, body: &AuthenticationStatus{0}}
	client.wt <- msg

	client.AddClient()
	client.SendOfflineMessage()

	atomic.AddInt64(&server_summary.nclients, 1)
}

func (client *Client) AddClient() {
	route := app_route.FindOrAddRoute(client.appid)
	route.AddClient(client)
}

func (client *Client) HandleSubsribe(msg *MessageSubsribeState) {
	if client.uid == 0 {
		return
	}

	//todo 获取在线状态
	for _, uid := range msg.uids {
		state := &MessageOnlineState{uid, 0}
		m := &Message{cmd: MSG_ONLINE_STATE, body: state}
		client.wt <- m
	}
}

//离线消息入apns队列
func (client *Client) PublishPeerMessage(im *IMMessage) {
	conn := redis_pool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	_, err := conn.Do("RPUSH", "push_queue", b)
	if err != nil {
		log.Info("rpush error:", err)
	}
}

func (client *Client) HandleIMMessage(msg *IMMessage, seq int) {
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_IM, body: msg}

	msgid := storage.SaveMessage(m)
	storage.EnqueueOffline(msgid, msg.receiver)
	
	emsg := &EMessage{msgid:msgid, msg:m}
	r := client.SendEMessage(msg.receiver, emsg)
	if !r {
		client.PublishPeerMessage(msg)
	}

	client.wt <- &Message{cmd: MSG_ACK, body: MessageACK(seq)}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d", msg.sender, msg.receiver)
}

func (client *Client) HandleGroupIMMessage(msg *IMMessage, seq int) {
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_GROUP_IM, body: msg}
	msgid := storage.SaveMessage(m)

	group := group_manager.FindGroup(msg.receiver)
	if group == nil {
		log.Info("can't find group:", msg.receiver)
		return
	}

	for member := range group.Members() {
		//群消息不再发送给自己
		if member == client.uid {
			continue
		}

		storage.EnqueueOffline(msgid, member)

		emsg := &EMessage{msgid:msgid, msg:m}
		client.SendEMessage(member, emsg)
	}
	
	client.wt <- &Message{cmd: MSG_ACK, body: MessageACK(seq)}
	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d", msg.sender, msg.receiver)
}

func (client *Client) HandleInputing(inputing *MessageInputing) {
	msg := &Message{cmd: MSG_INPUTING, body: inputing}
	client.SendMessage(inputing.receiver, msg)
	log.Infof("inputting sender:%d receiver:%d", inputing.sender, inputing.receiver)
}

func (client *Client) HandleACK(ack MessageACK) {
	log.Info("ack:", ack)
	msg := client.RemoveUnAckMessage(ack)
	if msg == nil {
		return
	}
	
	if msg.cmd == MSG_IM {
		im := msg.body.(*IMMessage)
		ack := &MessagePeerACK{im.receiver, im.sender, im.msgid}
		m := &Message{cmd: MSG_PEER_ACK, body: ack}
		msgid := storage.SaveMessage(m)
		storage.EnqueueOffline(msgid, im.sender)

		emsg := &EMessage{msgid:msgid, msg:m}
		client.SendEMessage(im.sender, emsg)
	}
}

func (client *Client) HandlePing() {
	m := &Message{cmd: MSG_PONG}
	client.wt <- m
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
	client.RefreshLoginInfo()
}

func (client *Client) RemoveUnAckMessage(ack MessageACK) *Message {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	seq := int(ack)
	if msgid, ok := client.unacks[seq]; ok {
		log.Infof("dequeue offline msgid:%d uid:%d\n", msgid, client.uid)
		storage.DequeueOffline(msgid, client.uid)
		delete(client.unacks, seq)
	}
	if emsg, ok := client.unackMessages[seq]; ok {
		msg := emsg.msg
		delete(client.unackMessages, seq)
		return msg
	}
	return nil
}

func (client *Client) AddUnAckMessage(emsg *EMessage) {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	seq := emsg.msg.seq
	client.unacks[seq] = emsg.msgid
	if emsg.msg.cmd == MSG_IM {
		client.unackMessages[seq] = emsg
	}
}

func (client *Client) Write() {
	seq := 0
	running := true
	for running {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				atomic.AddInt64(&server_summary.nconnections, -1)
				if client.uid > 0 {
					atomic.AddInt64(&server_summary.nclients, -1)
				}
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			seq++
			msg.seq = seq
			client.send(msg)
		case emsg := <- client.ewt:
			msg := emsg.msg
			seq++
			msg.seq = seq
			client.AddUnAckMessage(emsg)
			if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
				atomic.AddInt64(&server_summary.out_message_count, 1)
			}
			client.send(msg)
		}
	}
}

// 根据连接类型获取消息
func (client *Client) read() *Message {
	if conn, ok := client.conn.(*net.TCPConn); ok {
		conn.SetDeadline(time.Now().Add(CLIENT_TIMEOUT * time.Second))
		return ReceiveMessage(conn)
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		return ReadEngineIOMessage(conn)
	}
	return nil
}

// 根据连接类型发送消息
func (client *Client) send(msg *Message) {
	if conn, ok := client.conn.(*net.TCPConn); ok {
		SendMessage(conn, msg)
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		SendEngineIOMessage(conn, msg)
	}
}

// 根据连接类型关闭
func (client *Client) close() {
	if conn, ok := client.conn.(*net.TCPConn); ok {
		conn.Close()
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		conn.Close()
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}
