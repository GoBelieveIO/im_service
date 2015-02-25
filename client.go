package main

import "net"
import "time"
import "sync"
import "sync/atomic"
import log "github.com/golang/glog"
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
	client.conn = conn // conn is net.Conn or engineio.Conn
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
	route.RemoveClient(client)
	if client.uid > 0 {
		client.RemoveLoginInfo()
		channel := client.GetChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid)
	}
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
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
		client.HandleACK(msg.body.(*MessageACK))
	case MSG_HEARTBEAT:
		// nothing to do
	case MSG_PING:
		client.HandlePing()
	case MSG_INPUTING:
		client.HandleInputing(msg.body.(*MessageInputing))
	case MSG_SUBSCRIBE_ONLINE_STATE:
		client.HandleSubsribe(msg.body.(*MessageSubsribeState))
	case MSG_RT:
		client.HandleRTMessage(msg)
	default:
		log.Info("unknown msg:", msg.cmd)
	}
}

func (client *Client) GetStorageConnPool(uid int64) *StorageConnPool {
	index := uid%int64(len(storage_pools))
	return storage_pools[index]
}

func (client *Client) SendOfflineMessage() {
	storage_pool := client.GetStorageConnPool(client.uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	offline_messages, err := storage.LoadOfflineMessage(client.appid, client.uid)
	if err != nil {
		log.Error("load offline message err:", err)
	}

	log.Info("load offline message count:", len(offline_messages))
	for _, emsg := range offline_messages {
		log.Info("send offline message:", emsg.msgid)
		client.ewt <- emsg
	}
}

func (client *Client) SendEMessage(uid int64, emsg *EMessage) bool {
	channel := client.GetChannel(uid)
	amsg := &AppMessage{appid:client.appid, receiver:uid, 
		msgid:emsg.msgid, msg:emsg.msg}
	channel.Publish(amsg)

	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route, msg cmd:", 
			Command(emsg.msg.cmd))
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
	channel := client.GetChannel(uid)
	amsg := &AppMessage{appid:client.appid, receiver:uid, msgid:0, msg:msg}
	channel.Publish(amsg)

	route := app_route.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route, msg cmd:", Command(msg.cmd))
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

func (client *Client) SendLoginPoint() {
	point := &LoginPoint{}
	point.up_timestamp = int32(client.tm.Unix())
	point.platform_id = client.platform_id
	point.device_id = client.device_id
	msg := &Message{cmd:MSG_LOGIN_POINT, body:point}
	client.SendMessage(client.uid, msg)
}

func (client *Client) AuthToken(token string) (int64, int64, error) {
	appid, uid, _, err := LoadUserAccessToken(token)
	return appid, uid, err
}

func (client *Client) HandleAuthToken(login *AuthenticationToken) {
	var err error
	client.appid, client.uid, err = client.AuthToken(login.token)
	if err != nil {
		log.Info("auth token err:", err)
		msg := &Message{cmd: MSG_AUTH_STATUS, body: &AuthenticationStatus{1}}
		client.wt <- msg
		return
	}
	client.device_id = login.device_id
	client.platform_id = login.platform_id
	client.tm = time.Now()
	log.Infof("auth token:%s appid:%d uid:%d device id:%s", 
		login.token, client.appid, client.uid, client.device_id)

	client.SaveLoginInfo()
	msg := &Message{cmd: MSG_AUTH_STATUS, body: &AuthenticationStatus{0}}
	client.wt <- msg

	client.SendLoginPoint()
	client.SendOfflineMessage()
	client.AddClient()
	channel := client.GetChannel(client.uid)
	channel.Subscribe(client.appid, client.uid)
	atomic.AddInt64(&server_summary.nclients, 1)
}

func (client *Client) GetChannel(uid int64) *Channel{
	index := uid%int64(len(channels))
	return channels[index]
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

	client.SendOfflineMessage()
	client.AddClient()
	channel := client.GetChannel(client.uid)
	channel.Subscribe(client.appid, client.uid)

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

func (client *Client) HandleRTMessage(msg *Message) {
	im := msg.body.(*IMMessage)
	im.timestamp = int32(time.Now().Unix())
	
	m := &Message{cmd:MSG_RT, body:im}
	client.SendMessage(im.receiver, m)

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(msg.seq)}}
	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("realtime message sender:%d receiver:%d", im.sender, im.receiver)
}

func (client *Client) HandleIMMessage(msg *IMMessage, seq int) {
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_IM, body: msg}

	storage_pool := client.GetStorageConnPool(msg.receiver)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	sae := &SAEMessage{}
	sae.msg = m
	sae.receivers = make([]*AppUserID, 1)
	sae.receivers[0] = &AppUserID{appid:client.appid, uid:msg.receiver}

	msgid, err := storage.SaveAndEnqueueMessage(sae)
	if err != nil {
		log.Error("saveandequeue message err:", err)
		return
	}

	emsg := &EMessage{msgid:msgid, msg:m}
	client.SendEMessage(msg.receiver, emsg)

	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}

	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d", msg.sender, msg.receiver)
}

func (client *Client) HandleGroupIMMessage(msg *IMMessage, seq int) {
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MSG_GROUP_IM, body: msg}

	group := group_manager.FindGroup(msg.receiver)
	if group == nil {
		log.Warning("can't find group:", msg.receiver)
		return
	}
	members := group.Members()
	for member := range members {
		//群消息不再发送给自己
		if member == client.uid {
			continue
		}

		storage_pool := client.GetStorageConnPool(member)
		storage, err := storage_pool.Get()
		if err != nil {
			log.Error("connect storage err:", err)
			return
		}
		defer storage_pool.Release(storage)

		sae := &SAEMessage{}
		sae.msg = m
		sae.receivers = make([]*AppUserID, 0, 1)
		id := &AppUserID{appid:client.appid, uid:member}
		sae.receivers = append(sae.receivers, id)
		msgid, err := storage.SaveAndEnqueueMessage(sae)
		if err != nil {
			log.Error("saveandequeue message err:", err)
			return
		}

		emsg := &EMessage{msgid:msgid, msg:m}
		client.SendEMessage(member, emsg)
	}

	
	client.wt <- &Message{cmd: MSG_ACK, body: &MessageACK{int32(seq)}}
	atomic.AddInt64(&server_summary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d", msg.sender, msg.receiver)
}

func (client *Client) HandleInputing(inputing *MessageInputing) {
	msg := &Message{cmd: MSG_INPUTING, body: inputing}
	client.SendMessage(inputing.receiver, msg)
	log.Infof("inputting sender:%d receiver:%d", inputing.sender, inputing.receiver)
}

func (client *Client) HandleACK(ack *MessageACK) {
	log.Info("ack:", ack)
	emsg := client.RemoveUnAckMessage(ack)
	if emsg == nil {
		return
	}

	if (emsg.msgid > 0) {
		client.DequeueMessage(emsg.msgid)
	}

	msg := emsg.msg
	if msg == nil {
		return
	}
	
	if msg.cmd == MSG_IM {
		im := msg.body.(*IMMessage)
		ack := &MessagePeerACK{im.receiver, im.sender, im.msgid}
		m := &Message{cmd: MSG_PEER_ACK, body: ack}

		storage_pool := client.GetStorageConnPool(im.sender)
		storage, err := storage_pool.Get()
		if err != nil {
			log.Error("connect storage err:", err)
			return
		}
		defer storage_pool.Release(storage)
		
		sae := &SAEMessage{}
		sae.msg = m
		sae.receivers = make([]*AppUserID, 1)
		sae.receivers[0] = &AppUserID{appid:client.appid, uid:im.sender}

		msgid, err := storage.SaveAndEnqueueMessage(sae)
		if err != nil {
			log.Error("saveandequeue message err:", err)
			return
		}

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

func (client *Client) DequeueMessage(msgid int64) {
	storage_pool := client.GetStorageConnPool(client.uid)
	storage, err := storage_pool.Get()
	if err != nil {
		log.Error("connect storage err:", err)
		return
	}
	defer storage_pool.Release(storage)

	dq := &DQMessage{msgid:msgid, appid:client.appid, receiver:client.uid}
	err = storage.DequeueMessage(dq)
	if err != nil {
		log.Error("dequeue message err:", err)
	}
}

func (client *Client) RemoveUnAckMessage(ack *MessageACK) *EMessage {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	var msgid int64
	var msg *Message
	var ok bool

	seq := int(ack.seq)
	if msgid, ok = client.unacks[seq]; ok {
		log.Infof("dequeue offline msgid:%d uid:%d\n", msgid, client.uid)
		delete(client.unacks, seq)
	} else {
		log.Warning("can't find msgid with seq:", seq)
	}
	if emsg, ok := client.unackMessages[seq]; ok {
		msg = emsg.msg
		delete(client.unackMessages, seq)
	}

	return &EMessage{msgid:msgid, msg:msg}
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
			if msg.cmd == MSG_RT {
				atomic.AddInt64(&server_summary.out_message_count, 1)
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
			if msg.cmd == MSG_PEER_ACK {
				client.RemoveUnAckMessage(&MessageACK{int32(seq)})
				client.DequeueMessage(emsg.msgid)
			}
		}
	}
}

// 根据连接类型获取消息
func (client *Client) read() *Message {
	if conn, ok := client.conn.(net.Conn); ok {
		conn.SetDeadline(time.Now().Add(CLIENT_TIMEOUT * time.Second))
		return ReceiveMessage(conn)
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		return ReadEngineIOMessage(conn)
	}
	return nil
}

// 根据连接类型发送消息
func (client *Client) send(msg *Message) {
	if conn, ok := client.conn.(net.Conn); ok {
		SendMessage(conn, msg)
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		SendEngineIOMessage(conn, msg)
	}
}

// 根据连接类型关闭
func (client *Client) close() {
	if conn, ok := client.conn.(net.Conn); ok {
		conn.Close()
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		conn.Close()
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}
