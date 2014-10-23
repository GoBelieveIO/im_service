package main
import "net"
import "time"
import "fmt"
import "sync"
import "sync/atomic"
import log "github.com/golang/glog"

const CLIENT_TIMEOUT = (60*6)
type Client struct {
    tm time.Time
    wt chan *Message
    uid int64
    conn *net.TCPConn
    unacks []*Message
    mutex sync.Mutex
}

func NewClient(conn *net.TCPConn) *Client {
    client := new(Client)
    client.conn = conn
    client.wt = make(chan *Message, 10)
    client.unacks = make([]*Message, 0, 4)
    atomic.AddInt64(&server_summary.nconnections, 1)
    return client
}

func (client *Client) Read() {
    for {
        client.conn.SetDeadline(time.Now().Add(CLIENT_TIMEOUT*time.Second))
        msg := ReceiveMessage(client.conn)
        if msg == nil {
            route.RemoveClient(client)
            if client.uid > 0 {
                cluster.RemoveClient(client.uid)
            }
            client.wt <- nil
            client.PublishState(false)
            break
        }
        log.Info("msg:", msg.cmd)
        if msg.cmd == MSG_AUTH {
            client.HandleAuth(msg.body.(*Authentication))
        } else if msg.cmd == MSG_IM {
            client.HandleIMMessage(msg.body.(*IMMessage), msg.seq)
        } else if msg.cmd == MSG_GROUP_IM {
            client.HandleGroupIMMessage(msg.body.(*IMMessage), msg.seq)
        } else if msg.cmd == MSG_ACK {
            client.HandleACK(msg.body.(MessageACK))
        } else if msg.cmd == MSG_HEARTBEAT {
            
        } else if msg.cmd == MSG_PING {
            client.HandlePing()
        } else if msg.cmd == MSG_INPUTING {
            client.HandleInputing(msg.body.(*MessageInputing))
        } else if msg.cmd == MSG_SUBSCRIBE_ONLINE_STATE {
            client.HandleSubsribe(msg.body.(*MessageSubsribeState))
        } else {
            log.Info("unknown msg:", msg.cmd)
        }
    }
}
    
func (client *Client) SendOfflineMessage() {
    go func() {
        c := storage.LoadOfflineMessage(client.uid)
        if c != nil {
            for m := range c {
                client.wt <- m
            }
            storage.ClearOfflineMessage(client.uid)
        }
    }()
}

func (client *Client) ResetClient(uid int64) {
	//单点登录
    c := route.FindClient(client.uid)
    if c != nil {
        c.wt <- &Message{cmd:MSG_RST}
    }
}

func (client *Client) SendMessage(uid int64, msg *Message) bool {
    other := route.FindClient(uid)
    if other != nil {
        other.wt <- msg
        return true
    } else {
        peer := route.FindPeerClient(uid)
        if peer != nil {
            peer.wt <- msg
            return true
        }
    }
    return false
}

func (client *Client) PublishState(online bool) {
    subs := state_center.FindSubsriber(client.uid)
    state := &MessageOnlineState{client.uid, 0}
    if online {
        state.online = 1
    }

    log.Info("publish online state")
    set := NewIntSet()
    msg := &Message{cmd:MSG_ONLINE_STATE, body:state}
    for _, sub := range subs {
        log.Info("send online state:", sub)
        other := route.FindClient(sub)
        if other != nil {
            other.wt <- msg
        } else {
            set.Add(sub)
        }
    }
    if len(set) > 0 {
        state_center.Unsubscribe(client.uid, set)
    }
}

func (client *Client) IsOnline(uid int64) bool {
    other := route.FindClient(uid)
    if other != nil {
        return true
    } else {
        peer := route.FindPeerClient(uid)
        if peer != nil {
            return true
        }
    }
    return false
}

func (client *Client) SaveLoginInfo(platform_id int8) {
    conn := redis_pool.Get()
    defer conn.Close()

    key := fmt.Sprintf("users_%d", client.uid)

    var platform string
    if platform_id == PLATFORM_IOS {
        platform = "ios"
    } else if platform_id == PLATFORM_ANDROID {
        platform = "android"
    } else {
        platform = "unknown"
    }

    _, err := conn.Do("HMSET", key, "up_timestamp", client.tm.Unix(), "platform", platform)
    if err != nil {
        log.Info("hset err:", err)
    }
}


func (client *Client) HandleAuth(login *Authentication) {
    client.tm = time.Now()
    client.uid = login.uid
    log.Info("auth:", login.uid)

    client.SaveLoginInfo(login.platform_id)
    msg := &Message{cmd:MSG_AUTH_STATUS, body:&AuthenticationStatus{0}}
    client.wt <- msg

    client.ResetClient(client.uid)

    route.AddClient(client)
    cluster.AddClient(client.uid, int32(client.tm.Unix()))
    client.PublishState(true)
    client.SendOfflineMessage()

    atomic.AddInt64(&server_summary.nclients, 1)
}

func (client *Client) HandleSubsribe(msg *MessageSubsribeState) {
    if client.uid == 0 {
        return
    }

    for _, uid := range msg.uids {
        online := client.IsOnline(uid)
        var on int32
        if online {
            on = 1
        }
        state := &MessageOnlineState{uid, on}
        m := &Message{cmd:MSG_ONLINE_STATE, body:state}
        client.wt <- m
    }

    set := NewIntSet()
    for _, uid := range msg.uids {
        set.Add(uid)
        log.Info(client.uid, " subscribe:", uid)
    }
    state_center.Subscribe(client.uid, set)
}

func (client *Client) HandleIMMessage(msg *IMMessage, seq int) {
    msg.timestamp = int32(time.Now().Unix())
    m := &Message{cmd:MSG_IM, body:msg}
    r := client.SendMessage(msg.receiver, m)
    if !r {
        storage.SaveOfflineMessage(msg.receiver, &Message{cmd:MSG_IM, body:msg})
    }
    client.wt <- &Message{cmd:MSG_ACK, body:MessageACK(seq)}

    atomic.AddInt64(&server_summary.in_message_count, 1)
}

func (client *Client) HandleGroupIMMessage(msg *IMMessage, seq int) {
    group := group_manager.FindGroup(msg.receiver)
    if group == nil {
        log.Info("can't find group:", msg.receiver)
        return
    }
    peers := make(map[*PeerClient]struct{})
    for member := range group.Members() {
        //群消息不再发送给自己
        if member == client.uid {
            continue
        }
        other := route.FindClient(member)
        if other != nil {
            other.wt <- &Message{cmd:MSG_GROUP_IM, body:msg}
        } else {
            peer := route.FindPeerClient(member)
            if peer != nil {
                peers[peer] = struct{}{}
            } else {
                storage.SaveOfflineMessage(member, &Message{cmd:MSG_GROUP_IM, body:msg})           
            }
        }
    }
    for peer, _ := range peers {
        peer.wt <- &Message{cmd:MSG_GROUP_IM, body:msg}
    }
    client.wt <- &Message{cmd:MSG_ACK, body:MessageACK(seq)}
    atomic.AddInt64(&server_summary.in_message_count, 1)
}

func (client *Client) HandleInputing(inputing *MessageInputing) {
    msg := &Message{cmd:MSG_INPUTING, body:inputing}
    client.SendMessage(inputing.receiver, msg)
}

func (client *Client) HandleACK(ack MessageACK) {
    msg := client.RemoveUnAckMessage(ack)
    if msg == nil {
        return
    }
    if msg.cmd == MSG_IM {
        im := msg.body.(*IMMessage)
        ack := &MessagePeerACK{im.receiver, im.sender, im.msgid}
        m := &Message{cmd:MSG_PEER_ACK, body:ack}
        r := client.SendMessage(im.sender, m)
        if !r {
            storage.SaveOfflineMessage(im.sender, m)
        }
    }
}

func (client *Client) HandlePing() {
    m := &Message{cmd:MSG_PONG}
    client.wt <- m
}

func (client *Client) RemoveUnAckMessage(ack MessageACK) *Message {
    client.mutex.Lock()
    defer client.mutex.Unlock()

    pos := -1
    for i, msg := range client.unacks {
        if msg.seq == int(ack) {
            pos = i
            break
        }
    }
    if pos == -1 {
        log.Info("invalid ack seq:", ack)
        return nil
    } else {
        m := client.unacks[pos]
        client.unacks = client.unacks[pos+1:]
        log.Info("remove unack msg:", len(client.unacks))
        return m
    }
}

func (client *Client) AddUnAckMessage(msg *Message) {
    client.mutex.Lock()
    defer client.mutex.Unlock()
    client.unacks = append(client.unacks, msg)
}

func (client *Client) SaveUnAckMessage() {
    client.mutex.Lock()
    defer client.mutex.Unlock()
    for _, msg := range client.unacks {
        storage.SaveOfflineMessage(client.uid, msg)
    }
}

//unack消息重新发送給新登录的客户端
func (client *Client) ResendUnAckMessage() {
    client.mutex.Lock()
    defer client.mutex.Unlock()

    other := route.FindClient(client.uid)
    if other != nil {
        //assert(other != client)
        for _, msg := range client.unacks {
            other.wt <- msg
        }
        client.unacks = client.unacks[0:0]
    } else {
        peer := route.FindPeerClient(client.uid)
        if peer != nil {
            for _, msg := range client.unacks {
                peer.wt <- msg
            }
            client.unacks = client.unacks[0:0]
        }
    }
}

func (client *Client) Write() {
    seq := 0
    rst := false
    for {
        msg := <- client.wt
        if msg == nil {
            if rst {
                client.ResendUnAckMessage()
            }
            client.SaveUnAckMessage()
            client.conn.Close()
            atomic.AddInt64(&server_summary.nconnections, -1)
            if client.uid > 0 {
                atomic.AddInt64(&server_summary.nclients, -1)
            }
            log.Info("socket closed")
            break
        }
        seq++
        msg.seq = seq
        if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
            client.AddUnAckMessage(msg)
            atomic.AddInt64(&server_summary.out_message_count, 1)
        }

		if rst {
			continue
		}
        SendMessage(client.conn, msg)
        if msg.cmd == MSG_RST {
            client.conn.Close()
            rst = true
        }
    }
}

func (client *Client) Run() {
    go client.Write()
    go client.Read()
}
