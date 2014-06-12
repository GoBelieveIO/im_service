package main
import "net"
import "log"
import "sync"

type Client struct {
    wt chan *Message
    uid int64
    conn *net.TCPConn
    unacks []*Message
    mutex sync.Mutex
}

func NewClient(conn *net.TCPConn) *Client {
    client := new(Client)
    client.conn = conn
    client.wt = make(chan *Message)
    client.unacks = make([]*Message, 0, 4)
    return client
}

func (client *Client) Read() {
    for {
        msg := ReceiveMessage(client.conn)
        if msg == nil {
            route.RemoveClient(client)
            if client.uid > 0 {
                cluster.RemoveClient(client.uid)
            }
            client.wt <- nil
            break
        }
        log.Println("msg:", msg.cmd)
        if msg.cmd == MSG_AUTH {
            client.HandleAuth(msg.body.(*Authentication))
        } else if msg.cmd == MSG_IM {
            client.HandleIMMessage(msg.body.(*IMMessage), msg.seq)
        } else if msg.cmd == MSG_ACK {
            client.HandleACK(msg.body.(MessageACK))
        } else if msg.cmd == MSG_HEARTBEAT {
            
        }
    }
}
    
func (client *Client) HandleAuth(login *Authentication) {
    client.uid = login.uid
    log.Println("auth:", login.uid)
    msg := &Message{cmd:MSG_AUTH, body:&AuthenticationStatus{0}}
    client.wt <- msg

    route.AddClient(client)
    cluster.AddClient(client.uid)
    c := storage.LoadOfflineMessage(client.uid)
    if c != nil {
        for m := range c {
            client.wt <- &Message{cmd:MSG_IM, body:m}
        }
        storage.ClearOfflineMessage(client.uid)
    }
}

func (client *Client) HandleIMMessage(msg *IMMessage, seq int) {
    other := route.FindClient(msg.receiver)
    if other != nil {
        other.wt <- &Message{cmd:MSG_IM, body:msg}
    } else {
        peer := route.FindPeerClient(msg.receiver)
        if peer != nil {
            peer.wt <- &Message{cmd:MSG_IM, body:msg}
        } else {
            storage.SaveOfflineMessage(msg)
        }
    }
    client.wt <- &Message{cmd:MSG_ACK, body:MessageACK(seq)}
}

func (client *Client) HandleACK(ack MessageACK) {
    client.RemoveUnAckMessage(ack)
}

func (client *Client) RemoveUnAckMessage(ack MessageACK) {
    client.mutex.Lock()
    defer client.mutex.Unlock()

    pos := -1
    for i, msg := range client.unacks {
        if msg.seq == int(ack) {
            pos = i
            break
        }
    }
    client.unacks = client.unacks[pos+1:]
    if pos == -1 {
        log.Println("invalid ack seq:", ack)
    }
    log.Println("remove unack msg:", len(client.unacks))
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
        storage.SaveOfflineMessage(msg.body.(*IMMessage))
    }
}

func (client *Client) Write() {
    seq := 0
    for {
        msg := <- client.wt
        if msg == nil {
            log.Println("socket closed")
            client.SaveUnAckMessage()
            break
        }
        seq++
        msg.seq = seq
        if msg.cmd == MSG_IM {
            client.AddUnAckMessage(msg)
        }
        SendMessage(client.conn, msg)
    }
}

func (client *Client) Run() {
    go client.Write()
    go client.Read()
}
