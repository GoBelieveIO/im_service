package main
import "net"
import "log"


type Client struct {
    wt chan *Message
    uid int64
    conn *net.TCPConn
}

func NewClient(conn *net.TCPConn) *Client {
    client := new(Client)
    client.conn = conn
    client.wt = make(chan *Message)
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
            client.HandleIMMessage(msg.body.(*IMMessage))
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
    client.LoadMessage()
}

func (client *Client) HandleIMMessage(msg *IMMessage) {
    other := route.FindClient(msg.receiver)
    if other != nil {
        other.wt <- &Message{cmd:MSG_IM, body:msg}
    } else {
        peer := route.FindPeerClient(msg.receiver)
        if peer != nil {
            peer.wt <- &Message{cmd:MSG_IM, body:msg}
        } else {
            client.SaveMessage(msg)
        }
    }
}

//加载离线消息
func (client *Client) LoadMessage() {
    
}

//存储离线消息
func (client *Client) SaveMessage(message *IMMessage) {
    
}

func (client *Client) Write() {
    seq := 0
    for {
        msg := <- client.wt
        if msg == nil {
            log.Println("socket closed")
            break
        }
        seq++
        msg.seq = seq
        SendMessage(client.conn, msg)
    }
}

func (client *Client) Run() {
    go client.Write()
    go client.Read()
}
