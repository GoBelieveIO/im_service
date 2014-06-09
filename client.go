package main
import "io"
import "net"
import "bytes"
import "encoding/binary"
import "log"

const MSG_HEARTBEAT = 1
const MSG_AUTH = 2
const MSG_IM = 3

type IMMessage struct {
    sender int64
    receiver int64
    content string
}

type Authentication struct {
    uid int64
}

type AuthenticationStatus struct {
    status int32
}

type Message struct {
    cmd int
    body interface{}
}

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
        msg := client.ReceiveMessage()
        if msg == nil {
            route.RemoveClient(client)
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
    msg := &Message{MSG_AUTH, &AuthenticationStatus{0}}
    client.wt <- msg

    route.AddClient(client)
    client.LoadMessage()
}

func (client *Client) HandleIMMessage(msg *IMMessage) {
    other := route.FindClient(msg.receiver)
    if other != nil {
        other.wt <- &Message{MSG_IM, msg}
    } else {
        client.SaveMessage(msg)
    }
}
//加载离线消息
func (client *Client) LoadMessage() {
    
}

//存储离线消息
func (client *Client) SaveMessage(message *IMMessage) {
    
}

func (client *Client) ReceiveMessage() *Message {
    buff := make([]byte, 8)
    _, err := io.ReadFull(client.conn, buff)
    if err != nil {
        return nil
    }
    var len int32
    buffer := bytes.NewBuffer(buff)
    binary.Read(buffer, binary.BigEndian, &len)
    cmd, _ := buffer.ReadByte()

    if len < 0 || len > 64*1024 {
        log.Println("invalid len:", len)
        return nil
    }
    buff = make([]byte, len)
    _, err = io.ReadFull(client.conn, buff)
    if err != nil {
        return nil
    }
    
    if cmd == MSG_AUTH {
        buffer := bytes.NewBuffer(buff)
        var uid int64
        binary.Read(buffer, binary.BigEndian, &uid)
        log.Println("uid:", uid)
        return &Message{MSG_AUTH, &Authentication{uid}}
    } else if cmd == MSG_IM {
        buffer := bytes.NewBuffer(buff)
        im := &IMMessage{}
        binary.Read(buffer, binary.BigEndian, &im.sender)
        binary.Read(buffer, binary.BigEndian, &im.receiver)
        im.content = string(buff[16:])
        return &Message{MSG_IM, im}
    } else {
        return nil
    }
}

func (client *Client) WriteHeader(len int32, cmd byte, buffer *bytes.Buffer) {
    binary.Write(buffer, binary.BigEndian, len)
    buffer.WriteByte(cmd)
    buffer.WriteByte(byte(0))
    buffer.WriteByte(byte(0))
    buffer.WriteByte(byte(0))
}

func (client *Client) WriteMessage(message *IMMessage) {
    var length int32 = int32(len(message.content) + 16)
    buffer := new(bytes.Buffer)
    client.WriteHeader(length, MSG_IM, buffer)
    binary.Write(buffer, binary.BigEndian, message.sender)
    binary.Write(buffer, binary.BigEndian, message.receiver)
    buffer.Write([]byte(message.content))
    buf := buffer.Bytes()

    n, err := client.conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func (client *Client) WriteAuthStatus(auth *AuthenticationStatus) {
    var length int32  = 4
    buffer := new(bytes.Buffer)
    client.WriteHeader(length, MSG_AUTH, buffer)
    binary.Write(buffer, binary.BigEndian, auth.status)
    buf := buffer.Bytes()
    n, err := client.conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func (client *Client) Write() {
    for {
        msg := <- client.wt
        if msg == nil {
            log.Println("socket closed")
            break
        }
        if msg.cmd == MSG_AUTH {
            client.WriteAuthStatus(msg.body.(*AuthenticationStatus))
        } else if msg.cmd == MSG_IM {
            client.WriteMessage(msg.body.(*IMMessage))
        } else {
            log.Println("unknow cmd", msg.cmd)
        }
    }
}

func (client *Client) Run() {
    go client.Write()
    go client.Read()
}
