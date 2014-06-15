package main
import "encoding/binary"
import "io"
import "net"
import "bytes"
import "log"

const MSG_HEARTBEAT = 1
const MSG_AUTH = 2
const MSG_AUTH_STATUS = 3
const MSG_IM = 4
const MSG_ACK = 5
const MSG_RST = 6

const MSG_ADD_CLIENT = 128
const MSG_REMOVE_CLIENT = 129

type IMMessage struct {
    sender int64
    receiver int64
    content string
}

type MessageACK int32

type Authentication struct {
    uid int64
}

type AuthenticationStatus struct {
    status int32
}

type Message struct {
    cmd int
    seq int
    body interface{}
}

func ReceiveMessage(conn *net.TCPConn) *Message {
    buff := make([]byte, 12)
    _, err := io.ReadFull(conn, buff)
    if err != nil {
        log.Println("sock read error:", err)
        return nil
    }
    var len int32
    var seq int32
    buffer := bytes.NewBuffer(buff)
    binary.Read(buffer, binary.BigEndian, &len)
    binary.Read(buffer, binary.BigEndian, &seq)
    cmd, _ := buffer.ReadByte()
    log.Println("cmd:", cmd)
    if len < 0 || len > 64*1024 {
        log.Println("invalid len:", len)
        return nil
    }
    buff = make([]byte, len)
    _, err = io.ReadFull(conn, buff)
    if err != nil {
        log.Println("sock read error:", err)
        return nil
    }
    
    if cmd == MSG_AUTH {
        buffer := bytes.NewBuffer(buff)
        var uid int64
        binary.Read(buffer, binary.BigEndian, &uid)
        log.Println("uid:", uid)
        return &Message{MSG_AUTH, int(seq), &Authentication{uid}}
    } else if cmd == MSG_AUTH_STATUS {
        buffer := bytes.NewBuffer(buff)
        var status int32
        binary.Read(buffer, binary.BigEndian, &status)
        return &Message{MSG_AUTH_STATUS, int(seq), &AuthenticationStatus{status}}
    } else if cmd == MSG_IM {
        if len < 16 {
            return nil
        }
        buffer := bytes.NewBuffer(buff)
        im := &IMMessage{}
        binary.Read(buffer, binary.BigEndian, &im.sender)
        binary.Read(buffer, binary.BigEndian, &im.receiver)
        im.content = string(buff[16:])
        return &Message{MSG_IM, int(seq), im}
    } else if cmd == MSG_ADD_CLIENT || cmd == MSG_REMOVE_CLIENT{
        buffer := bytes.NewBuffer(buff)
        var uid int64
        binary.Read(buffer, binary.BigEndian, &uid)
        return &Message{int(cmd), int(seq), uid}
    } else if cmd == MSG_ACK {
        buffer := bytes.NewBuffer(buff)
        var ack int32
        binary.Read(buffer, binary.BigEndian, &ack)
        return &Message{int(cmd), int(seq), MessageACK(ack)}
    } else if cmd == MSG_HEARTBEAT {
        return &Message{int(cmd), int(seq), nil}
    } else {
        return nil
    }
}


func WriteHeader(len int32, seq int32, cmd byte, buffer *bytes.Buffer) {
    binary.Write(buffer, binary.BigEndian, len)
    binary.Write(buffer, binary.BigEndian, seq)
    buffer.WriteByte(cmd)
    buffer.WriteByte(byte(0))
    buffer.WriteByte(byte(0))
    buffer.WriteByte(byte(0))
}

func WriteMessage(conn *net.TCPConn, seq int, message *IMMessage) {
    var length int32 = int32(len(message.content) + 16)
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_IM, buffer)
    binary.Write(buffer, binary.BigEndian, message.sender)
    binary.Write(buffer, binary.BigEndian, message.receiver)
    buffer.Write([]byte(message.content))
    buf := buffer.Bytes()

    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteAuth(conn *net.TCPConn, seq int, auth *Authentication) {
    var length int32  = 8
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_AUTH, buffer)
    binary.Write(buffer, binary.BigEndian, auth.uid)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func  WriteAuthStatus(conn *net.TCPConn, seq int, auth *AuthenticationStatus) {
    var length int32  = 4
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_AUTH_STATUS, buffer)
    binary.Write(buffer, binary.BigEndian, auth.status)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteAddClient(conn *net.TCPConn, seq int, uid int64) {
    var length int32  = 8
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_ADD_CLIENT, buffer)
    binary.Write(buffer, binary.BigEndian, uid)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteRemoveClient(conn *net.TCPConn, seq int, uid int64) {
    var length int32  = 8
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_REMOVE_CLIENT, buffer)
    binary.Write(buffer, binary.BigEndian, uid)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteACK(conn *net.TCPConn, seq int, ack MessageACK) {
    var length int32  = 4
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_ACK, buffer)
    binary.Write(buffer, binary.BigEndian, int32(ack))
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteRST(conn *net.TCPConn, seq int) {
    var length int32 = 0
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_RST, buffer)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteHeartbeat(conn *net.TCPConn, seq int) {
    var length int32 = 0
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_HEARTBEAT, buffer)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func SendMessage(conn *net.TCPConn, msg *Message) {
    if msg.cmd == MSG_AUTH {
        WriteAuth(conn, msg.seq, msg.body.(*Authentication))
    } else if msg.cmd == MSG_AUTH_STATUS {
        WriteAuthStatus(conn, msg.seq, msg.body.(*AuthenticationStatus))
    } else if msg.cmd == MSG_IM {
        WriteMessage(conn, msg.seq, msg.body.(*IMMessage))
    } else if msg.cmd == MSG_ADD_CLIENT {
        WriteAddClient(conn, msg.seq, msg.body.(int64))
    } else if msg.cmd == MSG_REMOVE_CLIENT {
        WriteRemoveClient(conn, msg.seq, msg.body.(int64))
    } else if msg.cmd == MSG_ACK {
        WriteACK(conn, msg.seq, msg.body.(MessageACK))
    } else if msg.cmd == MSG_RST {
        WriteRST(conn, msg.seq)
    } else if msg.cmd == MSG_HEARTBEAT {
        WriteHeartbeat(conn, msg.seq)
    } else {
        log.Println("unknow cmd", msg.cmd)
    }
}
