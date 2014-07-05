package main
import "encoding/binary"
import "io"
import "bytes"
import "log"

const MSG_HEARTBEAT = 1
const MSG_AUTH = 2
const MSG_AUTH_STATUS = 3
const MSG_IM = 4
const MSG_ACK = 5
const MSG_RST = 6
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8
const MSG_PEER_ACK = 9
const MSG_INPUTING = 10
const MSG_SUBSCRIBE_ONLINE_STATE = 11
const MSG_ONLINE_STATE = 12

const MSG_ADD_CLIENT = 128
const MSG_REMOVE_CLIENT = 129

type IMMessage struct {
    sender int64
    receiver int64
    msgid int32
    content string
}

type MessageInputing struct {
    sender int64
    receiver int64
}

type MessageSubsribeState struct {
    uids []int64
}

type MessageOnlineState struct {
    sender int64
    online int32
}

type MessageACK int32

type MessagePeerACK struct {
    sender int64
    receiver int64
    msgid int32
}
type Authentication struct {
    uid int64
}

type AuthenticationStatus struct {
    status int32
}

type MessageAddClient struct {
    uid int64
    timestamp int32
}

type Message struct {
    cmd int
    seq int
    body interface{}
}

func ReceiveMessage(conn io.Reader) *Message {
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
    } else if cmd == MSG_IM || cmd == MSG_GROUP_IM{
        if len < 20 {
            return nil
        }
        buffer := bytes.NewBuffer(buff)
        im := &IMMessage{}
        binary.Read(buffer, binary.BigEndian, &im.sender)
        binary.Read(buffer, binary.BigEndian, &im.receiver)
        binary.Read(buffer, binary.BigEndian, &im.msgid)
        im.content = string(buff[20:])
        return &Message{int(cmd), int(seq), im}
    } else if cmd == MSG_ADD_CLIENT {
        buffer := bytes.NewBuffer(buff)
        ac := &MessageAddClient{}
        binary.Read(buffer, binary.BigEndian, &ac.uid)
        binary.Read(buffer, binary.BigEndian, &ac.timestamp)
        return &Message{int(cmd), int(seq), ac}
    } else if cmd == MSG_REMOVE_CLIENT{
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
    } else if cmd == MSG_INPUTING {
        if len < 16 {
            return nil
        }
        buffer := bytes.NewBuffer(buff)
        inputing := &MessageInputing{}
        binary.Read(buffer, binary.BigEndian, &inputing.sender)
        binary.Read(buffer, binary.BigEndian, &inputing.receiver)
        return &Message{int(cmd), int(seq), inputing}
    } else if cmd == MSG_GROUP_NOTIFICATION {
        return &Message{int(cmd), int(seq), string(buff)}
    } else if cmd == MSG_PEER_ACK {
        if len < 20 {
            return nil;
        }
        buffer := bytes.NewBuffer(buff)
        ack := &MessagePeerACK{}
        binary.Read(buffer, binary.BigEndian, &ack.sender)
        binary.Read(buffer, binary.BigEndian, &ack.receiver)
        binary.Read(buffer, binary.BigEndian, &ack.msgid)
        return &Message{int(cmd), int(seq), ack}
    } else if cmd == MSG_SUBSCRIBE_ONLINE_STATE {
        sub := &MessageSubsribeState{}
        buffer := bytes.NewBuffer(buff)
        var count int32
        binary.Read(buffer, binary.BigEndian, &count)
        sub.uids = make([]int64, count)
        for i := 0; i < int(count); i++ {
            binary.Read(buffer, binary.BigEndian, &sub.uids[i])
        }
        return &Message{int(cmd), int(seq), sub}
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

func WriteMessage(conn io.Writer, cmd byte, seq int, message *IMMessage) {
    var length int32 = int32(len(message.content) + 20)
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), cmd, buffer)
    binary.Write(buffer, binary.BigEndian, message.sender)
    binary.Write(buffer, binary.BigEndian, message.receiver)
    binary.Write(buffer, binary.BigEndian, message.msgid)
    buffer.Write([]byte(message.content))
    buf := buffer.Bytes()

    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteAuth(conn io.Writer, seq int, auth *Authentication) {
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

func  WriteAuthStatus(conn io.Writer, seq int, auth *AuthenticationStatus) {
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

func WriteAddClient(conn io.Writer, seq int, ac *MessageAddClient) {
    var length int32  = 12
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_ADD_CLIENT, buffer)
    binary.Write(buffer, binary.BigEndian, ac.uid)
    binary.Write(buffer, binary.BigEndian, ac.timestamp)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteRemoveClient(conn io.Writer, seq int, uid int64) {
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

func WriteACK(conn io.Writer, seq int, ack MessageACK) {
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

func WritePeerACK(conn io.Writer, seq int, ack *MessagePeerACK) {
    var length int32  = 20
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_PEER_ACK, buffer)
    binary.Write(buffer, binary.BigEndian, ack.sender)
    binary.Write(buffer, binary.BigEndian, ack.receiver)
    binary.Write(buffer, binary.BigEndian, ack.msgid)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteRST(conn io.Writer, seq int) {
    var length int32 = 0
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_RST, buffer)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteHeartbeat(conn io.Writer, seq int) {
    var length int32 = 0
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_HEARTBEAT, buffer)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteInputing(conn io.Writer, seq int, inputing *MessageInputing) {
    var length int32 = 16
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_INPUTING, buffer)
    binary.Write(buffer, binary.BigEndian, inputing.sender)
    binary.Write(buffer, binary.BigEndian, inputing.receiver)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}


func WriteGroupNotification(conn io.Writer, seq int, notification string) {
    var length int32 = int32(len(notification))
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_GROUP_NOTIFICATION, buffer)
    buffer.Write([]byte(notification))
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func WriteState(conn io.Writer, seq int, state *MessageOnlineState) {
    var length int32 = 12
    buffer := new(bytes.Buffer)
    WriteHeader(length, int32(seq), MSG_ONLINE_STATE, buffer)
    binary.Write(buffer, binary.BigEndian, state.sender)
    binary.Write(buffer, binary.BigEndian, state.online)
    buf := buffer.Bytes()
    n, err := conn.Write(buf)
    if err != nil || n != len(buf) {
        log.Println("sock write error")
    }
}

func SendMessage(conn io.Writer, msg *Message) {
    if msg.cmd == MSG_AUTH {
        WriteAuth(conn, msg.seq, msg.body.(*Authentication))
    } else if msg.cmd == MSG_AUTH_STATUS {
        WriteAuthStatus(conn, msg.seq, msg.body.(*AuthenticationStatus))
    } else if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
        WriteMessage(conn, byte(msg.cmd), msg.seq, msg.body.(*IMMessage))
    } else if msg.cmd == MSG_ADD_CLIENT {
        WriteAddClient(conn, msg.seq, msg.body.(*MessageAddClient))
    } else if msg.cmd == MSG_REMOVE_CLIENT {
        WriteRemoveClient(conn, msg.seq, msg.body.(int64))
    } else if msg.cmd == MSG_ACK {
        WriteACK(conn, msg.seq, msg.body.(MessageACK))
    } else if msg.cmd == MSG_PEER_ACK {
        WritePeerACK(conn, msg.seq, msg.body.(*MessagePeerACK))
    } else if msg.cmd == MSG_RST {
        WriteRST(conn, msg.seq)
    } else if msg.cmd == MSG_HEARTBEAT {
        WriteHeartbeat(conn, msg.seq)
    } else if msg.cmd == MSG_INPUTING {
        WriteInputing(conn, msg.seq, msg.body.(*MessageInputing))
    } else if msg.cmd == MSG_GROUP_NOTIFICATION {
        WriteGroupNotification(conn, msg.seq, msg.body.(string))
    } else if msg.cmd == MSG_ONLINE_STATE {
        WriteState(conn, msg.seq, msg.body.(*MessageOnlineState))
    } else {
        log.Println("unknow cmd", msg.cmd)
    }
}
