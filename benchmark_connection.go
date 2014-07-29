package main
import "net"
import "log"
import "runtime"
import "time"

const HOST = "127.0.0.1"
const PORT = 23000

var c chan bool
const concurrent = 5
const count = 10000

func receive(uid int64) {
    ip := net.ParseIP(HOST)
    addr := net.TCPAddr{ip, PORT, ""}

    conn, err := net.DialTCP("tcp4", nil, &addr)
    if err != nil {
        log.Println("connect error")
        return
    }
    seq := 1

    SendMessage(conn, &Message{MSG_AUTH, seq, &Authentication{uid}})
    ReceiveMessage(conn)

    total := count
    for i := 0; i < count; i++ {
        conn.SetDeadline(time.Now().Add(60*8*time.Second))
        msg := ReceiveMessage(conn)
        if msg == nil {
            log.Println("send heartbeat message")
            seq++
            conn.SetDeadline(time.Now().Add(60*8*time.Second))
            SendMessage(conn, &Message{MSG_HEARTBEAT, seq, nil})
            continue
        }
        seq++
        ack := &Message{MSG_ACK, seq, MessageACK(msg.seq)}
        SendMessage(conn, ack)
    }
    conn.Close()
    c <- true
    log.Printf("%d received:%d", uid, total)
}

func main() {
    runtime.GOMAXPROCS(4)

	log.SetFlags(log.Lshortfile|log.LstdFlags)
    c = make(chan bool, 100)
    u := int64(13635273140)
    var i int64

    for i = 0; i < concurrent; i++ {
        go receive(u+concurrent+i)
    }
  
    for i = 0; i < concurrent; i++ {
        <- c
    }
}
