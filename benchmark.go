package main
import "fmt"
import "net"
import "log"
import "runtime"
import "time"

const HOST = "127.0.0.1"
const PORT = 23000

var c chan bool
const concurrent = 10
const count = 100000
func send(uid int64, receiver int64) {
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

    for i := 0; i < count; i++ {
        content := fmt.Sprintf("test....%d", i)
        seq++
        msg := &Message{MSG_IM, seq, &IMMessage{uid, receiver, int32(i), content}}
        SendMessage(conn, msg)
        for {
            ack := ReceiveMessage(conn)
            if ack.cmd == MSG_ACK {
                break
            }
        }
    }
    conn.Close()
    c <- true
    log.Printf("%d send complete", uid)
}

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
        conn.SetDeadline(time.Now().Add(40*time.Second))
        msg := ReceiveMessage(conn)
        if msg == nil {
            log.Println("receive nill message")
            total = i
            break
        }
        if msg.cmd != MSG_IM {
            log.Println("mmmmmm")
        }
        m := msg.body.(*IMMessage)

        log.Printf("sender:%d receiver:%d content:%s", m.sender, m.receiver, m.content)
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
    time.Sleep(2*time.Second)
    for  i  = 0; i < concurrent; i++ {
        go send(u+i, u + i + concurrent)
    }
    for i = 0; i < 2*concurrent; i++ {
        <- c
    }
}
