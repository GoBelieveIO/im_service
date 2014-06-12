package main
import "time"
import "net"
import "log"

type Peer struct {
    host string
    port int
    wt chan *Message
    conn *net.TCPConn
    alive bool
}

func (peer *Peer) PeerID() int64 {
    ip := net.ParseIP(peer.host)
    i := int64(ip[0])<<24|int64(ip[1])<<16|int64(ip[2])<<8|int64(ip[3])
    return i << 32 | int64(peer.port)
}

func (peer *Peer) Read() {
    for {
        msg := ReceiveMessage(peer.conn)
        if msg == nil {
            peer.wt <- nil
            break
        }
        log.Println("msg:", msg.cmd)
        if msg.cmd == MSG_IM {
            peer.HandleIMMessage(msg.body.(*IMMessage))
        }
    }
}

func (peer *Peer) HandleIMMessage(msg *IMMessage) {
    other := route.FindClient(msg.receiver)
    if other != nil {
        other.wt <- &Message{cmd:MSG_IM, body:msg}
    } else {
        log.Println("can't find client:", msg.receiver)
        storage.SaveOfflineMessage(msg)
    }
}

func (peer *Peer) Write() {
    for {
        msg := <- peer.wt
        if msg == nil {
            log.Println("socket closed")
            peer.conn = nil
            break
        }
        log.Println("peer msg:", msg.cmd)
        SendMessage(peer.conn, msg)
    }
}

func (peer *Peer) Start() {
    peer.alive = true
    go peer.Connect()
}

func (peer *Peer) Stop() {
    peer.alive = false
}

func (peer *Peer) AddAllClient() {
    uids := route.GetClientUids()
    for _, uid := range uids {
        msg := &Message{cmd:MSG_ADD_CLIENT, body:uid}
        peer.wt <- msg
    }
}

func (peer *Peer) Connect() {
    ip := net.ParseIP(peer.host)
    addr := net.TCPAddr{ip, peer.port, ""}
    for {
        if peer.conn == nil && peer.alive {
            conn, err := net.DialTCP("tcp4", nil, &addr)
            if err != nil {
                log.Println("connect error:", ip, " ", peer.port)
            } else {
                peer.conn = conn
                go peer.Read()
                go peer.Write()
                peer.AddAllClient()
            }
        }
        timer := time.NewTimer(60*time.Second)
        <- timer.C
    }
}
