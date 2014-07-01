package main
import "net"
import "sync"
import "log"
import "time"

const PEER_TIMEOUT = 20
type PeerClient struct {
    wt chan *Message
    conn *net.TCPConn

    mutex sync.Mutex
    uids IntSet
}

func NewPeerClient(conn *net.TCPConn) *PeerClient {
    client := new(PeerClient)
    client.wt = make(chan *Message)
    client.conn = conn
    client.uids = NewIntSet()
    return client
}

func (peer *PeerClient) Read() {
    for {
        peer.conn.SetDeadline(time.Now().Add(PEER_TIMEOUT*time.Second))
        msg := ReceiveMessage(peer.conn)
        if msg == nil {
            route.RemovePeerClient(peer)
            peer.wt <- nil
            break
        }
        log.Println("msg:", msg.cmd)
        if msg.cmd == MSG_ADD_CLIENT {
            peer.HandleAddClient(msg.body.(*MessageAddClient))
        } else if msg.cmd == MSG_REMOVE_CLIENT {
            peer.HandleRemoveClient(msg.body.(int64))
        } else if msg.cmd == MSG_HEARTBEAT {
            log.Println("peer heartbeat")
        }
    }
}

func (peer *PeerClient) ContainUid(uid int64) bool {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()
    return peer.uids.IsMember(uid)
}

func (peer *PeerClient) ResetClient(uid int64, ts int32) {
	//单点登录
    c := route.FindClient(uid)
    if c != nil {
        if c.tm.Unix() <= int64(ts) {
            c.wt <- &Message{cmd:MSG_RST}
        }
    }
}

func (peer *PeerClient) HandleAddClient(ac *MessageAddClient) {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()
    uid := ac.uid
    if peer.uids.IsMember(uid) {
        log.Printf("uid:%d exists\n", uid)
        return
    }
    log.Println("add uid:", uid)
    peer.uids.Add(uid)

    peer.ResetClient(uid, ac.timestamp)

    c := storage.LoadOfflineMessage(uid)
    if c != nil {
        for m := range c {
            peer.wt <- &Message{cmd:MSG_IM, body:m}
        }
        storage.ClearOfflineMessage(uid)
    }
}

func (peer *PeerClient) HandleRemoveClient(uid int64) {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()
    peer.uids.Remove(uid)
    log.Println("remove uid:", uid)
}

func (peer *PeerClient) Write() {
    for {
        msg := <- peer.wt
        if msg == nil {
            log.Println("socket closed")
            break
        }
        SendMessage(peer.conn, msg)
    }
}

func (peer *PeerClient) Run() {
    route.AddPeerClient(peer)
    go peer.Write()
    go peer.Read()
}
