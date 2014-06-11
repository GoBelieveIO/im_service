package main
import "net"
import "sync"
import "log"

type PeerClient struct {
    wt chan *Message
    conn *net.TCPConn

    mutex sync.Mutex
    uids map[int64]struct{}
}

func NewPeerClient(conn *net.TCPConn) *PeerClient {
    client := new(PeerClient)
    client.wt = make(chan *Message)
    client.conn = conn
    client.uids = make(map[int64]struct{})
    return client
}

func (peer *PeerClient) Read() {
    for {
        msg := ReceiveMessage(peer.conn)
        if msg == nil {
            route.RemovePeerClient(peer)
            peer.wt <- nil
            break
        }
        log.Println("msg:", msg.cmd)
        if msg.cmd == MSG_ADD_CLIENT {
            peer.HandleAddClient(msg.body.(int64))
        } else if msg.cmd == MSG_HEARTBEAT {
            peer.HandleRemoveClient(msg.body.(int64))
        }
    }
}

func (peer *PeerClient) ContainUid(uid int64) bool {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()

    _, ok := peer.uids[uid]
    return ok
}

func (peer *PeerClient) HandleAddClient(uid int64) {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()
    if _, ok := peer.uids[uid]; ok {
        log.Printf("uid:%d exists\n", uid)
        return
    }
    peer.uids[uid] = struct{}{}
}

func (peer *PeerClient) HandleRemoveClient(uid int64) {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()
    if _, ok := peer.uids[uid]; !ok {
        log.Printf("uid:%d non exists\n", uid)
        return
    }
    delete(peer.uids, uid)
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
