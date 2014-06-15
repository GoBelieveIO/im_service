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
        } else if msg.cmd == MSG_REMOVE_CLIENT {
            peer.HandleRemoveClient(msg.body.(int64))
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

func (peer *PeerClient) ResetClient(uid int64) {
	//单点登录
    c := route.FindClient(uid)
    if c != nil {
        c.wt <- &Message{cmd:MSG_RST}
    }
}

func (peer *PeerClient) HandleAddClient(uid int64) {
    peer.mutex.Lock()
    defer peer.mutex.Unlock()
    if _, ok := peer.uids[uid]; ok {
        log.Printf("uid:%d exists\n", uid)
        return
    }
    log.Println("add uid:", uid)
    peer.uids[uid] = struct{}{}

    peer.ResetClient(uid)

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
    if _, ok := peer.uids[uid]; !ok {
        log.Printf("uid:%d non exists\n", uid)
        return
    }
    log.Println("remove uid:", uid)
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
