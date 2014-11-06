package main
import "sync"
import log "github.com/golang/glog"

type PeerClientSet struct {
    peers map[*PeerClient]bool    
}

func NewPeerClientSet() *PeerClientSet {
    set := new(PeerClientSet)
    set.peers = make(map[*PeerClient]bool)
    return set
}

func (set *PeerClientSet) Add(peer *PeerClient) {
    set.peers[peer] = true
}

func (set *PeerClientSet) Contains(peer *PeerClient) bool {
    _, ok := set.peers[peer]
    return ok
}

func (set *PeerClientSet) Remove(peer *PeerClient) {
    if _, ok := set.peers[peer]; ok {
        delete(set.peers, peer)
    } else {
        log.Info("peer client no exists")
    }
}

type Route struct {
    mutex sync.Mutex
    clients map[int64]*Client
    peers *PeerClientSet
}

func NewRoute() *Route {
    route := new(Route)
    route.clients = make(map[int64]*Client)
    route.peers = NewPeerClientSet()
    return route
}

func (route *Route) AddClient(client *Client) {
    route.mutex.Lock()
    defer route.mutex.Unlock()
    if _, ok := route.clients[client.uid]; ok {
        log.Info("client exists")
    }
    route.clients[client.uid] = client
}

func (route *Route) RemoveClient(client *Client) bool {
    route.mutex.Lock()
    defer route.mutex.Unlock()
    if _, ok := route.clients[client.uid]; ok {
        if route.clients[client.uid] == client {
            delete(route.clients, client.uid)
            return true
        }
    }
    log.Info("client non exists")
    return false
}

func (route *Route) FindClient(uid int64) *Client{
    route.mutex.Lock()
    defer route.mutex.Unlock()

    c, ok :=  route.clients[uid]
    if ok {
        return c
    } else {
        return nil
    }
}

func (route *Route) GetClientUids() map[int64]int32 {
    route.mutex.Lock()
    defer route.mutex.Unlock()
    uids := make(map[int64]int32)
    for uid, c := range route.clients {
        uids[uid] = int32(c.tm.Unix())
    }
    return uids
}

func (route *Route) AddPeerClient(peer *PeerClient) {
    route.mutex.Lock()
    defer route.mutex.Unlock()

    if route.peers.Contains(peer) {
        return
    }
    route.peers.Add(peer)
}

func (route *Route) RemovePeerClient(peer *PeerClient) {
    route.mutex.Lock()
    defer route.mutex.Unlock()

    route.peers.Remove(peer)
}

func (route *Route) FindPeerClient(uid int64) *PeerClient {
    route.mutex.Lock()
    defer route.mutex.Unlock()

    for peer := range route.peers.peers {
        if peer.ContainUid(uid) {
            return peer
        }
    }
    return nil
}
