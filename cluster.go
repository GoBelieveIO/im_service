package main
import "net"

type Cluster struct {
    peers map[int64]*Peer
}

func NewCluster(addrs []*net.TCPAddr) *Cluster {
    cluster := new(Cluster)
    cluster.peers = make(map[int64]*Peer)
    for _, addr := range addrs {
        peer := new(Peer)
        peer.host = addr.IP.String()
        peer.port = addr.Port
        peer.wt = make(chan *Message)
        cluster.peers[peer.PeerID()] = peer
    }
    return cluster
}

func (cluster *Cluster) Start() {
    for _, peer := range cluster.peers {
        peer.Start()
    }
}

func (cluster *Cluster) AddClient(uid int64) {
    msg := &Message{cmd:MSG_ADD_CLIENT, body:uid}
    for _, peer := range cluster.peers {
        peer.wt <- msg
    }
}

func (cluster *Cluster) RemoveClient(uid int64) {
    msg := &Message{cmd:MSG_REMOVE_CLIENT, body:uid}
    for _, peer := range cluster.peers {
        peer.wt <- msg
    }
}
