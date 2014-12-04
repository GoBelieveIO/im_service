package main

import "net"
import "time"
import log "github.com/golang/glog"

type Cluster struct {
	peers map[int64]*Peer
	c     chan *Message
}

func NewCluster(addrs []*net.TCPAddr) *Cluster {
	cluster := new(Cluster)
	cluster.c = make(chan *Message)
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
	go cluster.Run()
}

func (cluster *Cluster) Run() {
	for {
		select {
		case msg := <-cluster.c:
			for _, peer := range cluster.peers {
				if !peer.Connected() {
					continue
				}
				select {
				case peer.wt <- msg:
				case <-time.After(1 * time.Second):
					log.Info("peer recieve message timeout")
				}
			}
		}
	}
}

func (cluster *Cluster) AddClient(uid int64, ts int32) {
	ac := &MessageAddClient{uid, ts}
	msg := &Message{cmd: MSG_ADD_CLIENT, body: ac}
	cluster.c <- msg
}

func (cluster *Cluster) RemoveClient(uid int64) {
	msg := &Message{cmd: MSG_REMOVE_CLIENT, body: uid}
	cluster.c <- msg
}
