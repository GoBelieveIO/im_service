package main

import "net"
import "sync"
import log "github.com/golang/glog"

type PeerClient struct {
	wt   chan *Message
	conn *net.TCPConn

	mutex sync.Mutex
	uids  IntSet
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
		msg := ReceiveMessage(peer.conn)
		if msg == nil {
			route.RemovePeerClient(peer)
			peer.wt <- nil
			peer.PublishOffline()
			break
		}
		log.Info("msg:", msg.cmd)
		if msg.cmd == MSG_ADD_CLIENT {
			peer.HandleAddClient(msg.body.(*MessageAddClient))
		} else if msg.cmd == MSG_REMOVE_CLIENT {
			peer.HandleRemoveClient(msg.body.(int64))
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
			c.wt <- &Message{cmd: MSG_RST}
		}
	}
}

func (peer *PeerClient) PublishOffline() {
	peer.mutex.Lock()
	uids := make([]int64, len(peer.uids))
	i := 0
	for uid, _ := range peer.uids {
		uids[i] = uid
		i++
	}
	peer.mutex.Unlock()
	for _, uid := range uids {
		peer.PublishState(uid, false)
	}
}

func (peer *PeerClient) PublishState(uid int64, online bool) {
	subs := state_center.FindSubsriber(uid)
	state := &MessageOnlineState{uid, 0}
	if online {
		state.online = 1
	}

	log.Info("publish online state")
	set := NewIntSet()
	msg := &Message{cmd: MSG_ONLINE_STATE, body: state}
	for _, sub := range subs {
		log.Info("send online state:", sub)
		other := route.FindClient(sub)
		if other != nil {
			other.wt <- msg
		} else {
			set.Add(sub)
		}
	}
	if len(set) > 0 {
		state_center.Unsubscribe(uid, set)
	}
}

func (peer *PeerClient) HandleAddClient(ac *MessageAddClient) {
	peer.mutex.Lock()
	uid := ac.uid
	if peer.uids.IsMember(uid) {
		log.Infof("uid:%d exists\n", uid)
		peer.mutex.Unlock()
		return
	}
	log.Info("add uid:", uid)
	peer.uids.Add(uid)
	peer.mutex.Unlock()

	peer.ResetClient(uid, ac.timestamp)
	peer.PublishState(uid, true)

	c := storage.LoadOfflineMessage(uid)
	if c != nil {
		for m := range c {
			peer.wt <- m
		}
		storage.ClearOfflineMessage(uid)
	}
}

func (peer *PeerClient) HandleRemoveClient(uid int64) {
	peer.mutex.Lock()
	peer.uids.Remove(uid)
	peer.mutex.Unlock()
	peer.PublishState(uid, false)
	log.Info("remove uid:", uid)
}

func (peer *PeerClient) Write() {
	for {
		msg := <-peer.wt
		if msg == nil {
			log.Info("socket closed")
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
