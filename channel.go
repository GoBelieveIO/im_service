package main

import "net"
import "time"
import "sync"
import log "github.com/golang/glog"

type Subscriber struct {
	uids map[int64]int
}

func NewSubscriber() *Subscriber {
	s := new(Subscriber)
	s.uids = make(map[int64]int)
	return s
}

type Channel struct {
	addr string
	wt   chan *Message

	mutex       sync.Mutex
	subscribers map[int64]*Subscriber

	dispatch func(*AppMessage)
}

func NewChannel(addr string, f func(*AppMessage)) *Channel {
	channel := new(Channel)
	channel.subscribers = make(map[int64]*Subscriber)
	channel.dispatch = f
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

//返回添加前的计数
func (channel *Channel) AddSubscribe(appid, uid int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在count==0
	count := subscriber.uids[uid]
	subscriber.uids[uid] = count + 1
	return count
}

//返回删除前的计数
func (channel *Channel) RemoveSubscribe(appid, uid int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0
	}

	count, ok := subscriber.uids[uid]
	if ok {
		if count > 1 {
			subscriber.uids[uid] = count - 1
		} else {
			delete(subscriber.uids, uid)
		}
	}
	return count
}

func (channel *Channel) Subscribe(appid int64, uid int64) {
	count := channel.AddSubscribe(appid, uid)
	log.Info("sub count:", count)
	if count == 0 {
		id := &AppUserID{appid: appid, uid: uid}
		msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Unsubscribe(appid int64, uid int64) {
	count := channel.RemoveSubscribe(appid, uid)
	log.Info("unsub count:", count)
	if count == 1 {
		id := &AppUserID{appid: appid, uid: uid}
		msg := &Message{cmd: MSG_UNSUBSCRIBE, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Publish(amsg *AppMessage) {
	msg := &Message{cmd: MSG_PUBLISH, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) ReSubscribe(conn *net.TCPConn, seq int) int {
	for appid, s := range channel.subscribers {
		for uid, _ := range s.uids {
			id := &AppUserID{appid: appid, uid: uid}
			msg := &Message{cmd: MSG_SUBSCRIBE, body: id}
			seq = seq + 1
			msg.seq = seq
			SendMessage(conn, msg)
		}
	}
	return seq
}

func (channel *Channel) RunOnce(conn *net.TCPConn) {
	closed_ch := make(chan bool)
	seq := 0
	seq = channel.ReSubscribe(conn, seq)

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closed_ch)
				return
			}
			log.Info("channel recv message:", Command(msg.cmd))
			if msg.cmd == MSG_PUBLISH {
				amsg := msg.body.(*AppMessage)
				if channel.dispatch != nil {
					channel.dispatch(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.cmd)
			}
		}
	}()

	for {
		select {
		case _ = <-closed_ch:
			log.Info("channel closed")
			return
		case msg := <-channel.wt:
			seq = seq + 1
			msg.seq = seq
			SendMessage(conn, msg)
		}
	}
	conn.Close()
}

func (channel *Channel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", channel.addr)
		if err != nil {
			log.Info("connect storage server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		tconn.SetKeepAlive(true)
		tconn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
		log.Info("channel connected")
		nsleep = 100
		channel.RunOnce(tconn)
	}
}

func (channel *Channel) Start() {
	go channel.Run()
}
