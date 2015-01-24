package main

import "sync"
import log "github.com/golang/glog"


type Route struct {
	appid  int64
	mutex   sync.Mutex
	clients map[int64]ClientSet
}

func NewRoute(appid int64) *Route {
	route := new(Route)
	route.appid = appid
	route.clients = make(map[int64]ClientSet)
	return route
}

func (route *Route) AddClient(client *Client) {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	set, ok := route.clients[client.uid]; 
	if !ok {
		set = NewClientSet()
		route.clients[client.uid] = set
	}
	set.Add(client)
}

func (route *Route) RemoveClient(client *Client) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	if set, ok := route.clients[client.uid]; ok {
		set.Remove(client)
		if set.Count() == 0 {
			delete(route.clients, client.uid)
		}
		return true
	}
	log.Info("client non exists")
	return false
}

func (route *Route) FindClientSet(uid int64) ClientSet {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	set, ok := route.clients[uid]
	if ok {
		return set.Clone()
	} else {
		return nil
	}
}

func (route *Route) GetClientUids() map[int64]int32 {
	return nil
	// route.mutex.Lock()
	// defer route.mutex.Unlock()
	// uids := make(map[int64]int32)
	// for uid, c := range route.clients {
	// 	uids[uid] = int32(c.tm.Unix())
	// }
	// return uids
}

