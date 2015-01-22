package main

import "sync"
import log "github.com/golang/glog"

type AppRoute struct {
	mutex sync.Mutex
	apps  map[int64]*Route
}

func NewAppRoute() *AppRoute {
	app_route := new(AppRoute)
	app_route.apps = make(map[int64]*Route)
	return app_route
}

func (app_route *AppRoute) FindOrAddRoute(appid int64) *Route {
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	if route, ok := app_route.apps[appid]; ok {
		return route
	}
	route := NewRoute(appid)
	app_route.apps[appid] = route
	return route
}

func (app_route *AppRoute) FindRoute(appid int64) *Route{
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	return app_route.apps[appid]
}

func (app_route *AppRoute) AddRoute(route *Route) {
	app_route.mutex.Lock()
	defer app_route.mutex.Unlock()
	app_route.apps[route.appid] = route
}

type ClientSet map[*Client]struct{}

func NewClientSet() ClientSet {
	return make(map[*Client]struct{})
}

func (set ClientSet) Add(c *Client) {
	set[c] = struct{}{}
}

func (set ClientSet) IsMember(c *Client) bool {
	if _, ok := set[c]; ok {
		return true
	}
	return false
}

func (set ClientSet) Remove(c *Client) {
	if _, ok := set[c]; !ok {
		return
	}
	delete(set, c)
}

func (set ClientSet) Count() int {
	return len(set)
}

func (set ClientSet) Clone() ClientSet {
	n := make(map[*Client]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}

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

