package main

import "sync"
import log "github.com/golang/glog"

//一个聊天室中不允许有多个相同uid的client
const ROOM_SINGLE = false
//一个聊天室中不允许有多个相同uid和deviceid的client
const ROOM_DEVICE_SINGLE = true

type Route struct {
	appid  int64
	mutex   sync.Mutex
	clients map[int64]ClientSet
	room_clients map[int64]ClientSet
}

func NewRoute(appid int64) *Route {
	route := new(Route)
	route.appid = appid
	route.clients = make(map[int64]ClientSet)
	route.room_clients = make(map[int64]ClientSet)
	return route
}

func (route *Route) GetRoomCount(low_level int) (int, int, map[int64]int) {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	stat := make(map[int64]int)
	count := 0
	for k, v := range(route.room_clients) {
		if len(v) >= low_level {
			stat[k] = len(v)
		}
		count += len(v)
	}
	return len(route.room_clients), count, stat
}

func (route *Route) AddRoomClient(room_id int64, client *Client) {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	set, ok := route.room_clients[room_id]; 
	if !ok {
		set = NewClientSet()
		route.room_clients[room_id] = set
	}

	if ROOM_SINGLE {
		var old *Client = nil
		for k, _ := range set {
			//根据uid去重
			if k.uid == client.uid {
				old = k
				break
			}
		}
		if old != nil {
			set.Remove(old)
		}
	} else if ROOM_DEVICE_SINGLE && client.device_ID > 0 {
		var old *Client = nil
		for k, _ := range set {
			//根据uid&device_id去重
			if k.uid == client.uid && k.device_ID == client.device_ID {
				old = k
				break
			}
		}
		if old != nil {
			set.Remove(old)
		}
	}
	set.Add(client)
}


//todo optimise client set clone
func (route *Route) FindRoomClientSet(room_id int64) ClientSet {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	set, ok := route.room_clients[room_id]
	if ok {
		return set.Clone()
	} else {
		return nil
	}
}

func (route *Route) RemoveRoomClient(room_id int64, client *Client) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	if set, ok := route.room_clients[room_id]; ok {
		set.Remove(client)
		if set.Count() == 0 {
			delete(route.room_clients, room_id)
		}
		return true
	}
	log.Info("room client non exists")
	return false
}


func (route *Route) GetClientCount() (int, int) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	count := 0
	for _, v := range(route.clients) {
		count += len(v)
	}
	return len(route.clients), count
}

func (route *Route) AddClient(client *Client) bool {
	is_new := false
	route.mutex.Lock()
	defer route.mutex.Unlock()
	set, ok := route.clients[client.uid]; 
	if !ok {
		set = NewClientSet()
		route.clients[client.uid] = set
		is_new = true
	}
	set.Add(client)
	return is_new
}

func (route *Route) RemoveClient(client *Client) (bool, bool) {
	route.mutex.Lock()
	defer route.mutex.Unlock()
	if set, ok := route.clients[client.uid]; ok {
		is_delete := false		
		set.Remove(client)
		if set.Count() == 0 {
			delete(route.clients, client.uid)
			is_delete = true
		}
		return true, is_delete
	}
	log.Info("client non exists")
	return false, false
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

func (route *Route) IsOnline(uid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	set, ok := route.clients[uid]
	if ok {
		return len(set) > 0
	}
	return false
}


func (route *Route) GetUserIDs() IntSet {
	return NewIntSet()
}
