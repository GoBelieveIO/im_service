package main
import "sync"
import "log"

type Route struct {
    mutex sync.Mutex
    clients map[int64]*Client
}

func NewRoute() *Route {
    route := new(Route)
    route.clients = make(map[int64]*Client)
    return route
}

func (route *Route) AddClient(client *Client) {
    route.mutex.Lock()
    defer route.mutex.Unlock()
    if _, ok := route.clients[client.uid]; ok {
        log.Println("client exists")
    }
    route.clients[client.uid] = client
}

func (route *Route) RemoveClient(client *Client) {
    route.mutex.Lock()
    defer route.mutex.Unlock()
    if _, ok := route.clients[client.uid]; ok {
        delete(route.clients, client.uid)
    } else {
        log.Println("client non exists")
    }
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
