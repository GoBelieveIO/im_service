package main

import "log"
import "net"
import "fmt"

var route *Route

func handle_client(conn *net.TCPConn) {
    client := NewClient(conn)
    client.Run()
}

func init() {
    route = NewRoute()
    log.Println("init.....")
}

func main() {
	log.SetFlags(log.Lshortfile|log.LstdFlags)
	ip := net.ParseIP("0.0.0.0")
	addr := net.TCPAddr{ip, 23000, ""}

	listen, err := net.ListenTCP("tcp", &addr);
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	for {
		client, err := listen.AcceptTCP();
		if err != nil {
			return
		}
		handle_client(client)
	}
}
