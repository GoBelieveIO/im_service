all:im benchmark

im:im.go peer.go peer_client.go client.go cluster.go route.go protocol.go storage.go group_server.go group_manager.go group.go set.go state_center.go
	go build im.go peer.go peer_client.go client.go cluster.go route.go protocol.go storage.go group_server.go group_manager.go group.go set.go state_center.go

benchmark:benchmark.go protocol.go
	go build benchmark.go protocol.go

clean:
	rm -f im benchmark
