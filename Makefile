all:im benchmark

im:im.go peer.go peer_client.go client.go cluster.go route.go protocol.go storage.go
	go build im.go peer.go peer_client.go client.go cluster.go route.go protocol.go storage.go

benchmark:benchmark.go protocol.go
	go build benchmark.go protocol.go

clean:
	rm -f im benchmark
