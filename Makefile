all:im benchmark benchmark_connection benchmark_sender main.test

im:im.go client.go route.go protocol.go storage.go group_server.go group_manager.go group.go set.go state_center.go config.go monitoring.go sio.go
	go build im.go client.go route.go protocol.go storage.go group_server.go group_manager.go group.go set.go state_center.go config.go monitoring.go sio.go

benchmark:benchmark.go protocol.go
	go build benchmark.go protocol.go

benchmark_connection:benchmark_connection.go protocol.go
	go build benchmark_connection.go protocol.go

benchmark_sender:benchmark_sender.go protocol.go
	go build benchmark_sender.go protocol.go

main.test:storage_test.go storage.go protocol.go
	go test -c  storage.go storage_test.go protocol.go

install:all
	cp im ./bin
	cp benchmark ./bin
	cp benchmark_connection ./bin
	cp benchmark_sender ./bin
	cp push.py ./bin
clean:
	rm -f im benchmark benchmark_connection benchmark_sender
