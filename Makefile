all:im ims benchmark benchmark_connection benchmark_sender main.test benchmark_storage

im:im.go client.go route.go protocol.go group_server.go group_manager.go group.go set.go state_center.go config.go monitoring.go sio.go storage_client.go
	go build im.go client.go route.go protocol.go group_server.go group_manager.go group.go set.go state_center.go config.go monitoring.go sio.go storage_client.go

ims:storage_server.go protocol.go storage.go config.go
	go build -o ims storage_server.go protocol.go storage.go config.go

benchmark:benchmark.go protocol.go
	go build benchmark.go protocol.go

benchmark_connection:benchmark_connection.go protocol.go
	go build benchmark_connection.go protocol.go

benchmark_sender:benchmark_sender.go protocol.go
	go build benchmark_sender.go protocol.go

benchmark_storage:benchmark_storage.go storage_client.go protocol.go
	go build -o benchmark_storage benchmark_storage.go storage_client.go protocol.go

main.test:storage_test.go storage.go protocol.go
	go test -c  storage.go storage_test.go protocol.go

install:all
	cp im ./bin
	cp benchmark ./bin
	cp benchmark_connection ./bin
	cp benchmark_sender ./bin
	cp push.py ./bin
clean:
	rm -f im ims benchmark benchmark_connection benchmark_sender main.test
