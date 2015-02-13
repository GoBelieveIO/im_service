all:im ims imr im_api benchmark benchmark_connection benchmark_sender main.test benchmark_storage benchmark_route

im:im.go client.go route.go app_route.go protocol.go  group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go login.go storage_message.go route_message.go user.go
	go build im.go client.go route.go app_route.go protocol.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go login.go storage_message.go route_message.go user.go

ims:storage_server.go protocol.go storage.go config.go storage_message.go storage_sync.go
	go build -o ims storage_server.go protocol.go storage.go config.go storage_message.go storage_sync.go

imr:route_server.go app_route.go protocol.go config.go set.go route_message.go 
	go build -o imr route_server.go app_route.go protocol.go config.go set.go route_message.go 

im_api:api.go group_server.go group_manager.go config.go protocol.go group.go set.go storage_message.go storage_client.go route_message.go channel.go auth.go user.go
	go build -o im_api api.go group_server.go group_manager.go config.go protocol.go group.go set.go storage_message.go storage_client.go route_message.go channel.go auth.go user.go

benchmark:benchmark.go protocol.go
	go build benchmark.go protocol.go

benchmark_connection:benchmark_connection.go protocol.go
	go build benchmark_connection.go protocol.go

benchmark_sender:benchmark_sender.go protocol.go
	go build benchmark_sender.go protocol.go

benchmark_storage:benchmark_storage.go storage_client.go protocol.go storage_message.go
	go build -o benchmark_storage benchmark_storage.go storage_client.go protocol.go storage_message.go

benchmark_route:benchmark_route.go channel.go protocol.go route_message.go
	go build -o benchmark_route benchmark_route.go channel.go protocol.go route_message.go

main.test:storage_test.go storage.go protocol.go storage_message.go storage_sync.go
	go test -c  storage.go storage_test.go protocol.go storage_message.go storage_sync.go

install:all
	cp im ./bin
	cp ims ./bin
	cp imr ./bin
	cp im_api ./bin
	cp benchmark ./bin
	cp benchmark_connection ./bin
	cp benchmark_sender ./bin

clean:
	rm -f im ims imr im_api benchmark benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test
