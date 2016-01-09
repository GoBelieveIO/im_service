all:im ims imr benchmark benchmark_group benchmark_connection benchmark_sender main.test benchmark_storage benchmark_route

im:im.go connection.go client.go im_client.go room_client.go route.go app_route.go protocol.go  group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go storage_message.go route_message.go user.go reload.go rpc.go storage_channel.go group_center.go device.go
	go build im.go connection.go client.go im_client.go room_client.go route.go app_route.go protocol.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go storage_message.go route_message.go user.go reload.go rpc.go storage_channel.go group_center.go device.go

ims:storage_server.go protocol.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go group_manager.go group.go set.go route_message.go app_route.go
	go build -o ims storage_server.go protocol.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go group_manager.go group.go set.go route_message.go app_route.go

imr:route_server.go app_route.go protocol.go config.go set.go route_message.go
	go build -o imr route_server.go app_route.go protocol.go config.go set.go route_message.go

benchmark:benchmark.go protocol.go
	go build benchmark.go protocol.go

benchmark_group:benchmark_group.go protocol.go
	go build benchmark_group.go protocol.go

benchmark_connection:benchmark_connection.go protocol.go
	go build benchmark_connection.go protocol.go

benchmark_sender:benchmark_sender.go protocol.go
	go build benchmark_sender.go protocol.go

benchmark_storage:benchmark_storage.go storage_client.go protocol.go storage_message.go
	go build -o benchmark_storage benchmark_storage.go storage_client.go protocol.go storage_message.go

benchmark_route:benchmark_route.go channel.go protocol.go route_message.go
	go build -o benchmark_route benchmark_route.go channel.go protocol.go route_message.go

main.test:storage_test.go storage.go storage_file.go peer_storage.go group_storage.go protocol.go storage_message.go storage_sync.go set.go route_message.go
	go test -c  storage.go storage_file.go peer_storage.go group_storage.go storage_test.go protocol.go storage_message.go storage_sync.go set.go route_message.go

install:all
	cp im ./bin
	cp ims ./bin
	cp imr ./bin

clean:
	rm -f im ims imr benchmark benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test
