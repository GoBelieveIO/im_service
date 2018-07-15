all:im benchmark benchmark_group benchmark_connection benchmark_sender benchmark_storage benchmark_route

#dummy_grpc.go <=> grpc.go
im:im.go subscriber.go connection.go client.go im_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go  storage_rpc.go channel.go storage_message.go route_message.go user.go rpc.go dummy_grpc.go device.go group_message_deliver.go
	go build im.go subscriber.go connection.go client.go im_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_rpc.go channel.go storage_message.go route_message.go user.go rpc.go dummy_grpc.go device.go group_message_deliver.go



benchmark:benchmark.go protocol.go message.go
	go build benchmark.go protocol.go message.go

benchmark_group:benchmark_group.go protocol.go message.go
	go build benchmark_group.go protocol.go message.go

benchmark_connection:benchmark_connection.go protocol.go message.go
	go build benchmark_connection.go protocol.go message.go

benchmark_sender:benchmark_sender.go protocol.go message.go
	go build benchmark_sender.go protocol.go message.go

benchmark_storage:benchmark_storage.go storage_rpc.go protocol.go message.go storage_message.go
	go build -o benchmark_storage benchmark_storage.go storage_rpc.go protocol.go message.go storage_message.go

benchmark_route:benchmark_route.go channel.go protocol.go message.go route_message.go
	go build -o benchmark_route benchmark_route.go channel.go protocol.go message.go route_message.go


main.test:im_test.go user.go
	go test -c  im_test.go user.go


clean:
	rm -f im benchmark benchmark_group benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test
