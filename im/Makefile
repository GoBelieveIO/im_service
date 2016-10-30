all:im benchmark benchmark_group benchmark_connection benchmark_sender benchmark_storage benchmark_route

im:im.go connection.go client.go im_client.go room_client.go voip_client.go customer_service_client.go route.go app_route.go customer_service.go protocol.go  group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go storage_message.go route_message.go user.go reload.go rpc.go storage_channel.go group_center.go device.go
	go build im.go connection.go client.go im_client.go room_client.go voip_client.go customer_service_client.go route.go app_route.go customer_service.go protocol.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go storage_message.go route_message.go user.go reload.go rpc.go storage_channel.go group_center.go device.go



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


clean:
	rm -f im
