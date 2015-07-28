all:im ims imr im_api benchmark benchmark_group benchmark_connection benchmark_sender main.test benchmark_storage benchmark_route

im:im.go client.go route.go app_route.go protocol.go  group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go storage_message.go route_message.go user.go reload.go rpc.go storage_channel.go
	go build im.go client.go route.go app_route.go protocol.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_client.go channel.go storage_message.go route_message.go user.go reload.go rpc.go storage_channel.go

ims:storage_server.go protocol.go storage.go config.go storage_message.go storage_sync.go group_manager.go group.go set.go comparer.go route_message.go app_route.go
	go build -o ims storage_server.go protocol.go storage.go config.go storage_message.go storage_sync.go group_manager.go group.go set.go comparer.go route_message.go app_route.go

imr:route_server.go app_route.go protocol.go config.go set.go route_message.go
	go build -o imr route_server.go app_route.go protocol.go config.go set.go route_message.go

im_api:api.go group_server.go config.go group.go set.go  auth.go user.go reload.go
	go build -o im_api api.go group_server.go config.go group.go set.go auth.go user.go reload.go

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

main.test:storage_test.go storage.go protocol.go storage_message.go storage_sync.go comparer.go set.go
	go test -c  storage.go storage_test.go protocol.go storage_message.go storage_sync.go comparer.go set.go

install:all
	cp im ./bin
	cp ims ./bin
	cp imr ./bin
	cp im_api ./bin

clean:
	rm -f im ims imr im_api benchmark benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test
