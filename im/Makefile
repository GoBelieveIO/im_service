all:im benchmark benchmark_group benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test

#dummy_grpc.go <=> grpc.go
im: im_server.go im.go subscriber.go connection.go client.go peer_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go  storage_rpc.go channel.go storage_message.go route_message.go user.go rpc.go dummy_grpc.go device.go group_message_deliver.go relationship.go relationship_pool.go
	go build -ldflags "-X main.VERSION=2.0.0 -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X 'main.GIT_COMMIT_ID=`git log --pretty=format:"%h" -1`' -X 'main.GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`'" -o im im_server.go im.go subscriber.go connection.go client.go peer_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go storage_rpc.go channel.go storage_message.go route_message.go user.go rpc.go dummy_grpc.go device.go group_message_deliver.go relationship.go relationship_pool.go



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


main.test:im_test.go user.go relationship.go relationship_pool.go protocol.go message.go config.go
	go test -c  im_test.go user.go relationship.go relationship_pool.go protocol.go message.go config.go


clean:
	rm -f im benchmark benchmark_group benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test
