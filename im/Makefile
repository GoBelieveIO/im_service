all:im benchmark benchmark_group benchmark_storage benchmark_room benchmark_connection benchmark_sender benchmark_route main.test

im: im_server.go im.go subscriber.go connection.go client.go peer_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go  channel.go im_message.go route_message.go user.go auth.go rpc.go api.go device.go group_message_deliver.go relationship.go relationship_pool.go
	go build -ldflags "-X main.VERSION=2.0.0 -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X 'main.GIT_COMMIT_ID=`git log --pretty=format:"%h" -1`' -X 'main.GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`'" -o im im_server.go im.go subscriber.go connection.go client.go peer_client.go group_client.go room_client.go customer_client.go route.go app_route.go protocol.go message.go group_manager.go group.go set.go config.go monitoring.go sio.go channel.go im_message.go route_message.go user.go auth.go rpc.go api.go device.go group_message_deliver.go relationship.go relationship_pool.go



benchmark:benchmark.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark.go benchmark_login.go protocol.go message.go im_message.go

benchmark_group:benchmark_group.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_group.go benchmark_login.go protocol.go message.go im_message.go

benchmark_room:benchmark_room.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_room.go benchmark_login.go protocol.go message.go im_message.go

benchmark_connection:benchmark_connection.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_connection.go benchmark_login.go protocol.go message.go im_message.go

benchmark_sender:benchmark_sender.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_sender.go benchmark_login.go protocol.go message.go im_message.go

benchmark_storage:benchmark_storage.go rpc.go protocol.go message.go im_message.go 
	go build -o benchmark_storage benchmark_storage.go rpc.go protocol.go message.go im_message.go 

benchmark_route:benchmark_route.go channel.go protocol.go message.go route_message.go
	go build -o benchmark_route benchmark_route.go channel.go protocol.go message.go im_message.go route_message.go


main.test:im_test.go user.go relationship.go relationship_pool.go protocol.go message.go im_message.go config.go
	go test -c  im_test.go user.go relationship.go relationship_pool.go protocol.go message.go im_message.go config.go


clean:
	rm -f im benchmark benchmark_group benchmark_room benchmark_connection benchmark_sender benchmark_storage benchmark_route main.test
