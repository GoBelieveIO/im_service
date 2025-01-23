all:benchmark benchmark_group benchmark_room benchmark_connection benchmark_sender benchmark_storage benchmark_route

benchmark:benchmark.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark.go benchmark_login.go protocol.go message.go im_message.go;

benchmark_group:benchmark_group.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_group.go benchmark_login.go protocol.go message.go im_message.go;

benchmark_room:benchmark_room.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_room.go benchmark_login.go protocol.go message.go im_message.go;

benchmark_connection:benchmark_connection.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_connection.go benchmark_login.go protocol.go message.go im_message.go;

benchmark_sender:benchmark_sender.go benchmark_login.go protocol.go message.go im_message.go
	go build benchmark_sender.go benchmark_login.go protocol.go message.go im_message.go;

benchmark_storage:benchmark_storage.go rpc.go protocol.go message.go im_message.go 
	go build -o benchmark_storage benchmark_storage.go rpc.go protocol.go message.go im_message.go;

benchmark_route:benchmark_route.go channel.go protocol.go message.go route_message.go
	go build -o benchmark_route benchmark_route.go channel.go protocol.go message.go im_message.go route_message.go;

clean:
	rm -f benchmark benchmark_group benchmark_room benchmark_connection benchmark_sender benchmark_storage benchmark_route
