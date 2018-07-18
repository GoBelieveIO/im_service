all:imr

imr:route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go group_manager.go group.go rpc.go
	go build -o imr route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go group_manager.go group.go rpc.go


clean:
	rm -f imr
