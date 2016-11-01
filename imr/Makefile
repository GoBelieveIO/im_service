all:imr

imr:route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go group_manager.go group.go
	go build -o imr route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go group_manager.go group.go


clean:
	rm -f imr
