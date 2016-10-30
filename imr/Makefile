all:imr

imr:route_server.go app_route.go protocol.go config.go set.go route_message.go
	go build -o imr route_server.go app_route.go protocol.go config.go set.go route_message.go


clean:
	rm -f imr
