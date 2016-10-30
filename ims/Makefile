all:ims main.test

ims:storage_server.go protocol.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go group_manager.go group.go set.go route_message.go app_route.go
	go build -o ims storage_server.go protocol.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go group_manager.go group.go set.go route_message.go app_route.go


main.test:storage_test.go storage.go storage_file.go peer_storage.go group_storage.go protocol.go storage_message.go storage_sync.go set.go route_message.go
	go test -c  storage.go storage_file.go peer_storage.go group_storage.go storage_test.go protocol.go storage_message.go storage_sync.go set.go route_message.go

clean:
	rm -f ims main.test
