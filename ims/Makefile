all:ims main.test

ims:storage_server.go storage_rpc.go rpc.go protocol.go message.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go
	go build -o ims storage_server.go storage_rpc.go rpc.go protocol.go message.go storage.go storage_file.go peer_storage.go group_storage.go config.go storage_message.go storage_sync.go 


main.test:storage_test.go storage.go storage_file.go peer_storage.go group_storage.go protocol.go message.go storage_message.go storage_sync.go config.go
	go test -c  storage.go storage_file.go peer_storage.go group_storage.go storage_test.go protocol.go message.go storage_message.go storage_sync.go config.go

clean:
	rm -f ims main.test
