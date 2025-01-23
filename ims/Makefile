all:ims

ims:
	go build -ldflags "-X main.VERSION=2.0.0 -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X 'main.GIT_COMMIT_ID=`git log --pretty=format:"%h" -1`' -X 'main.GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`'" -o ./ims


ims_truncate:truncate.go
	go build -o ims_truncate truncate.go

main.test:storage_test.go storage.go storage_file.go peer_storage.go group_storage.go protocol.go message.go storage_message.go storage_sync.go config.go
	go test -c  storage.go storage_file.go peer_storage.go group_storage.go storage_test.go protocol.go message.go storage_message.go storage_sync.go config.go

clean:
	rm -f ims ims_trunncate main.test
