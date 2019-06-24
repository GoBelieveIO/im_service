all:imr

imr:route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go rpc.go
	go build -ldflags "-X main.VERSION=2.0.0 -X 'main.BUILD_TIME=`date`' -X 'main.GO_VERSION=`go version`' -X 'main.GIT_COMMIT_ID=`git log --pretty=format:"%h" -1`' -X 'main.GIT_BRANCH=`git rev-parse --abbrev-ref HEAD`'" -o imr route_server.go client.go push.go route.go app_route.go protocol.go message.go config.go set.go route_message.go rpc.go


clean:
	rm -f imr
