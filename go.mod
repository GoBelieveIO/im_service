module github.com/GoBelieveIO/im_service

go 1.12

require (
	github.com/bitly/go-simplejson v0.5.0
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.3.2
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/googollee/go-engine.io v0.0.0-20180611083002-3c3145340e67
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.0
	github.com/importcjj/sensitive v0.0.0-20190611120559-289e87ec4108
	github.com/kr/pretty v0.1.0 // indirect
	github.com/richmonkey/cfg v0.0.0-20130815005846-4b1e3c1869d4
	github.com/smartystreets/goconvey v0.0.0-20190731233626-505e41936337 // indirect
	github.com/valyala/gorpc v0.0.0-20160519171614-908281bef774
	golang.org/x/net v0.0.0-20190813141303-74dc4d7220e7
	google.golang.org/grpc v1.23.0
)

replace github.com/GoBelieveIO/im_service/lru => ./lru
