module github.com/GoBelieveIO/im_service

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/bitly/go-simplejson v0.5.0
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gomodule/redigo v1.8.1
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/websocket v1.4.2
	github.com/importcjj/sensitive v0.0.0-20190611120559-289e87ec4108
	github.com/jackc/puddle v1.1.4 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/richmonkey/cfg v0.0.0-20130815005846-4b1e3c1869d4
	github.com/sirupsen/logrus v1.6.0
	github.com/valyala/gorpc v0.0.0-20160519171614-908281bef774
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace github.com/GoBelieveIO/im_service/lru => ./lru
