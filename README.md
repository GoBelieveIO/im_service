
#im service
1. 支持点对点消息, 群组消息, 聊天室消息
2. 支持集群部署
3. 单机支持50w用户在线
4. 单机处理消息5000条/s
5. 支持超大群组(3000人)


##编译运行

1. 安装go编译环境

   参考链接:https://golang.org/doc/install

2. 下载im_service代码

   cd $GOPATH/src/github.com/GoBelieveIO
   
   git clone https://github.com/GoBelieveIO/im_service.git

3. 编译proto文件

   cd im_service

   //注意需要翻墙

   go get google.golang.org/grpc

   go get -u github.com/golang/protobuf/{proto,protoc-gen-go}

   export PATH=$PATH:$GOPATH/bin

   protoc -Irpc/ rpc/rpc.proto --go_out=plugins=grpc:rpc

   python -m grpc.tools.protoc -Irpc --python_out=rpc/ --grpc_python_out=rpc/ rpc/rpc.proto

4. 编译

  cd im_service

  mkdir bin

  go get github.com/bitly/go-simplejson

  go get github.com/golang/glog

  go get github.com/go-sql-driver/mysql

  go get github.com/garyburd/redigo/redis

  go get github.com/googollee/go-engine.io

  go get github.com/richmonkey/cfg

  go get github.com/syndtr/goleveldb/leveldb/opt

  go get github.com/syndtr/goleveldb/leveldb

  go get github.com/valyala/gorpc

  //注意需要翻墙

  go get google.golang.org/grpc

  make install

  可执行程序在bin目录下

5. 安装mysql数据库, redis, 并导入db.sql

6. 配置程序
   配置项的说明参考ims.cfg.sample, imr.cfg.sample, im.cfg.sample


7. 启动程序

    创建ims消息存放路径

    创建日志文件路径
    mkdir /data/logs/ims
    mkdir /data/logs/imr
    mkdir /data/logs/im


    pushd `dirname $0` > /dev/null
    BASEDIR=`pwd`

    nohup $BASEDIR/ims -log_dir=/data/logs/ims ims.cfg >/data/logs/ims/ims.log 2>&1 &

    nohup $BASEDIR/imr -log_dir=/data/logs/imr imr.cfg >/data/logs/imr/imr.log 2>&1 &

    nohup $BASEDIR/im -log_dir=/data/logs/im im.cfg >/data/logs/im/im.log 2>&1 &



##官方QQ群
1. 450359487

##官方网站
   https://developer.gobelieve.io/
