package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GoBelieveIO/im_service/server"
	"github.com/gomodule/redigo/redis"
	"github.com/importcjj/sensitive"
)

func TestReadCfg(t *testing.T) {
	configContent := `port=23000
ssl_port=24430
pending_root="/tmp/pending"
memory_limit="2G"

[mysql]
user="root"
password="123#@@"
host="127.0.0.1"
port=3306
db_name="gobelieve"

[redis]
address="127.0.0.1:6379"
password=""
db=0
`

	configPath := filepath.Join(t.TempDir(), "im_test.cfg")
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("write config file failed: %v", err)
	}

	conf := read_cfg(configPath)
	if conf.Port != 23000 {
		t.Fatalf("unexpected port: %d", conf.Port)
	}
	log.Println("dsn:", conf.MySql.DSN())
	if conf.MySql.User != "root" || conf.MySql.Password != "123#@@" ||
		conf.MySql.Host != "127.0.0.1" || conf.MySql.Port != 3306 || conf.MySql.DBName != "gobelieve" {
		t.Fatalf("unexpected mysql config: %+v", conf.MySql)
	}

	expectedDSNPrefix := "root:123#@@@tcp(127.0.0.1:3306)/gobelieve"

	if !strings.HasPrefix(conf.MySql.DSN(), expectedDSNPrefix) {
		t.Fatalf("unexpected mysql dsn: %s", conf.MySql.DSN())
	}

	if conf.AuthMethod != "redis" {
		t.Fatalf("unexpected default auth method: %s", conf.AuthMethod)
	}

	if conf.memory_limit != 2*1024*1024*1024 {
		t.Fatalf("unexpected memory limit: %d", conf.memory_limit)
	}
}

func TestFilter(t *testing.T) {
	filter := sensitive.New()

	err := filter.LoadWordDict("../bin/dict.txt")
	if err != nil {
		log.Println("Load word dict err:", err)
	}
	filter.AddWord("长者")

	msg := &server.IMMessage{}

	//TODO
	//msg.content = "{\"text\": \"\\u6211\\u4e3a\\u5171*\\u4ea7\\u515a\\u7eed\\u4e00\\u79d2\"}"
	server.FilterDirtyWord(filter, msg)
	//log.Println("msg:", string(msg.content))

	s := "我为共*产党续一秒"
	t1 := filter.RemoveNoise(s)
	log.Println(filter.Replace(t1, '*'))
	e, t2 := filter.FindIn(s)
	log.Println(e, t2)
}

func TestConfig(t *testing.T) {
	conf := read_cfg("../bin/im.cfg")
	log.Println("config:", conf)
	log.Println("redis config:", conf.Redis)
	log.Println("log config:", conf.Log)
	log.Println("route channels:", conf.RouteAddrs)
}

func TestRelationship(t *testing.T) {
	config := read_cfg("../bin/im.cfg")
	redis_pool := NewRedisPool(config.Redis.Address, config.Redis.Password,
		config.Redis.Db)
	relationship_pool := server.NewRelationshipPool(config.MySql.DSN(), redis_pool)
	rs := relationship_pool.GetRelationship(7, 1, 2)
	log.Println("rs:", rs, rs.IsMyFriend(), rs.IsYourFriend(), rs.IsInMyBlacklist(), rs.IsInYourBlacklist())

	relationship_pool.SetMyFriend(7, 1, 2, true)
	relationship_pool.SetYourFriend(7, 1, 2, true)
	relationship_pool.SetInMyBlacklist(7, 1, 2, true)
	relationship_pool.SetInYourBlacklist(7, 1, 2, true)

	rs = relationship_pool.GetRelationship(7, 1, 2)

	log.Println("rs:", rs, rs.IsMyFriend(), rs.IsYourFriend(), rs.IsInMyBlacklist(), rs.IsInYourBlacklist())

	if !rs.IsMyFriend() || !rs.IsYourFriend() || !rs.IsInMyBlacklist() || !rs.IsInYourBlacklist() {
		t.Error("error")
		t.FailNow()
	}

	log.Println("rs:", rs, rs.IsMyFriend(), rs.IsYourFriend(), rs.IsInMyBlacklist(), rs.IsInYourBlacklist())

	relationship_pool.SetMyFriend(7, 1, 2, false)
	relationship_pool.SetYourFriend(7, 1, 2, false)
	relationship_pool.SetInMyBlacklist(7, 1, 2, false)
	relationship_pool.SetInYourBlacklist(7, 1, 2, false)

	rs = relationship_pool.GetRelationship(7, 1, 2)

	if rs.IsMyFriend() || rs.IsYourFriend() || rs.IsInMyBlacklist() || rs.IsInYourBlacklist() {
		t.Error("error")
		t.FailNow()
	}

	log.Println("rs:", rs, rs.IsMyFriend(), rs.IsYourFriend(), rs.IsInMyBlacklist(), rs.IsInYourBlacklist())

}

func TestStreamRange(t *testing.T) {
	config := read_cfg("../bin/im.cfg")
	redis_pool := NewRedisPool(config.Redis.Address, config.Redis.Password,
		config.Redis.Db)
	conn := redis_pool.Get()
	defer conn.Close()

	r, err := redis.Values(conn.Do("XREVRANGE", "test_stream", "+", "-", "COUNT", "1"))

	if err != nil {
		log.Println("redis err:", err)
		return
	}

	for len(r) > 0 {
		var entries []interface{}
		r, err = redis.Scan(r, &entries)
		if err != nil {
			t.Error("redis err:", err)
			return
		}

		var id string
		var fields []interface{}
		_, err = redis.Scan(entries, &id, &fields)
		if err != nil {
			t.Error("redis err:", err)
			return
		}
		log.Println("id:", id)

		event := &server.GroupEvent{}
		event.Id = id
		err = redis.ScanStruct(fields, event)
		if err != nil {
			log.Println("scan err:", err)
		}
		log.Println("event:", event.Id, event.Name, event.GroupId, event.MemberId, event.IsSuper)
	}
}

func TestStreamRead(t *testing.T) {
	config := read_cfg("../bin/im.cfg")
	redis_pool := NewRedisPool(config.Redis.Address, config.Redis.Password,
		config.Redis.Db)

	conn := redis_pool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("XREAD", "COUNT", "2", "STREAMS", "test_stream", "0-0"))

	if err != nil {
		log.Println("redis err:", err)
		return
	}

	var stream_res []interface{}
	_, err = redis.Scan(reply, &stream_res)
	if err != nil {
		log.Println("redis scan err:", err)
		return
	}

	var ss string
	var r []interface{}
	_, err = redis.Scan(stream_res, &ss, &r)
	if err != nil {
		log.Println("redis scan err:", err)
		return
	}

	for len(r) > 0 {
		var entries []interface{}
		r, err = redis.Scan(r, &entries)
		if err != nil {
			t.Error("redis err:", err)
			return
		}

		var id string
		var fields []interface{}
		_, err = redis.Scan(entries, &id, &fields)
		if err != nil {
			t.Error("redis err:", err)
			return
		}
		log.Println("id:", id)

		event := &server.GroupEvent{}
		event.Id = id
		err = redis.ScanStruct(fields, event)
		if err != nil {
			log.Println("scan err:", err)
		}
		log.Println("event:", event.Id, event.Name, event.GroupId, event.MemberId, event.IsSuper)
	}
}
