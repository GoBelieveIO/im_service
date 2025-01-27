package main

import (
	"log"
	"testing"

	"github.com/GoBelieveIO/im_service/server"
	"github.com/gomodule/redigo/redis"
	"github.com/importcjj/sensitive"
)

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

func Test_Relationship(t *testing.T) {
	config := read_cfg("../bin/im.cfg")
	redis_pool := NewRedisPool(config.redis_address, config.redis_password,
		config.redis_db)
	relationship_pool := server.NewRelationshipPool(config.mysqldb_datasource, redis_pool)
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
	redis_pool := NewRedisPool(config.redis_address, config.redis_password,
		config.redis_db)
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
	redis_pool := NewRedisPool(config.redis_address, config.redis_password,
		config.redis_db)
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
