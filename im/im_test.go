package main

import "testing"
import "log"
import "time"
import "flag"
import "os"
import "fmt"
import "context"
import "github.com/go-redis/redis/v8"
import "github.com/importcjj/sensitive"
import "github.com/bitly/go-simplejson"
import "github.com/GoBelieveIO/im_service/hscan"

var redis_client *redis.Client
var filter *sensitive.Filter
var config *Config
var relationship_pool *RelationshipPool

func init() {
	filter = sensitive.New()
}

type GroupEvent struct {
	Id string //stream entry id
	ActionId int64 `redis:"action_id"`
	PreviousActionId int64 `redis:"previous_action_id"`
	Name string `redis:"name"`
	AppId int64 `redis:"app_id"`
	GroupId int64 `redis:"group_id"`
	MemberId int64 `redis:"member_id"`
	IsSuper bool `redis:"super"`
	IsMute bool `redis:"mute"`
}



func NewRedisClient(server, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: server,
		Password:password,
		DB:db,
		IdleTimeout: 480 * time.Second,
	});
}

func TestMain(m *testing.M) {
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Println("usage:main.test config_file")
		return
	}

	config = read_cfg(flag.Args()[0])
	
	relationship_pool = NewRelationshipPool()
	redis_client = NewRedisClient(config.redis_address, config.redis_password, 
		config.redis_db)	

	filter.LoadWordDict(config.word_file)
	filter.AddWord("长者")
	
	os.Exit(m.Run())
}


func FilterDirtyWord(msg *IMMessage) {
	if filter == nil {
		return
	}

	obj, err := simplejson.NewJson([]byte(msg.content))
	if err != nil {
		return
	}

	text, err := obj.Get("text").String()
	if err != nil {
		return
	}

	t := filter.RemoveNoise(text)
	replacedText := filter.Replace(t, 42)

	if (replacedText != text) {
		obj.Set("text", replacedText)

		c, err := obj.Encode()
		if err != nil {
			return
		}
		msg.content = string(c)
	}
}


func TestFilter(t *testing.T) {
	log.Println("hhhhh test")

	msg := &IMMessage{}
	
	msg.content = "{\"text\": \"\\u6211\\u4e3a\\u5171*\\u4ea7\\u515a\\u7eed\\u4e00\\u79d2\"}"
	FilterDirtyWord(msg)
	log.Println("msg:", string(msg.content))

	
	s := "我为党续一秒"
	t1 := filter.RemoveNoise(s)
	log.Println(filter.Replace(t1, '*'))
	e, t2 := filter.FindIn(s)
	log.Println(e, t2)
}

func Test_Relationship(t *testing.T) {
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
	var ctx = context.Background()

	vals := map[string]interface{}{
		"action_id":0,
		"previous_action_id":0,
		"name":"test",
		"app_id":7,
		"group_id":12,
		"member_id":100,
		"super":true,
		"mute":true,
	}

	args := &redis.XAddArgs{
		Stream:"test_stream",
		ID:"*",
		Values:vals,
	}	
	id, err := redis_client.XAdd(ctx, args).Result()

	if err != nil {
		log.Println("xadd err:", err)
		return
	}
	log.Println("xadd res:", id)
	
	r, err := redis_client.XRevRangeN(ctx, "test_stream", "+", "-", 1).Result()
	if err != nil {
		log.Println("redis err:", err)
		return
	}
	for _, m := range(r) {
		event := &GroupEvent{}
		event.Id = m.ID
		err = hscan.ScanMap(event, m.Values)
		if err != nil {
			log.Println("scan err:", err)
		}
		log.Println("event:", event.Id, event.Name, event.GroupId, event.MemberId, event.IsSuper)
	}
}

func getLastId(stream string) string {
	lastId := "0-0"	
	var ctx = context.Background()	
	r, err := redis_client.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	if err != nil {
		log.Println("redis err:", err)
		return lastId
	}

	for _, m := range(r) {
		lastId = m.ID
	}
	return lastId
}

func TestStreamRead(t *testing.T) {
	var ctx = context.Background()

	vals := map[string]interface{}{
		"action_id":0,
		"previous_action_id":0,
		"name":"test",
		"app_id":7,
		"group_id":11,
		"member_id":100,
		"super":true,
		"mute":true,
	}

	args := &redis.XAddArgs{
		Stream:"test_stream",
		ID:"*",
		Values:vals,
	}	
	id, err := redis_client.XAdd(ctx, args).Result()

	if err != nil {
		log.Println("xadd err:", err)
		return
	}
	log.Println("xadd res:", id)
	
	r, err := redis_client.XRead(ctx, &redis.XReadArgs{Streams:[]string{"test_stream", "0-0"}, Count:2}).Result()

	if err != nil {
		log.Println("redis err:", err)
		return
	}

	if len(r) != 1 {
		return
	}

	s := r[0]
	for _, m := range(s.Messages) {
		log.Println("id:", m.ID);
		event := &GroupEvent{}
		event.Id = m.ID
		err = hscan.ScanMap(event, m.Values)
		if err != nil {
			log.Println("scan err:", err)
		}
		log.Println("event:", event.Id, event.Name, event.GroupId, event.MemberId, event.IsSuper)
	}
}



func TestStreamBlockRead(t *testing.T) {
	var ctx = context.Background()

	lastId := getLastId("test_stream")

	args := &redis.XReadArgs{
		Streams:[]string{"test_stream", lastId},
		Count:1,
		Block:time.Duration(1*time.Second),
	}	
	r, err := redis_client.XRead(ctx, args).Result()

	if err != nil {
		log.Println("redis err:", err)
		return
	}

	if len(r) != 1 {
		return
	}

	s := r[0]
	for _, m := range(s.Messages) {
		log.Println("id:", m.ID);
		event := &GroupEvent{}
		event.Id = m.ID
		err = hscan.ScanMap(event, m.Values)
		if err != nil {
			log.Println("scan err:", err)
		}
		log.Println("event:", event.Id, event.Name, event.GroupId, event.MemberId, event.IsSuper)
	}
}


func TestAuth(t *testing.T) {
	var ctx = context.Background()

	token := "AXfqBIEx3YelmRZnoXkNIFlsyNtWsG"
	key := fmt.Sprintf("access_token_%s", token)
	r := redis_client.HMGet(ctx, key, "user_id", "app_id")
	if r.Err() != nil {
		return
	}
	val := r.Val()
	if len(val) == 2 && val[0] == redis.Nil && val[1] == redis.Nil {
		return
	}
	u := struct {
		Id int64 `redis:"user_id"`
		AppId int64 `redis:"app_id"`
	}{}

	err := r.Scan(&u)
	if err != nil {
		return
	}

	log.Println("uid1:", u.Id, " appid:", u.AppId)
		
}
