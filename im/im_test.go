package main

import "testing"
import "log"
import "time"
import "flag"
import "os"
import "github.com/gomodule/redigo/redis"
import "github.com/importcjj/sensitive"
import "github.com/bitly/go-simplejson"

var redis_pool *redis.Pool
var filter *sensitive.Filter
var config *Config
var relationship_pool *RelationshipPool

func init() {
	filter = sensitive.New()
	relationship_pool = NewRelationshipPool()	
}




func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2)*time.Second
			c, err := redis.DialTimeout("tcp", server, timeout, 0, 0)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if db > 0 && db < 16 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}


func TestMain(m *testing.M) {
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Println("usage:main.test config_file")
		return
	}
	
	config = read_cfg(flag.Args()[0])

	redis_pool = NewRedisPool(config.redis_address, config.redis_password, 
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


func Test_Filter(t *testing.T) {
	log.Println("hhhhh test")

	msg := &IMMessage{}
	
	msg.content = "{\"text\": \"\\u6211\\u4e3a\\u5171*\\u4ea7\\u515a\\u7eed\\u4e00\\u79d2\"}"
	FilterDirtyWord(msg)
	log.Println("msg:", string(msg.content))

	
	s := "我为共*产党续一秒"
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

