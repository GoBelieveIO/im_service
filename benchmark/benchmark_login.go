//go:build exclude

package main

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

const APP_ID = 7
const CHARSET = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2) * time.Second
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

func login(uid int64) (string, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	token := RandomStringWithCharset(24, CHARSET)

	key := fmt.Sprintf("access_token_%s", token)
	_, err := conn.Do("HMSET", key, "access_token", token, "user_id", uid, "app_id", APP_ID)
	if err != nil {
		return "", err
	}
	_, err = conn.Do("PEXPIRE", key, 1000*3600*4)
	if err != nil {
		return "", err
	}

	return token, nil
}
