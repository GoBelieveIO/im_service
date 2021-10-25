package main

import "time"
import "fmt"
import "context"
import "github.com/go-redis/redis/v8"

const APP_ID = 7
const CHARSET = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomStringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}


func NewRedisClient(server, password string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: server,
		Password:password,
		DB:db,
		IdleTimeout: 480 * time.Second,
	});
}

func login(uid int64) (string, error) {
	var ctx = context.Background()
	
	token := RandomStringWithCharset(24, CHARSET)
	
	key := fmt.Sprintf("access_token_%s", token)

	_, err := redis_client.Do(ctx, "HMSET", key, "access_token", token, "user_id", uid, "app_id", APP_ID).Result()
	if err != nil {
		return "", err
	}
	_, err = redis_client.Do(ctx, "PEXPIRE", key, 1000*3600*4).Result()
	if err != nil {
		return "", err
	}

	return token, nil
}
