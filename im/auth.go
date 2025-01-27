/**
 * Copyright (c) 2014-2015, GoBelieve
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/GoBelieveIO/im_service/server"
	"github.com/dgrijalva/jwt-go"
	"github.com/gomodule/redigo/redis"
)

type RedisAuth struct {
	redis_pool *redis.Pool
}

func (a *RedisAuth) LoadUserAccessToken(token string) (int64, int64, error) {
	conn := a.redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("access_token_%s", token)
	var uid int64
	var appid int64

	err := conn.Send("EXISTS", key)
	if err != nil {
		return 0, 0, err
	}
	err = conn.Send("HMGET", key, "user_id", "app_id")
	if err != nil {
		return 0, 0, err
	}
	err = conn.Flush()
	if err != nil {
		return 0, 0, err
	}

	exists, err := redis.Bool(conn.Receive())
	if err != nil {
		return 0, 0, err
	}
	reply, err := redis.Values(conn.Receive())
	if err != nil {
		return 0, 0, err
	}

	if !exists {
		return 0, 0, errors.New("token non exists")
	}
	_, err = redis.Scan(reply, &uid, &appid)
	if err != nil {
		return 0, 0, err
	}

	return appid, uid, nil
}

type JWTAuth struct {
	jwt_signing_key string
}

func (a *JWTAuth) LoadUserAccessToken(tokenString string) (int64, int64, error) {
	var appid, uid int64
	p := &jwt.Parser{UseJSONNumber: true}
	token, err := p.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.jwt_signing_key, nil
	})
	if err != nil {
		return 0, 0, err
	}

	if !token.Valid {
		return 0, 0, errors.New("invalid token")
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if n, ok := claims["appid"].(json.Number); ok {
			appid, err = n.Int64()
			if err != nil {
				return 0, 0, err
			}
		}
		if n, ok := claims["uid"].(json.Number); ok {
			uid, err = n.Int64()
			if err != nil {
				return 0, 0, err
			}
		}
		return appid, uid, nil
	} else {
		return 0, 0, errors.New("invalid token")
	}
}

func NewRedisAuth() server.Auth {
	return &RedisAuth{}
}

func NewJWTAuth() server.Auth {
	return &JWTAuth{}
}

func NewAuth(method string) server.Auth {
	if method == "redis" {
		return NewRedisAuth()
	} else if method == "jwt" {
		return NewJWTAuth()
	} else {
		return nil
	}
}
