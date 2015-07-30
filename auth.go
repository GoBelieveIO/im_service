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

import "net/http"
import "encoding/json"
import "io/ioutil"
import "errors"
import "strings"
import "strconv"
import "encoding/base64"
import "encoding/hex"
import "crypto/md5"
import "github.com/bitly/go-simplejson"
import log "github.com/golang/glog"
import "fmt"
import "github.com/garyburd/redigo/redis"

func WriteHttpError(status int, err string, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	meta := make(map[string]interface{})
	meta["code"] = status
	meta["message"] = err
	obj["meta"] = meta
	b, _ := json.Marshal(obj)
	w.WriteHeader(status)
	w.Write(b)
}

func WriteHttpObj(data map[string]interface{}, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = data
	b, _ := json.Marshal(obj)
	w.Write(b)
}

func GetAppKeyAndSecretFromCache(app_id int64) (string, string, error) {
	conn := redis_pool.Get()
	defer conn.Close()

	key := fmt.Sprintf("apps_%d", app_id)
	reply, err := redis.Values(conn.Do("HMGET", key, "key", "secret"))
	if err != nil {
		log.Infof("hget %s err:%s\n", key, err)
		return "", "", err
	}

	var app_key string
	var app_secret string
	_, err = redis.Scan(reply, &app_key, &app_secret)
	if err != nil {
		log.Warning("scan error:", err)
		return "", "", err
	}

	log.Info("get from cache:", app_key, " ", app_secret)
	return app_key, app_secret, err
}

func SetAppKeyAndSecretToCache(app_id int64, app_key string , app_secret string) error {
	conn := redis_pool.Get()
	defer conn.Close()
	
	key := fmt.Sprintf("apps_%d", app_id)
	
	_, err := conn.Do("HMSET", key, "key", app_key, "secret", app_secret)
	if err != nil {
		log.Info("hmset err:", err)
		return err
	}
	return nil
}

func GetAppKeyAndSecret(app_id int64) (string, string, error) {
	var key string
	var secret string

	stmtIns, err := db.Prepare("SELECT `key`, secret FROM app where id=?")
	if err != nil {
		return "", "", err
	}

	defer stmtIns.Close()

	rows, err := stmtIns.Query(app_id)
	if err != nil {
		return "", "", err
	}

	defer rows.Close()
	for rows.Next() {
		rows.Scan(&key, &secret)
		break
	}

	return key, secret, nil
}

func BasicAuthorization(r *http.Request) (int64, error) {
	auth := r.Header.Get("Authorization");
	if len(auth) <= 6 {
		return 0, errors.New("no authorization header")
	}
	if auth[:6] != "Basic " {
		return 0, errors.New("no authorization header")
	}

	basic, err := base64.StdEncoding.DecodeString(string(auth[6:]))
	if err != nil {
		return 0, err
	}
	p := strings.Split(string(basic), ":")
	if len(p) != 2 {
		return 0, errors.New("invalid auth header")
	}

	appid, err := strconv.ParseInt(p[0], 10, 64)
	if err != nil {
		return 0, errors.New("invalid client id")
	}

	if len(p[1]) == 0 {
		return 0, errors.New("invalid auth header")
	}

	_, secret, err := GetAppKeyAndSecret(appid)
	if err != nil {
		log.Info("mysql err:", err)
		return 0, errors.New("invalid client id")
	}
	md5_secret := md5.Sum([]byte(secret))
	app_secret := hex.EncodeToString(md5_secret[:])

	if p[1] != app_secret {
		return 0, errors.New("invalid app id")
	}
	return appid, nil
}

func AuthGrant(w http.ResponseWriter, r *http.Request) {
	appid, err := BasicAuthorization(r)
	if err != nil {
		WriteHttpError(401, err.Error(), w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}
	
	obj, err := simplejson.NewJson(body)
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}

	uid, err := obj.Get("uid").Int64()
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}

	//uname可以为空
	uname, _ := obj.Get("user_name").String()
	
	token := GetUserAccessToken(appid, uid)
	if token == "" {
		token = GenUserToken()
		CountUser(appid, uid)
	}

	err = SaveUserAccessToken(appid, uid, uname, token)
	if err != nil {
		WriteHttpError(400, "server error", w)
		return
	}
	resp := make(map[string]interface{})
	resp["token"] = token
	WriteHttpObj(resp, w)
}


func BearerAuthentication(r *http.Request) (int64, int64, error) {
	token := r.Header.Get("Authorization");
	if len(token) <= 7 {
		return 0, 0, errors.New("no authorization header")
	}
	if token[:7] != "Bearer " {
		return 0, 0, errors.New("no authorization header")
	}
	appid, uid, _, err := LoadUserAccessToken(token[7:])
	return appid, uid, err
}

func BindToken(w http.ResponseWriter, r *http.Request) {
	appid, uid, err := BearerAuthentication(r)
	if err != nil {
		WriteHttpError(401, "require auth", w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}
	
	obj, err := simplejson.NewJson(body)
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}

	device_token, _ := obj.Get("apns_device_token").String()
	ng_device_token, _ := obj.Get("ng_device_token").String()
	xg_device_token, _ := obj.Get("xg_device_token").String()

	if len(device_token) == 0 && 
		len(ng_device_token) == 0 && 
		len(xg_device_token) == 0 {
		WriteHttpError(400, "invalid param", w)
		return
	}
	
	err = SaveUserDeviceToken(appid, uid, device_token, ng_device_token, xg_device_token)
	if err != nil {
		WriteHttpError(400, "server error", w)
		return
	}
	w.WriteHeader(200)
}

func UnbindToken(w http.ResponseWriter, r *http.Request) {
	appid, uid, err := BearerAuthentication(r)
	if err != nil {
		WriteHttpError(401, "require auth", w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}
	
	obj, err := simplejson.NewJson(body)
	if err != nil {
		WriteHttpError(400, "invalid param", w)
		return
	}

	device_token, _ := obj.Get("apns_device_token").String()
	ng_device_token, _ := obj.Get("ng_device_token").String()
	xg_device_token, _ := obj.Get("xg_device_token").String()

	if len(device_token) == 0 && 
		len(ng_device_token) == 0 && 
		len(xg_device_token) == 0 {
		WriteHttpError(400, "invalid param", w)
		return
	}

	err = ResetUserDeviceToken(appid, uid, device_token, ng_device_token, xg_device_token)
	if err != nil {
		WriteHttpError(400, "server error", w)
		return
	}
	w.WriteHeader(200)	
}
