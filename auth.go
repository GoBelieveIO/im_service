package main

import "net/http"
import "encoding/json"
import "io/ioutil"
import "errors"
import "strings"
import "strconv"
import "encoding/base64"
import "github.com/bitly/go-simplejson"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"

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

func GetAppKeyAndSecret(app_id int64) (string, string, error) {
	db, err := sql.Open("mysql", config.appdb_datasource)
	if err != nil {
		return "", "", err
	}
	defer db.Close()
	stmtIns, err := db.Prepare("SELECT `key`, secret FROM app where id=?")
	if err != nil {
		return "", "", err
	}

	defer stmtIns.Close()
	var key string
	var secret string

	rows, err := stmtIns.Query(app_id)
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
	if p[1] != secret {
		return 0, errors.New("invalid client id")
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
