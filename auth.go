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

func GetClient(client_id int64) (int64, string, error) {
	db, err := sql.Open("mysql", config.appdb_datasource)
	if err != nil {
		return 0, "", err
	}
	defer db.Close()
	stmtIns, err := db.Prepare("SELECT app_id, secret FROM client where id=?")
	if err != nil {
		return 0, "", err
	}

	defer stmtIns.Close()
	var appid int64
	var secret string

	rows, err := stmtIns.Query(client_id)
	for rows.Next() {
		rows.Scan(&appid, &secret)
		break
	}
	return appid, secret, nil
}

type Application struct {
	appid int64
	android_client_id int64
	android_secret string
	ios_client_id int64
	ios_secret string
}

func GetROMClient(client_id int64) (int64, string, error) {
	rom := make(map[int64]*Application)
	var app *Application

	app = &Application{}
	app.appid = 0
	app.android_client_id = 0
	app.android_secret = "eopklfnsiulxaeuo"
	app.ios_client_id = 0
	app.ios_secret = "eopklfnsiulxaeuo"
	rom[0] = app

	app = &Application{}
	app.appid = 7
	app.android_client_id = 8
	app.android_secret = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
	app.ios_client_id = 9
	app.ios_secret = "0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1"
	
	rom[8] = app
	rom[9] = app

	app = &Application{}
	app.appid = 17
	app.android_client_id = 18
	app.android_secret = "lc83RwzODvxaGELdHiOiOmI4vqWC6GkA"
	app.ios_client_id = 19
	app.ios_secret = "5tFTKsZTYTmrZBCW71JZWOczZQBrxRHK"

	rom[18] = app
	rom[19] = app


	app = &Application{}
	app.appid = 27
	app.android_client_id = 28
	app.android_secret = "r5lvLhXb6TeC5e2c1HYTxLB5qzHfhXOJ"
	app.ios_client_id = 29
	app.ios_secret = "t9fAVadX0GVQF9QaCaNqYPxXKXvCJPvc"

	rom[28] = app
	rom[29] = app

	if app, ok := rom[client_id]; ok {
		if client_id == app.android_client_id {
			return app.appid, app.android_secret, nil
		} else if client_id == app.ios_client_id {
			return app.appid, app.ios_secret, nil
		}
	} 
	return 0, "", errors.New("not exist")
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

	client_id, err := strconv.ParseInt(p[0], 10, 64)
	if err != nil {
		return 0, errors.New("invalid client id")
	}
	
	if len(p[1]) == 0 {
		return 0, errors.New("invalid auth header")
	}

	appid, secret, err := GetROMClient(client_id)
	if err != nil {
		appid, secret, err = GetClient(client_id)
		if err != nil {
			return 0, errors.New("invalid client id")
		}
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
	if token != "" {
		obj := make(map[string]interface{})
		obj["token"] = token
		WriteHttpObj(obj, w)
		return
	}
	
	token = GenUserToken()
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

	if len(device_token) == 0 && len(ng_device_token) == 0 {
		WriteHttpError(400, "invalid param", w)
		return
	}
	
	err = SaveUserDeviceToken(appid, uid, device_token, ng_device_token)
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

	if len(device_token) == 0 && len(ng_device_token) == 0 {
		WriteHttpError(400, "invalid param", w)
		return
	}

	err = ResetUserDeviceToken(appid, uid, device_token, ng_device_token)
	if err != nil {
		WriteHttpError(400, "server error", w)
		return
	}
	w.WriteHeader(200)	
}
