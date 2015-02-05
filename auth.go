package main

import "net/http"
import "encoding/json"
import "io/ioutil"
import "errors"
import "strings"
import "strconv"
import "encoding/base64"
import "github.com/bitly/go-simplejson"

func WriteError(status int, err string, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["error"] = err
	b, _ := json.Marshal(obj)
	w.WriteHeader(status)
	w.Write(b)
}

func WriteObj(obj map[string]interface{}, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(obj)
	w.Write(b)
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

	if (p[1] == "eopklfnsiulxaeuo" && client_id == 0) {
		return client_id, nil
	}
	//todo get appid
	return client_id, nil
}

func AuthGrant(w http.ResponseWriter, r *http.Request) {
	appid, err := BasicAuthorization(r)
	if err != nil {
		WriteError(401, "require auth", w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteError(400, "invalid param", w)
		return
	}
	
	obj, err := simplejson.NewJson(body)
	if err != nil {
		WriteError(400, "invalid param", w)
		return
	}

	uid, err := obj.Get("uid").Int64()
	if err != nil {
		WriteError(400, "invalid param", w)
		return
	}

	//uname可以为空
	uname, _ := obj.Get("user_name").String()
	
	token := GetUserAccessToken(appid, uid)
	if token != "" {
		obj := make(map[string]interface{})
		obj["token"] = token
		WriteObj(obj, w)
		return
	}
	
	token = GenUserToken()
	err = SaveUserAccessToken(appid, uid, uname, token)
	if err != nil {
		WriteError(400, "server error", w)
		return
	}
	resp := make(map[string]interface{})
	resp["token"] = token
	WriteObj(resp, w)
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
		WriteError(401, "require auth", w)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		WriteError(400, "invalid param", w)
		return
	}
	
	obj, err := simplejson.NewJson(body)
	if err != nil {
		WriteError(400, "invalid param", w)
		return
	}

	device_token, _ := obj.Get("apns_device_token").String()
	ng_device_token, _ := obj.Get("ng_device_token").String()

	if len(device_token) == 0 && len(ng_device_token) == 0 {
		WriteError(400, "invalid param", w)
		return
	}
	
	err = SaveUserDeviceToken(appid, uid, device_token, ng_device_token)
	if err != nil {
		WriteError(400, "server error", w)
		return
	}
	w.WriteHeader(200)
}
