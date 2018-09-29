package main

import (
	"io"
	"io/ioutil"
	"net/http"
	"encoding/json"
	"log"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/favframework/debug"
	"github.com/bitly/go-simplejson"
)

// hello world, the web server
func AuthGrant(w http.ResponseWriter, req *http.Request) {

	log.Print("post group notification")
	body, err := ioutil.ReadAll(req.Body)
	obj, err := simplejson.NewJson(body)
	if err != nil {
		log.Print("error:", err)
		//WriteHttpError(400, "invalid json format", w)
		return
	}


	uid, err := obj.Get("uid").Int64()
	app_id, err := obj.Get("app_id").Int64()
	user_name, err := obj.Get("user_name").String()
	platform_id, err := obj.Get("platform_id").String()
	device_id, err := obj.Get("device_id").String()
	token, err := obj.Get("token").String()

	if err != nil {
		log.Print("error:", err)
		//WriteHttpError(400, "invalid json format", w)
		io.WriteString(w, "invalid token")
		return
	}


	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer c.Close()


	_, err = c.Do("hset", "access_token_"+token, "user_id", uid)
	_, err = c.Do("hset", "access_token_"+token, "app_id", app_id)
	_, err = c.Do("hset", "access_token_"+token, "platform_id", platform_id)
	_, err = c.Do("hset", "access_token_"+token, "device_id", device_id)

	if err != nil {
		fmt.Println("haset failed", err.Error())
	}


	userId, err := redis.String(c.Do("HGET", "access_token_"+token,"user_id"))
	if err != nil {
		fmt.Println("redis get failed:", err)
	} else {
		fmt.Printf("get user_id: %v \n", userId)
		fmt.Printf("set token: %v \n", token)
		godump.Dump(user_name)
		//io.WriteString(w, "set token:"+token)
		w.Header().Set("Content-Type", "application/json")
		obj := make(map[string]interface{})
		obj["token"] = token
		b, _ := json.Marshal(obj)
		w.Write(b)
	}

}

func main() {
	log.Print("Starting AuthGrant Service: ")
	http.HandleFunc("/auth/grant", AuthGrant)
	http.HandleFunc("/set/token", AuthGrant)
	http.HandleFunc("/auth/token", AuthGrant)

	err := http.ListenAndServe(":8001", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}