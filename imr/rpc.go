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
import "net/url"
import "strconv"
import log "github.com/golang/glog"


func WriteHttpObj(data map[string]interface{}, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = data
	b, _ := json.Marshal(obj)
	w.Write(b)
}


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


//获取当前所有在线的用户
func GetOnlineClients(w http.ResponseWriter, req *http.Request) {
	clients := GetClientSet()

	type App struct {
		AppId int64 `json:"appid"`
		Users []int64 `json:"users"`
	}
	
	r := make(map[int64]IntSet)
	for c := range(clients) {
		app_users := c.app_route.GetUsers()
		for appid, users := range(app_users) {
			if _, ok := r[appid]; !ok {
				r[appid] = NewIntSet()
			}
			uids := r[appid]
			for uid := range(users) {
				uids.Add(uid)
			}
		}
	}

	apps := make([]*App, 0, len(r))
	for appid, users := range(r) {
		app := &App{}
		app.AppId = appid
		app.Users = make([]int64, 0, len(users))
		for uid := range(users) {
			app.Users = append(app.Users, uid)
		}
		apps = append(apps, app)
	}

	res, err := json.Marshal(apps)
	if err != nil {
		log.Info("json marshal:", err)
		WriteHttpError(400, "json marshal err", w)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	_, err = w.Write(res)
	if err != nil {
		log.Info("write err:", err)
	}
}

//获取单个用户在线状态
func GetOnlineStatus(w http.ResponseWriter, req *http.Request) {
	log.Info("get user online status")
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	online := IsUserOnline(appid, uid)
	resp := make(map[string]interface{})
	resp["online"] = online

	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(resp)
	w.Write(b)
	
	WriteHttpObj(resp, w)
}
