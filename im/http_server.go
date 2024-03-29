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
	"net/http"

	"github.com/gomodule/redigo/redis"

	log "github.com/sirupsen/logrus"
)

type loggingHandler struct {
	handler http.Handler
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Infof("http request:%s %s %s", r.RemoteAddr, r.Method, r.URL)
	h.handler.ServeHTTP(w, r)
}

func StartHttpServer(addr string, app_route *AppRoute, app *App, redis_pool *redis.Pool, server_summary *ServerSummary, rpc_storage *RPCStorage) {
	http.HandleFunc("/stack", Stack)
	handle_http2("/summary", Summary, app_route, server_summary)
	handle_http2("/post_group_notification", PostGroupNotification, app, rpc_storage)
	handle_http3("/post_peer_message", PostPeerMessage, app, server_summary, rpc_storage)
	handle_http3("/post_group_message", PostGroupMessage, app, server_summary, rpc_storage)
	handle_http2("/post_system_message", SendSystemMessage, app, rpc_storage)
	handle_http("/post_notification", SendNotification, app)
	handle_http("/post_room_message", SendRoomMessage, app)
	handle_http2("/post_customer_message", SendCustomerMessage, app, rpc_storage)
	handle_http("/post_realtime_message", SendRealtimeMessage, app)
	handle_http2("/get_offline_count", GetOfflineCount, redis_pool, rpc_storage)
	handle_http("/load_latest_message", LoadLatestMessage, rpc_storage)
	handle_http("/load_history_message", LoadHistoryMessage, rpc_storage)

	handler := loggingHandler{http.DefaultServeMux}

	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}
