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

	"github.com/GoBelieveIO/im_service/handler"
	"github.com/GoBelieveIO/im_service/server"
	log "github.com/sirupsen/logrus"
)

func StartHttpServer(addr string, app_route *server.AppRoute, app *server.App, redis_pool *redis.Pool, server_summary *server.ServerSummary, rpc_storage *server.RPCStorage) {
	http.HandleFunc("/stack", server.Stack)
	handler.Handle2("/summary", server.Summary, app_route, server_summary)
	handler.Handle2("/post_group_notification", server.PostGroupNotification, app, rpc_storage)
	handler.Handle3("/post_peer_message", server.PostPeerMessage, app, server_summary, rpc_storage)
	handler.Handle3("/post_group_message", server.PostGroupMessage, app, server_summary, rpc_storage)
	handler.Handle2("/post_system_message", server.SendSystemMessage, app, rpc_storage)
	handler.Handle("/post_notification", server.SendNotification, app)
	handler.Handle("/post_room_message", server.SendRoomMessage, app)
	handler.Handle2("/post_customer_message", server.SendCustomerMessage, app, rpc_storage)
	handler.Handle("/post_realtime_message", server.SendRealtimeMessage, app)
	handler.Handle2("/get_offline_count", server.GetOfflineCount, redis_pool, rpc_storage)
	handler.Handle("/load_latest_message", server.LoadLatestMessage, rpc_storage)
	handler.Handle("/load_history_message", server.LoadHistoryMessage, rpc_storage)

	handler := handler.LoggingHandler{Handler: http.DefaultServeMux}

	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}
