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
	. "github.com/GoBelieveIO/im_service/protocol"
	log "github.com/sirupsen/logrus"
)

type GroupLoaderRequest struct {
	gid int64
	c   chan *Group
}

type GroupMessageDispatch struct {
	gid   int64
	appid int64
	msg   *Message
}

type GroupLoader struct {
	//保证单个群组结构只会在一个线程中被加载
	lt chan *GroupLoaderRequest   //加载group结构到内存
	dt chan *GroupMessageDispatch //dispatch 群组消息

	group_manager *GroupManager
	app_route     *AppRoute
}

func NewGroupLoader(group_manager *GroupManager, app_route *AppRoute) *GroupLoader {
	storage := new(GroupLoader)
	storage.lt = make(chan *GroupLoaderRequest)
	storage.dt = make(chan *GroupMessageDispatch, 1000)
	storage.group_manager = group_manager
	storage.app_route = app_route
	return storage
}

func (storage *GroupLoader) LoadGroup(gid int64) *Group {
	group := storage.group_manager.FindGroup(gid)
	if group != nil {
		return group
	}

	l := &GroupLoaderRequest{gid, make(chan *Group)}
	storage.lt <- l

	group = <-l.c
	return group
}

func (storage *GroupLoader) DispatchMessage(msg *Message, group_id int64, appid int64) {
	//assert(msg.appid > 0)
	group := storage.group_manager.FindGroup(group_id)
	if group != nil {
		storage.app_route.SendGroupMessage(appid, group, msg)
	} else {
		select {
		case storage.dt <- &GroupMessageDispatch{gid: group_id, appid: appid, msg: msg}:
		default:
			log.Warning("can't dispatch group message nonblock")
		}
	}
}

func (storage *GroupLoader) dispatchMessage(msg *Message, group_id int64, appid int64) {
	group := storage.group_manager.LoadGroup(group_id)
	if group == nil {
		log.Warning("load group nil, can't dispatch group message")
		return
	}
	storage.app_route.SendGroupMessage(appid, group, msg)
}

func (storage *GroupLoader) loadGroup(gl *GroupLoaderRequest) {
	group := storage.group_manager.LoadGroup(gl.gid)
	gl.c <- group
}

func (storage *GroupLoader) run() {
	log.Info("group message deliver running loop2")

	for {
		select {
		case gl := <-storage.lt:
			storage.loadGroup(gl)
		case m := <-storage.dt:
			storage.dispatchMessage(m.msg, m.gid, m.appid)
		}
	}
}

func (storage *GroupLoader) Start() {
	go storage.run()
}
