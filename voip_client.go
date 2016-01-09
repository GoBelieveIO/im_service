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

import log "github.com/golang/glog"


type VOIPClient struct {
	*Connection
}

func (client *VOIPClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MSG_VOIP_CONTROL:
		client.HandleVOIPControl(msg.body.(*VOIPControl))
	}
}

func (client *VOIPClient) HandleVOIPControl(msg *VOIPControl) {
	log.Info("send voip control:", msg.receiver)
	m := &Message{cmd: MSG_VOIP_CONTROL, body: msg}
	client.SendMessage(msg.receiver, m)
}

