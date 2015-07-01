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

import "io"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"
import "github.com/bitly/go-simplejson"
import "fmt"
import "errors"

const MSG_HEARTBEAT = 1
const MSG_AUTH = 2
const MSG_AUTH_STATUS = 3
const MSG_IM = 4
const MSG_ACK = 5
const MSG_RST = 6
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8
const MSG_PEER_ACK = 9
const MSG_INPUTING = 10
const MSG_SUBSCRIBE_ONLINE_STATE = 11
const MSG_ONLINE_STATE = 12
const MSG_PING = 13
const MSG_PONG = 14
const MSG_AUTH_TOKEN = 15
const MSG_LOGIN_POINT = 16
const MSG_RT = 17
const MSG_ENTER_ROOM = 18
const MSG_LEAVE_ROOM = 19
const MSG_ROOM_IM = 20


const PLATFORM_IOS = 1
const PLATFORM_ANDROID = 2
const PLATFORM_WEB = 3

const DEFAULT_VERSION = 1

var message_descriptions map[int]string = make(map[int]string)

type MessageCreator func()IMessage
var message_creators map[int]MessageCreator = make(map[int]MessageCreator)

type VersionMessageCreator func()IVersionMessage
var vmessage_creators map[int]VersionMessageCreator = make(map[int]VersionMessageCreator)


func init() {
	message_creators[MSG_AUTH] = func()IMessage {return new(Authentication)}
	message_creators[MSG_AUTH_STATUS] = func()IMessage {return new(AuthenticationStatus)}

	message_creators[MSG_ACK] = func()IMessage{return new(MessageACK)}
	message_creators[MSG_GROUP_NOTIFICATION] = func()IMessage{return new(GroupNotification)}

	message_creators[MSG_PEER_ACK] = func()IMessage{return new(MessagePeerACK)}
	message_creators[MSG_INPUTING] = func()IMessage{return new(MessageInputing)}
	message_creators[MSG_SUBSCRIBE_ONLINE_STATE] = func()IMessage{return new(MessageSubsribeState)}
	message_creators[MSG_ONLINE_STATE] = func()IMessage{return new(MessageOnlineState)}
	message_creators[MSG_AUTH_TOKEN] = func()IMessage{return new(AuthenticationToken)}
	message_creators[MSG_LOGIN_POINT] = func()IMessage{return new(LoginPoint)}
	message_creators[MSG_RT] = func()IMessage{return new(RTMessage)}
	message_creators[MSG_ENTER_ROOM] = func()IMessage{return new(Room)}
	message_creators[MSG_LEAVE_ROOM] = func()IMessage{return new(Room)}
	message_creators[MSG_ROOM_IM] = func()IMessage{return &RoomMessage{new(RTMessage)}}

	vmessage_creators[MSG_GROUP_IM] = func()IVersionMessage{return new(IMMessage)}
	vmessage_creators[MSG_IM] = func()IVersionMessage{return new(IMMessage)}

	message_descriptions[MSG_AUTH] = "MSG_AUTH"
	message_descriptions[MSG_AUTH_STATUS] = "MSG_AUTH_STATUS"
	message_descriptions[MSG_IM] = "MSG_IM"
	message_descriptions[MSG_ACK] = "MSG_ACK"
	message_descriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	message_descriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	message_descriptions[MSG_PEER_ACK] = "MSG_PEER_ACK"
	message_descriptions[MSG_INPUTING] = "MSG_INPUTING"
	message_descriptions[MSG_SUBSCRIBE_ONLINE_STATE] = "MSG_SUBSCRIBE_ONLINE_STATE"
	message_descriptions[MSG_ONLINE_STATE] = "MSG_ONLINE_STATE"
	message_descriptions[MSG_PING] = "MSG_PING"
	message_descriptions[MSG_PONG] = "MSG_PONG"
	message_descriptions[MSG_AUTH_TOKEN] = "MSG_AUTH_TOKEN"
	message_descriptions[MSG_LOGIN_POINT] = "MSG_LOGIN_POINT"
	message_descriptions[MSG_RT] = "MSG_RT"
	message_descriptions[MSG_ENTER_ROOM] = "MSG_ENTER_ROOM"
	message_descriptions[MSG_LEAVE_ROOM] = "MSG_LEAVE_ROOM"
	message_descriptions[MSG_ROOM_IM] = "MSG_ROOM_IM"
}

type Command int
func (cmd Command) String() string {
	c := int(cmd)
	if desc, ok := message_descriptions[c]; ok {
		return desc
	} else {
		return fmt.Sprintf("%d", c)
	}
}

type IMessage interface {
	ToData() []byte
	FromData(buff []byte) bool
}

type IVersionMessage interface {
	ToData(version int) []byte
	FromData(version int, buff []byte) bool
}

type Message struct {
	cmd  int
	seq  int
	version int
	
	body interface{}
}

func (message *Message) ToData() []byte {
	if message.body != nil {
		if m, ok := message.body.(IMessage); ok {
			return m.ToData()
		}
		if m, ok := message.body.(IVersionMessage); ok {
			return m.ToData(message.version)
		}
		return nil
	} else {
		return nil
	}
}

func (message *Message) FromData(buff []byte) bool {
	cmd := message.cmd
	if creator, ok := message_creators[cmd]; ok {
		c := creator()
		r := c.FromData(buff)
		message.body = c
		return r
	}
	if creator, ok := vmessage_creators[cmd]; ok {
		c := creator()
		r := c.FromData(message.version, buff)
		message.body = c
		return r
	}

	return len(buff) == 0
}

func (message *Message) ToMap() map[string]interface{} {
	data := make(map[string]interface{})
	data["cmd"] = message.cmd
	data["seq"] = message.seq
	cmd := message.cmd
	if cmd == MSG_AUTH {
		body := message.body.(*Authentication)
		data["body"] = map[string]interface{}{
			"uid":         body.uid,
		}
	} else if cmd == MSG_AUTH_STATUS {
		body := message.body.(*AuthenticationStatus)
		data["body"] = map[string]interface{}{
			"status": body.status,
		}
	} else if cmd == MSG_IM || cmd == MSG_GROUP_IM {
		body := message.body.(*IMMessage)
		data["body"] = map[string]interface{}{
			"sender":    body.sender,
			"receiver":  body.receiver,
			"timestamp": body.timestamp,
			"msgid":     body.msgid,
			"content":   body.content,
		}
	} else if cmd == MSG_ACK {
		data["body"] = message.body.(*MessageACK).seq
	} else if cmd == MSG_PEER_ACK {
		body := message.body.(*MessagePeerACK)
		data["body"] = map[string]interface{}{
			"sender":   body.sender,
			"receiver": body.receiver,
			"msgid":    body.msgid,
		}
	} else if cmd == MSG_HEARTBEAT || cmd == MSG_PING || cmd == MSG_PONG {
		data["body"] = nil
	} else if cmd == MSG_INPUTING {
		body := message.body.(*MessageInputing)
		data["body"] = map[string]interface{}{
			"sender":   body.sender,
			"receiver": body.receiver,
		}
	} else if cmd == MSG_GROUP_NOTIFICATION {
		data["body"] = message.body.(*GroupNotification).notification
	} else if cmd == MSG_ONLINE_STATE {
		body := message.body.(*MessageOnlineState)
		data["body"] = map[string]interface{}{
			"sender": body.sender,
			"online": body.online,
		}
	} else {
		data["body"] = nil
	}
	return data
}

func (message *Message) FromJson(msg *simplejson.Json) bool {
	switch message.cmd {
	case MSG_AUTH:
		uid, err := msg.Get("body").Get("uid").Int64()
		if err != nil {
			log.Info("get uid fail")
			return false
		}

		data := &Authentication{}
		data.uid = uid
		message.body = data
		return true
	case MSG_AUTH_TOKEN:
		token, err := msg.Get("body").Get("access_token").String()
		if err != nil {
			log.Info("get access token fail")
			return false
		}
		platform_id, err := msg.Get("body").Get("platform_id").Int()
		if err != nil {
			log.Info("get platform id fail")
			return false
		}
		device_id, err := msg.Get("body").Get("device_id").String()
		if err != nil {
			log.Info("get device id fail")
			return false
		}
		data := &AuthenticationToken{}
		data.token = token
		data.platform_id = int8(platform_id)
		data.device_id = device_id
		message.body = data
		return true
	case MSG_AUTH_STATUS:
		status, err := msg.Get("body").Get("status").Int()
		if err != nil {
			log.Info("get status fail")
			return false
		}

		data := &AuthenticationStatus{}
		data.status = int32(status)
		message.body = data
		return true

	case MSG_IM, MSG_GROUP_IM:
		sender, err := msg.Get("body").Get("sender").Int64()
		if err != nil {
			log.Info("get sender fail")
			return false
		}

		receiver, err := msg.Get("body").Get("receiver").Int64()
		if err != nil {
			log.Info("get receiver fail")
			return false
		}

		timestamp := msg.Get("body").Get("timestamp").MustInt(0)

		msgid, err := msg.Get("body").Get("msgid").Int()
		if err != nil {
			log.Info("get msgid fail")
			return false
		}

		content, err := msg.Get("body").Get("content").String()
		if err != nil {
			log.Info("get content fail")
			return false
		}
		data := &IMMessage{}
		data.sender = sender
		data.receiver = receiver
		data.timestamp = int32(timestamp)
		data.msgid = int32(msgid)
		data.content = content
		message.body = data
		return true
	case MSG_ACK:
		body, err := msg.Get("body").Int()
		if err != nil {
			log.Info("read body fail")
			return false
		}

		message.body = &MessageACK{int32(body)}
		return true
	case MSG_HEARTBEAT, MSG_PING, MSG_PONG:
		return true
	case MSG_INPUTING:
		sender, err := msg.Get("body").Get("sender").Int64()
		if err != nil {
			log.Info("get sender fail")
			return false
		}

		receiver, err := msg.Get("body").Get("receiver").Int64()
		if err != nil {
			log.Info("get receiver fail")
			return false
		}

		data := &MessageInputing{}
		data.sender = sender
		data.receiver = receiver
		message.body = data
		return true

	case MSG_GROUP_NOTIFICATION:
		body, err := msg.Get("body").String()
		if err != nil {
			log.Info("read body fail")
			return false
		}
		
		message.body = &GroupNotification{body}
		return true
	case MSG_PEER_ACK:
		sender, err := msg.Get("body").Get("sender").Int64()
		if err != nil {
			log.Info("get sender fail")
			return false
		}

		receiver, err := msg.Get("body").Get("receiver").Int64()
		if err != nil {
			log.Info("get receiver fail")
			return false
		}

		msgid, err := msg.Get("body").Get("msgid").Int()
		if err != nil {
			log.Info("get msgid fail")
			return false
		}

		data := &MessagePeerACK{}
		data.sender = sender
		data.receiver = receiver
		data.msgid = int32(msgid)
		message.body = data
		return true

	case MSG_SUBSCRIBE_ONLINE_STATE:
		tmp, err := msg.Get("body").Get("uids").Array()
		if err != nil {
			log.Info("read body fail")
			return false
		}

		data := &MessageSubsribeState{}
		uids := make([]int64, len(tmp))
		for i := range tmp {
			log.Info(tmp[i])
			if d, ok := tmp[i].(float64); ok {
				uids[i] = int64(d)
			}
		}
		data.uids = uids
		message.body = data
		return true
	default:
		return false
	}
}

func WriteHeader(len int32, seq int32, cmd byte, version byte, buffer *bytes.Buffer) {
	binary.Write(buffer, binary.BigEndian, len)
	binary.Write(buffer, binary.BigEndian, seq)
	buffer.WriteByte(cmd)
	buffer.WriteByte(byte(version))
	buffer.WriteByte(byte(0))
	buffer.WriteByte(byte(0))
}

func ReadHeader(buff []byte) (int, int, int, int) {
	var length int32
	var seq int32
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &length)
	binary.Read(buffer, binary.BigEndian, &seq)
	cmd, _ := buffer.ReadByte()
	version, _ := buffer.ReadByte()
	return int(length), int(seq), int(cmd), int(version)
}

type RTMessage struct {
	sender    int64
	receiver  int64
	content   string
}
func (message *RTMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (rt *RTMessage) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &rt.sender)
	binary.Read(buffer, binary.BigEndian, &rt.receiver)
	rt.content = string(buff[16:])
	return true
}


type IMMessage struct {
	sender    int64
	receiver  int64
	timestamp int32
	msgid     int32
	content   string
}


func (message *IMMessage) ToDataV0() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV0(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[20:])
	return true
}


func (message *IMMessage) ToDataV1() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.timestamp)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV1(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.timestamp)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[24:])
	return true
}


func (im *IMMessage) ToData(version int) []byte {
	if version == 0 {
		return im.ToDataV0()
	} else {
		return im.ToDataV1()
	}
}

func (im *IMMessage) FromData(version int, buff []byte) bool {
	if version == 0 {
		return im.FromDataV0(buff)
	} else {
		return im.FromDataV1(buff)
	}
}


type Authentication struct {
	uid         int64
}

func (auth *Authentication) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.uid)
	buf := buffer.Bytes()
	return buf
}

func (auth *Authentication) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.uid)
	return true
}

type AuthenticationToken struct {
	token       string
	platform_id int8
	device_id   string
}


func (auth *AuthenticationToken) ToData() []byte {
	var l int8

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.platform_id)

	l = int8(len(auth.token))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.token))

	l = int8(len(auth.device_id))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.device_id))

	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationToken) FromData(buff []byte) bool {
	var l int8
	if (len(buff) <= 3) {
		return false
	}
	auth.platform_id = int8(buff[0])

	buffer := bytes.NewBuffer(buff[1:])

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return false
	}
	token := make([]byte, l)
	buffer.Read(token)

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return false
	}
	device_id := make([]byte, l)
	buffer.Read(device_id)

	auth.token = string(token)
	auth.device_id = string(device_id)
	return true
}

type AuthenticationStatus struct {
	status int32
}

func (auth *AuthenticationStatus) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.status)
	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationStatus) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.status)
	return true
}


type LoginPoint struct {
	up_timestamp      int32
	platform_id       int8
	device_id         string
}

func (point *LoginPoint) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, point.up_timestamp)
	binary.Write(buffer, binary.BigEndian, point.platform_id)
	buffer.Write([]byte(point.device_id))
	buf := buffer.Bytes()
	return buf
}

func (point *LoginPoint) FromData(buff []byte) bool {
	if len(buff) <= 5 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &point.up_timestamp)
	binary.Read(buffer, binary.BigEndian, &point.platform_id)
	point.device_id = string(buff[5:])
	return true
}


type MessageACK struct {
	seq int32
}

func (ack *MessageACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.seq)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessageACK) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.seq)
	return true
}

type MessagePeerACK struct {
	sender   int64
	receiver int64
	msgid    int32
}

func (ack *MessagePeerACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.sender)
	binary.Write(buffer, binary.BigEndian, ack.receiver)
	binary.Write(buffer, binary.BigEndian, ack.msgid)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessagePeerACK) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.sender)
	binary.Read(buffer, binary.BigEndian, &ack.receiver)
	binary.Read(buffer, binary.BigEndian, &ack.msgid)
	return true
}

type MessageInputing struct {
	sender   int64
	receiver int64
}

func (inputing *MessageInputing) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, inputing.sender)
	binary.Write(buffer, binary.BigEndian, inputing.receiver)
	buf := buffer.Bytes()
	return buf
}

func (inputing *MessageInputing) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &inputing.sender)
	binary.Read(buffer, binary.BigEndian, &inputing.receiver)
	return true
}

type GroupNotification struct {
	notification string
}

func (notification *GroupNotification) ToData() []byte {
	return []byte(notification.notification)
}
 
func (notification *GroupNotification) FromData(buff []byte) bool {
	notification.notification = string(buff)
	return true
}

type Room int64
func (room *Room) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int64(*room))
	buf := buffer.Bytes()
	return buf	
}

func (room *Room) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, (*int64)(room))
	return true
}

func (room *Room) RoomID() int64 {
	return int64(*room)
}

type RoomMessage struct {
	*RTMessage
}

type MessageOnlineState struct {
	sender int64
	online int32
}

func (state *MessageOnlineState) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, state.sender)
	binary.Write(buffer, binary.BigEndian, state.online)
	buf := buffer.Bytes()
	return buf
}

func (state *MessageOnlineState) FromData(buff []byte) bool {
	if len(buff) < 12 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &state.sender)
	binary.Read(buffer, binary.BigEndian, &state.online)
	return true
}



type MessageSubsribeState struct {
	uids []int64
}


func (sub *MessageSubsribeState) ToData() []byte {
	return nil
}

func (sub *MessageSubsribeState) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	var count int32
	binary.Read(buffer, binary.BigEndian, &count)
	sub.uids = make([]int64, count)
	for i := 0; i < int(count); i++ {
		binary.Read(buffer, binary.BigEndian, &sub.uids[i])
	}
	return true
}


type AppUserID struct {
	appid    int64
	uid      int64
}

func (id *AppUserID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppUserID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
}

type AppRoomID struct {
	appid    int64
	room_id      int64
}

func (id *AppRoomID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.room_id)
	buf := buffer.Bytes()
	return buf
}

func (id *AppRoomID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.room_id)

	return true
}

func SendMessage(conn io.Writer, msg *Message) error {
	body := msg.ToData()
	buffer := new(bytes.Buffer)
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd), byte(msg.version), buffer)
	buffer.Write(body)
	buf := buffer.Bytes()
	n, err := conn.Write(buf)
	if err != nil {
		log.Info("sock write error:", err)
		return err
	}
	if n != len(buf) {
		log.Infof("write less:%d %d", n, len(buf))
		return errors.New("write less")
	}
	return nil
}

func ReceiveMessage(conn io.Reader) *Message {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd, version := ReadHeader(buff)
	if length < 0 || length > 64*1024 {
		log.Info("invalid len:", length)
		return nil
	}
	buff = make([]byte, length)
	_, err = io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	message := new(Message)
	message.cmd = cmd
	message.seq = seq
	message.version = version
	if !message.FromData(buff) {
		log.Warning("parse error")
		return nil
	}
	return message
}
