package main

import "io"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"
import "github.com/bitly/go-simplejson"
import "fmt"

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

const MSG_ADD_CLIENT = 128
const MSG_REMOVE_CLIENT = 129

//路由服务器消息
const MSG_SUBSCRIBE = 130
const MSG_UNSUBSCRIBE = 131
const MSG_PUBLISH = 132
const MSG_PUBLISH_GROUP = 133


//存储服务器消息
const MSG_SAVE_AND_ENQUEUE = 200
const MSG_DEQUEUE = 201
const MSG_LOAD_OFFLINE = 202
const MSG_RESULT = 203

//内部文件存储使用
const MSG_OFFLINE = 254
const MSG_ACK_IN = 255


const PLATFORM_IOS = 1
const PLATFORM_ANDROID = 2
const PLATFORM_WEB = 3

type Command int
func (cmd Command) String() string {
	switch cmd {
	case MSG_AUTH_STATUS:
		return "MSG_AUTH_STATUS"
	case MSG_IM:
		return "MSG_IM"
	case MSG_ACK:
		return "MSG_ACK"
	case MSG_PING:
		return "MSG_PING"
	case MSG_PONG:
		return "MSG_PONG"
	case MSG_SUBSCRIBE:
		return "MSG_SUBSCRIBE"
	case MSG_UNSUBSCRIBE:
		return "MSG_UNSUBSCRIBE"
	case MSG_PUBLISH:
		return "MSG_PUBLISH"
	case MSG_PEER_ACK:
		return "MSG_PEER_ACK"
	case MSG_AUTH_TOKEN:
		return "MSG_AUTH_TOKEN"
	case MSG_INPUTING:
		return "MSG_INPUTTING"
	default:
		return fmt.Sprintf("%d", cmd)
	}

}

type OfflineMessage struct {
	appid    int64
	receiver int64
	msgid    int64
}

type IMMessage struct {
	sender    int64
	receiver  int64
	timestamp int32
	msgid     int32
	content   string
}

type MessageInputing struct {
	sender   int64
	receiver int64
}

type MessageSubsribeState struct {
	uids []int64
}

type MessageOnlineState struct {
	sender int64
	online int32
}

type MessageACK int32

type MessagePeerACK struct {
	sender   int64
	receiver int64
	msgid    int32
}

type Authentication struct {
	uid         int64
}

type AuthenticationToken struct {
	token       string
	platform_id int8
	device_id   string
}

type AuthenticationStatus struct {
	status int32
}

type LoginPoint struct {
	up_timestamp      int32
	platform_id       int8
	device_id         string
}

type MessageAddClient struct {
	uid       int64
	timestamp int32
}

type Message struct {
	cmd  int
	seq  int
	body interface{}
}

type EMessage struct {
	msgid int64
	msg   *Message
}

type AppUserID struct {
	appid    int64
	uid      int64
}

type AppMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	msg      *Message
}

type SAEMessage struct {
	msg       *Message
	receivers []*AppUserID
}

type DQMessage OfflineMessage 

type MessageResult struct {
	status int32
	content []byte
}

func (message *Message) ToData() []byte {
	cmd := message.cmd
	if cmd == MSG_AUTH {
		return WriteAuth(message.body.(*Authentication))
	} else if cmd == MSG_AUTH_TOKEN {
		return WriteAuthToken(message.body.(*AuthenticationToken))
	} else if cmd == MSG_AUTH_STATUS {
		return WriteAuthStatus(message.body.(*AuthenticationStatus))
	} else if cmd == MSG_LOGIN_POINT {
		return WriteLoginPoint(message.body.(*LoginPoint))
	} else if cmd == MSG_IM || cmd == MSG_GROUP_IM {
		return WriteIMMessage(message.body.(*IMMessage))
	} else if cmd == MSG_ADD_CLIENT {
		return WriteAddClient(message.body.(*MessageAddClient))
	} else if cmd == MSG_REMOVE_CLIENT {
		return WriteRemoveClient(message.body.(int64))
	} else if cmd == MSG_ACK {
		return WriteACK(message.body.(MessageACK))
	} else if cmd == MSG_PEER_ACK {
		return WritePeerACK(message.body.(*MessagePeerACK))
	} else if cmd == MSG_HEARTBEAT || cmd == MSG_PING || cmd == MSG_PONG {
		return nil
	} else if cmd == MSG_INPUTING {
		return WriteInputing(message.body.(*MessageInputing))
	} else if cmd == MSG_GROUP_NOTIFICATION {
		return WriteGroupNotification(message.body.(string))
	} else if cmd == MSG_ONLINE_STATE {
		return WriteState(message.body.(*MessageOnlineState))
	} else if cmd == MSG_OFFLINE || cmd == MSG_ACK_IN {
		return WriteOfflineMessage(message.body.(*OfflineMessage))
	} else if cmd == MSG_SAVE_AND_ENQUEUE {
		return WriteSAEMessage(message.body.(*SAEMessage))
	} else if cmd == MSG_DEQUEUE {
		return WriteOfflineMessage((*OfflineMessage)(message.body.(*DQMessage)))
	} else if cmd == MSG_LOAD_OFFLINE {
		return WriteAppUserID(message.body.(*AppUserID))
	} else if cmd == MSG_RESULT {
		return WriteResult(message.body.(*MessageResult))
	} else if cmd == MSG_SUBSCRIBE || cmd == MSG_UNSUBSCRIBE {
		return WriteAppUserID(message.body.(*AppUserID))
	} else if cmd == MSG_PUBLISH {
		return WriteAppMessage(message.body.(*AppMessage))
	} else {
		log.Warning("unknown cmd:", cmd)
		return nil
	}
}

func (message *Message) FromData(buff []byte) bool {
	cmd := message.cmd
	if cmd == MSG_AUTH {
		body, ret := ReadAuth(buff)
		message.body = body
		return ret
	} else if cmd == MSG_AUTH_TOKEN {
		body, ret := ReadAuthToken(buff)
		message.body = body
		return ret
	} else if cmd == MSG_AUTH_STATUS {
		body, ret := ReadAuthStatus(buff)
		message.body = body
		return ret
	} else if cmd == MSG_LOGIN_POINT {
		body, ret := ReadLoginPoint(buff)
		message.body = body
		return ret
	} else if cmd == MSG_IM || cmd == MSG_GROUP_IM {
		body, ret := ReadIMMessage(buff)
		message.body = body
		return ret
	} else if cmd == MSG_ADD_CLIENT {
		body, ret := ReadAddClient(buff)
		message.body = body
		return ret
	} else if cmd == MSG_REMOVE_CLIENT {
		body, ret := ReadRemoveClient(buff)
		message.body = body
		return ret
	} else if cmd == MSG_ACK {
		body, ret := ReadACK(buff)
		message.body = body
		return ret
	} else if cmd == MSG_HEARTBEAT || cmd == MSG_PING || cmd == MSG_PONG {
		return true
	} else if cmd == MSG_INPUTING {
		body, ret := ReadInputing(buff)
		message.body = body
		return ret
	} else if cmd == MSG_GROUP_NOTIFICATION {
		body, ret := ReadGroupNotification(buff)
		message.body = body
		return ret
	} else if cmd == MSG_PEER_ACK {
		body, ret := ReadPeerACK(buff)
		message.body = body
		return ret
	} else if cmd == MSG_SUBSCRIBE_ONLINE_STATE {
		body, ret := ReadSubscribeState(buff)
		message.body = body
		return ret
	} else if cmd == MSG_OFFLINE || cmd == MSG_ACK_IN {
		body, ret := ReadOfflineMessage(buff)
		message.body = body
		return ret
	} else if cmd == MSG_SAVE_AND_ENQUEUE {
		body, ret := ReadSAEMessage(buff)
		message.body = body
		return ret
	} else if cmd == MSG_DEQUEUE {
		t, ret := ReadOfflineMessage(buff)
		body := (*DQMessage)(t)
		message.body = body
		return ret
	} else if cmd == MSG_LOAD_OFFLINE {
		body, ret := ReadAppUserID(buff)
		message.body = body
		return ret
	} else if cmd == MSG_RESULT {
		body, ret := ReadResult(buff)
		message.body = body
		return ret
	} else if cmd == MSG_SUBSCRIBE || cmd == MSG_UNSUBSCRIBE {
		body, ret := ReadAppUserID(buff)
		message.body = body
		return ret
	} else if cmd == MSG_PUBLISH {
		body, ret := ReadAppMessage(buff)
		message.body = body
		return ret
	} else {
		log.Warning("unknown cmd:", cmd)
		return false
	}
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
	} else if cmd == MSG_ADD_CLIENT {
		body := message.body.(*MessageAddClient)
		data["body"] = map[string]interface{}{
			"uid":       body.uid,
			"timestamp": body.timestamp,
		}
	} else if cmd == MSG_REMOVE_CLIENT {
		data["body"] = message.body.(int64)
	} else if cmd == MSG_ACK {
		data["body"] = message.body.(MessageACK)
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
		data["body"] = message.body.(string)
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

	case MSG_ADD_CLIENT:
		uid, err := msg.Get("body").Get("uid").Int64()
		if err != nil {
			log.Info("get uid fail")
			return false
		}

		timestamp, err := msg.Get("body").Get("timestamp").Int()
		if err != nil {
			log.Info("get timestamp fail")
			return false
		}

		data := &MessageAddClient{}
		data.uid = uid
		data.timestamp = int32(timestamp)
		message.body = data
		return true

	case MSG_REMOVE_CLIENT:
		body, err := msg.Get("body").Int64()
		if err != nil {
			log.Info("read body fail")
			return false
		}
		message.body = body
		return true
	case MSG_ACK:
		body, err := msg.Get("body").Int()
		if err != nil {
			log.Info("read body fail")
			return false
		}
		message.body = MessageACK(body)
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
		message.body = body
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

func WriteHeader(len int32, seq int32, cmd byte, buffer *bytes.Buffer) {
	binary.Write(buffer, binary.BigEndian, len)
	binary.Write(buffer, binary.BigEndian, seq)
	buffer.WriteByte(cmd)
	buffer.WriteByte(byte(0))
	buffer.WriteByte(byte(0))
	buffer.WriteByte(byte(0))
}

func ReadHeader(buff []byte) (int, int, int) {
	var length int32
	var seq int32
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &length)
	binary.Read(buffer, binary.BigEndian, &seq)
	cmd, _ := buffer.ReadByte()
	return int(length), int(seq), int(cmd)
}

func WriteIMMessage(message *IMMessage) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, message.sender)
	binary.Write(buffer, binary.BigEndian, message.receiver)
	binary.Write(buffer, binary.BigEndian, message.timestamp)
	binary.Write(buffer, binary.BigEndian, message.msgid)
	buffer.Write([]byte(message.content))
	buf := buffer.Bytes()
	return buf
}

func ReadIMMessage(buff []byte) (*IMMessage, bool) {
	if len(buff) < 24 {
		return nil, false
	}
	buffer := bytes.NewBuffer(buff)
	im := &IMMessage{}
	binary.Read(buffer, binary.BigEndian, &im.sender)
	binary.Read(buffer, binary.BigEndian, &im.receiver)
	binary.Read(buffer, binary.BigEndian, &im.timestamp)
	binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[24:])
	return im, true
}

func WriteAuth(auth *Authentication) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.uid)
	buf := buffer.Bytes()
	return buf
}

func ReadAuth(buff []byte) (*Authentication, bool) {
	if len(buff) != 9 {
		return nil, false
	}
	auth := &Authentication{}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.uid)
	return auth, true
}

func WriteAuthToken(auth *AuthenticationToken) []byte {
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

func ReadAuthToken(buff []byte) (*AuthenticationToken, bool) {
	var l int8
	if (len(buff) <= 3) {
		return nil, false
	}
	auth := &AuthenticationToken{}
	auth.platform_id = int8(buff[0])

	buffer := bytes.NewBuffer(buff[1:])

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return nil, false
	}
	token := make([]byte, l)
	buffer.Read(token)

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return nil, false
	}
	device_id := make([]byte, l)
	buffer.Read(device_id)

	auth.token = string(token)
	auth.device_id = string(device_id)
	return auth, true
}

func WriteAuthStatus(auth *AuthenticationStatus) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.status)
	buf := buffer.Bytes()
	return buf
}

func ReadAuthStatus(buff []byte) (*AuthenticationStatus, bool) {
	buffer := bytes.NewBuffer(buff)
	s := &AuthenticationStatus{}
	binary.Read(buffer, binary.BigEndian, &s.status)
	return s, true
}

func WriteLoginPoint(point *LoginPoint) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, point.up_timestamp)
	binary.Write(buffer, binary.BigEndian, point.platform_id)
	buffer.Write([]byte(point.device_id))
	buf := buffer.Bytes()
	return buf
}

func ReadLoginPoint(buff []byte) (*LoginPoint, bool) {
	if len(buff) <= 5 {
		return nil, false
	}

	buffer := bytes.NewBuffer(buff)
	point := &LoginPoint{}
	binary.Read(buffer, binary.BigEndian, &point.up_timestamp)
	binary.Read(buffer, binary.BigEndian, &point.platform_id)
	point.device_id = string(buff[5:])
	return point, true
}

func WriteAddClient(ac *MessageAddClient) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ac.uid)
	binary.Write(buffer, binary.BigEndian, ac.timestamp)
	buf := buffer.Bytes()
	return buf
}

func ReadAddClient(buff []byte) (*MessageAddClient, bool) {
	buffer := bytes.NewBuffer(buff)
	ac := &MessageAddClient{}
	binary.Read(buffer, binary.BigEndian, &ac.uid)
	binary.Read(buffer, binary.BigEndian, &ac.timestamp)
	return ac, true
}

func WriteRemoveClient(uid int64) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, uid)
	buf := buffer.Bytes()
	return buf
}

func ReadRemoveClient(buff []byte) (int64, bool) {
	buffer := bytes.NewBuffer(buff)
	var uid int64
	binary.Read(buffer, binary.BigEndian, &uid)
	return uid, true
}

func WriteACK(ack MessageACK) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(ack))
	buf := buffer.Bytes()
	return buf
}

func ReadACK(buff []byte) (MessageACK, bool) {
	buffer := bytes.NewBuffer(buff)
	var ack int32
	binary.Read(buffer, binary.BigEndian, &ack)
	return MessageACK(ack), true
}

func WritePeerACK(ack *MessagePeerACK) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.sender)
	binary.Write(buffer, binary.BigEndian, ack.receiver)
	binary.Write(buffer, binary.BigEndian, ack.msgid)
	buf := buffer.Bytes()
	return buf
}

func ReadPeerACK(buff []byte) (*MessagePeerACK, bool) {
	if len(buff) < 20 {
		return nil, false
	}
	buffer := bytes.NewBuffer(buff)
	ack := &MessagePeerACK{}
	binary.Read(buffer, binary.BigEndian, &ack.sender)
	binary.Read(buffer, binary.BigEndian, &ack.receiver)
	binary.Read(buffer, binary.BigEndian, &ack.msgid)
	return ack, true
}

func WriteInputing(inputing *MessageInputing) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, inputing.sender)
	binary.Write(buffer, binary.BigEndian, inputing.receiver)
	buf := buffer.Bytes()
	return buf
}

func ReadInputing(buff []byte) (*MessageInputing, bool) {
	if len(buff) < 16 {
		return nil, false
	}
	buffer := bytes.NewBuffer(buff)
	inputing := &MessageInputing{}
	binary.Read(buffer, binary.BigEndian, &inputing.sender)
	binary.Read(buffer, binary.BigEndian, &inputing.receiver)
	return inputing, true
}

func WriteGroupNotification(notification string) []byte {
	return []byte(notification)
}

func ReadGroupNotification(buff []byte) (string, bool) {
	return string(buff), true
}

func WriteState(state *MessageOnlineState) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, state.sender)
	binary.Write(buffer, binary.BigEndian, state.online)
	buf := buffer.Bytes()
	return buf
}

func ReadState(buff []byte) (*MessageOnlineState, bool) {
	buffer := bytes.NewBuffer(buff)
	s := &MessageOnlineState{}
	binary.Read(buffer, binary.BigEndian, &s.sender)
	binary.Read(buffer, binary.BigEndian, &s.online)
	return s, true
}

func ReadSubscribeState(buff []byte) (*MessageSubsribeState, bool) {
	sub := &MessageSubsribeState{}
	buffer := bytes.NewBuffer(buff)
	var count int32
	binary.Read(buffer, binary.BigEndian, &count)
	sub.uids = make([]int64, count)
	for i := 0; i < int(count); i++ {
		binary.Read(buffer, binary.BigEndian, &sub.uids[i])
	}
	return sub, true
}

func WriteOfflineMessage(off *OfflineMessage) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	buf := buffer.Bytes()
	return buf
}

func ReadOfflineMessage(buff []byte) (*OfflineMessage, bool) {
	if len(buff) < 24 {
		return nil, false
	}
	buffer := bytes.NewBuffer(buff)
	off := &OfflineMessage{}
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	return off, true
}

func WriteSAEMessage(sae *SAEMessage) []byte {
	if sae.msg == nil {
		return nil
	}

	if sae.msg.cmd == MSG_SAVE_AND_ENQUEUE {
		log.Warning("recusive sae message")
		return nil
	}

	buffer := new(bytes.Buffer)
	mbuffer := new(bytes.Buffer)
	SendMessage(mbuffer, sae.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)
	var count int16 = int16(len(sae.receivers))
	binary.Write(buffer, binary.BigEndian, count)
	for _, r := range(sae.receivers) {
		binary.Write(buffer, binary.BigEndian, r.appid)
		binary.Write(buffer, binary.BigEndian, r.uid)
	}
	buf := buffer.Bytes()
	return buf
}

func ReadSAEMessage(buff []byte) (*SAEMessage, bool) {
	if len(buff) < 4 {
		return nil, false
	}

	sae := &SAEMessage{}

	buffer := bytes.NewBuffer(buff)
	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return nil, false
	}

	msg_buf := make([]byte, l)
	buffer.Read(msg_buf)
	mbuffer := bytes.NewBuffer(msg_buf)
	//recusive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return nil, false
	}
	sae.msg = msg

	if buffer.Len() < 2 {
		return nil, false
	}
	var count int16
	binary.Read(buffer, binary.BigEndian, &count)
	if buffer.Len() < int(count)*16 {
		return nil, false
	}
	sae.receivers = make([]*AppUserID, count)
	for i := int16(0); i < count; i++ {
		r := &AppUserID{}
		binary.Read(buffer, binary.BigEndian, &r.appid)
		binary.Read(buffer, binary.BigEndian, &r.uid)
		sae.receivers[i] = r
	}
	return sae, true
}

func WriteAppUserID(id *AppUserID) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func ReadAppUserID(buff []byte) (*AppUserID, bool) {
	if len(buff) < 16 {
		return nil, false
	}

	id := &AppUserID{}

	buffer := bytes.NewBuffer(buff)	
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return id, true
}

func WriteResult(result *MessageResult) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, result.status)
	buffer.Write(result.content)
	buf := buffer.Bytes()
	return buf
}

func ReadResult(buff []byte) (*MessageResult, bool) {
	if len(buff) < 4 {
		return nil, false
	}
	result := &MessageResult{}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &result.status)
	result.content = buff[4:]
	return result, true
}

func WriteAppMessage(amsg *AppMessage) []byte {
	if amsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, amsg.appid)
	binary.Write(buffer, binary.BigEndian, amsg.receiver)
	binary.Write(buffer, binary.BigEndian, amsg.msgid)
	mbuffer := new(bytes.Buffer)
	SendMessage(mbuffer, amsg.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	buf := buffer.Bytes()
	return buf
}

func ReadAppMessage(buff []byte) (*AppMessage, bool) {
	if len(buff) < 26 {
		return nil, false
	}

	amsg := &AppMessage{}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &amsg.appid)
	binary.Read(buffer, binary.BigEndian, &amsg.receiver)
	binary.Read(buffer, binary.BigEndian, &amsg.msgid)

	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return nil, false
	}

	msg_buf := make([]byte, l)
	buffer.Read(msg_buf)

	mbuffer := bytes.NewBuffer(msg_buf)
	//recusive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return nil, false
	}
	amsg.msg = msg

	return amsg, true
}

func SendMessage(conn io.Writer, msg *Message) {
	body := msg.ToData()
	buffer := new(bytes.Buffer)
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd), buffer)
	buffer.Write(body)
	buf := buffer.Bytes()
	n, err := conn.Write(buf)
	if err != nil || n != len(buf) {
		log.Info("sock write error")
		return
	}
}

func ReceiveMessage(conn io.Reader) *Message {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd := ReadHeader(buff)
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
	if !message.FromData(buff) {
		log.Warning("parse error")
		return nil
	}
	return message
}
