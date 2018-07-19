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
import "errors"
import "encoding/hex"

//平台号
const PLATFORM_IOS = 1
const PLATFORM_ANDROID = 2
const PLATFORM_WEB = 3

const DEFAULT_VERSION = 1

const MSG_HEADER_SIZE = 12

var message_descriptions map[int]string = make(map[int]string)

type MessageCreator func()IMessage
var message_creators map[int]MessageCreator = make(map[int]MessageCreator)

type VersionMessageCreator func()IVersionMessage
var vmessage_creators map[int]VersionMessageCreator = make(map[int]VersionMessageCreator)



func WriteHeader(len int32, seq int32, cmd byte, version byte, flag byte, buffer io.Writer) {
	binary.Write(buffer, binary.BigEndian, len)
	binary.Write(buffer, binary.BigEndian, seq)
	t := []byte{cmd, byte(version), flag, 0}
	buffer.Write(t)
}

func ReadHeader(buff []byte) (int, int, int, int, int) {
	var length int32
	var seq int32
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &length)
	binary.Read(buffer, binary.BigEndian, &seq)
	cmd, _ := buffer.ReadByte()
	version, _ := buffer.ReadByte()
	flag, _ := buffer.ReadByte()
	return int(length), int(seq), int(cmd), int(version), int(flag)
}

func WriteMessage(w *bytes.Buffer, msg *Message) {
	body := msg.ToData()
	WriteHeader(int32(len(body)), int32(msg.seq), byte(msg.cmd), byte(msg.version), byte(msg.flag), w)
	w.Write(body)
}

func SendMessage(conn io.Writer, msg *Message) error {
	buffer := new(bytes.Buffer)
	WriteMessage(buffer, msg)
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

func ReceiveLimitMessage(conn io.Reader, limit_size int) *Message {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil
	}

	length, seq, cmd, version, flag := ReadHeader(buff)
	if length < 0 || length >= limit_size {
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
	message.flag = flag
	if !message.FromData(buff) {
		log.Warningf("parse error:%d, %d %d %d %s", cmd, seq, version,
			flag, hex.EncodeToString(buff))
		return nil
	}
	return message
}


func ReceiveMessage(conn io.Reader) *Message {
	return ReceiveLimitMessage(conn, 32*1024)
}

//消息大小限制在1M
func ReceiveStorageMessage(conn io.Reader) *Message {
	return ReceiveLimitMessage(conn, 1024*1024)
}


