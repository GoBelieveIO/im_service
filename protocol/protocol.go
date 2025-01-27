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

package protocol

import (
	"bytes"
	"encoding/binary"
	"io"

	"encoding/hex"
	"errors"

	log "github.com/sirupsen/logrus"
)

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
	WriteHeader(int32(len(body)), int32(msg.Seq), byte(msg.Cmd), byte(msg.Version), byte(msg.Flag), w)
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

func ReceiveLimitMessage(conn io.Reader, limit_size int, external bool) (*Message, error) {
	buff := make([]byte, 12)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil, err
	}

	length, seq, cmd, version, flag := ReadHeader(buff)
	if length < 0 || length >= limit_size {
		log.Info("invalid len:", length)
		return nil, errors.New("invalid length")
	}

	//0 <= cmd <= 255
	//收到客户端非法消息，断开链接
	if external && !external_messages[cmd] {
		log.Warning("invalid external message cmd:", Command(cmd))
		return nil, errors.New("invalid cmd")
	}

	buff = make([]byte, length)
	_, err = io.ReadFull(conn, buff)
	if err != nil {
		log.Info("sock read error:", err)
		return nil, err
	}

	message := new(Message)
	message.Cmd = cmd
	message.Seq = seq
	message.Version = version
	message.Flag = flag
	if !message.FromData(buff) {
		log.Warningf("parse error:%d, %d %d %d %s", cmd, seq, version,
			flag, hex.EncodeToString(buff))
		return nil, errors.New("parse error")
	}
	return message, nil
}

func ReceiveMessage(conn io.Reader) *Message {
	m, _ := ReceiveLimitMessage(conn, 32*1024, false)
	return m
}

// used by benchmark
func ReceiveServerMessage(conn io.Reader) (*Message, error) {
	return ReceiveLimitMessage(conn, 32*1024, false)
}

// 接受客户端消息(external messages)
func ReceiveClientMessage(conn io.Reader) (*Message, error) {
	return ReceiveLimitMessage(conn, 32*1024, true)
}

// 消息大小限制在32M
func ReceiveStorageSyncMessage(conn io.Reader) *Message {
	m, _ := ReceiveLimitMessage(conn, 32*1024*1024, false)
	return m
}

// 消息大小限制在1M
func ReceiveStorageMessage(conn io.Reader) *Message {
	m, _ := ReceiveLimitMessage(conn, 1024*1024, false)
	return m
}
