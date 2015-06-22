# -*- coding: utf-8 -*-

import struct
import socket
import threading
import time
import requests
import json
import uuid
import base64
import select


HOST = "imnode.gobelieve.io"
PORT = 23000

#command
MSG_HEARTBEAT = 1
#MSG_AUTH = 2
MSG_AUTH_STATUS = 3
MSG_IM = 4
MSG_ACK = 5
MSG_RST = 6
MSG_GROUP_NOTIFICATION = 7
MSG_GROUP_IM = 8
MSG_PEER_ACK = 9
MSG_INPUTING = 10
MSG_SUBSCRIBE_ONLINE_STATE = 11
MSG_ONLINE_STATE = 12
MSG_PING = 13
MSG_PONG = 14
MSG_AUTH_TOKEN = 15
MSG_LOGIN_POINT = 16
MSG_RT = 17

#platform
PLATFORM_IOS = 1
PLATFORM_ANDROID = 2
PLATFORM_WEB = 3
PLATFORM_SERVER = 4

PROTOCOL_VERSION = 1

class AuthenticationToken:
    def __init__(self):
        self.token = ""
        self.platform_id = PLATFORM_SERVER
        self.device_id = str(uuid.uuid1())

class IMMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.timestamp = 0
        self.msgid = 0
        self.content = ""
class RTMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.content = ""
    
def send_message(cmd, seq, msg, sock):
    if cmd == MSG_AUTH_TOKEN:
        b = struct.pack("!BB", msg.platform_id, len(msg.token)) + msg.token + struct.pack("!B", len(msg.device_id)) + msg.device_id
        length = len(b)
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        sock.sendall(h+b)
    elif cmd == MSG_IM or cmd == MSG_GROUP_IM:
        length = 24 + len(msg.content)
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qqii", msg.sender, msg.receiver, msg.timestamp, msg.msgid)
        sock.sendall(h+b+msg.content)
    elif cmd == MSG_RT:
        length = 16 + len(msg.content)
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qq", msg.sender, msg.receiver)
        sock.sendall(h+b+msg.content)
    elif cmd == MSG_ACK:
        h = struct.pack("!iibbbb", 4, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!i", msg)
        sock.sendall(h + b)
    elif cmd == MSG_INPUTING:
        sender, receiver = msg
        h = struct.pack("!iibbbb", 16, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qq", sender, receiver)
        sock.sendall(h + b)
    elif cmd == MSG_PING:
        h = struct.pack("!iibbbb", 0, seq, cmd, PROTOCOL_VERSION, 0, 0)
        sock.sendall(h)
    else:
        print "eeeeee"

def recv_message(sock):
    buf = sock.recv(12)
    if len(buf) != 12:
        return 0, 0, None
    length, seq, cmd = struct.unpack("!iib", buf[:9])

    if length == 0:
        return cmd, seq, None

    content = sock.recv(length)
    if len(content) != length:
        return 0, 0, None

    if cmd == MSG_AUTH_STATUS:
        status, = struct.unpack("!i", content)
        return cmd, seq, status
    elif cmd == MSG_LOGIN_POINT:
        up_timestamp, platform_id = struct.unpack("!ib", content[:5])
        device_id = content[5:]
        return cmd, seq, (up_timestamp, platform_id, device_id)
    elif cmd == MSG_IM or cmd == MSG_GROUP_IM:
        im = IMMessage()
        im.sender, im.receiver, im.timestamp, im.msgid = struct.unpack("!qqii", content[:24])
        im.content = content[24:]
        return cmd, seq, im
    elif cmd == MSG_RT:
        rt = RTMessage()
        rt.sender, rt.receiver, = struct.unpack("!qq", content[:16])
        rt.content = content[16:]
        return cmd, seq, rt
    elif cmd == MSG_ACK:
        ack, = struct.unpack("!i", content)
        return cmd, seq, ack
    elif cmd == MSG_INPUTING:
        sender, receiver = struct.unpack("!qq", content)
        return cmd, seq, (sender, receiver)
    elif cmd == MSG_PONG:
        return cmd, seq, None
    else:
        return cmd, seq, content

class Client(object):
    def __init__(self):
        self.seq = 0
        self.sock = None

    def connect_server(self, token, host=None):
        if host is not None:
            address = (host, PORT)
        else:
            address = (HOST, PORT)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
        sock.connect(address)
        auth = AuthenticationToken()
        auth.token = token
        self.seq = self.seq + 1
        send_message(MSG_AUTH_TOKEN, self.seq, auth, sock)
        cmd, _, msg = recv_message(sock)
        if cmd != MSG_AUTH_STATUS or msg != 0:
            return False

        self.sock = sock
        return True

    def close(self):
        self.sock.close()

    def recv_message(self):
        while True:
            rlist, _, xlist = select.select([self.sock], [], [self.sock], 60)
            if not rlist and not xlist:
                #timeout
                self.seq += 1
                print "ping..."
                send_message(MSG_PING, self.seq, None, self.sock)
                continue
            if xlist:
                return None
            if rlist:
                cmd, s, m = recv_message(self.sock)
                if cmd == 0 and s == 0 and m is None:
                    return None
                if cmd == MSG_IM:
                    self.seq += 1
                    send_message(MSG_ACK, self.seq, s, self.sock)
                    return m
                elif cmd == MSG_PEER_ACK:
                    print "peer ack"
                    continue
                elif cmd == MSG_PONG:
                    print "pong..."
                    continue
                else:
                    print "unknow message:", cmd
                    continue

