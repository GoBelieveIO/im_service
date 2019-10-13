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
import sys
import ssl
import traceback

SSL = True
HOST = "imnode2.gobelieve.io"
if SSL:
    PORT = 24430
else:
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
MSG_SYSTEM = 21

MSG_CUSTOMER = 24
MSG_CUSTOMER_SUPPORT = 25

MSG_SYNC = 26
MSG_SYNC_BEGIN = 27
MSG_SYNC_END = 28
MSG_SYNC_NOTIFY = 29

MSG_SYNC_GROUP = 30
MSG_SYNC_GROUP_BEGIN = 31
MSG_SYNC_GROUP_END = 32
MSG_SYNC_GROUP_NOTIFY = 33

MSG_SYNC_KEY  = 34
MSG_GROUP_SYNC_KEY = 35


MSG_NOTIFICATION = 36

#消息的meta信息
MSG_METADATA = 37



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
        self.device_id = ""

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
    elif cmd == MSG_SYNC or cmd == MSG_SYNC_KEY:
        h = struct.pack("!iibbbb", 8, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!q", msg)
        sock.sendall(h+b)
    elif cmd == MSG_SYNC_GROUP or cmd == MSG_GROUP_SYNC_KEY:
        group_id, sync_key = msg
        h = struct.pack("!iibbbb", 16, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qq", group_id, sync_key)
        sock.sendall(h+b)
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
    elif cmd == MSG_SYNC_BEGIN or \
         cmd == MSG_SYNC_END or \
         cmd == MSG_SYNC_NOTIFY:
        sync_key, = struct.unpack("!q", content)
        return cmd, seq, sync_key
    elif cmd == MSG_SYNC_GROUP_BEGIN or \
         cmd == MSG_SYNC_GROUP_END or \
         cmd == MSG_SYNC_GROUP_NOTIFY:
        group_id, sync_key = struct.unpack("!qq", content)
        return cmd, seq, (group_id, sync_key)
    elif cmd == MSG_GROUP_NOTIFICATION:
        return cmd, seq, content
    elif cmd == MSG_METADATA:
        sync_key, prev_sync_key = struct.unpack("!qq", content[:16])
        return cmd, seq, (sync_key, prev_sync_key)    
    else:
        return cmd, seq, content

class Client(object):
    def __init__(self):
        self.seq = 0
        self.sock = None
        self.sync_key = 0

    def connect_server(self, device_id, token, host=None):
        if host is not None:
            address = (host, PORT)
        else:
            address = (HOST, PORT)
        if SSL:
            sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock = ssl.wrap_socket(sock_fd)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
        sock.connect(address)
        auth = AuthenticationToken()
        auth.token = token
        auth.device_id = device_id
        self.seq = self.seq + 1
        send_message(MSG_AUTH_TOKEN, self.seq, auth, sock)
        cmd, _, msg = recv_message(sock)
        if cmd != MSG_AUTH_STATUS or msg != 0:
            return False

        self.sock = sock
        return True

    def close(self):
        self.sock.close()

    def ack_message(self, s):
        self.seq += 1
        send_message(MSG_ACK, self.seq, s, self.sock)

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
                return 0, 0, None
            if rlist:
                cmd, s, m = recv_message(self.sock)
                return cmd, s, m


    def send_sync(self):
        self.seq += 1
        send_message(MSG_SYNC, self.seq, self.sync_key, self.sock)        

    def send_sync_key(self):
        self.seq += 1
        send_message(MSG_SYNC_KEY, self.seq, self.sync_key, self.sock)                

DEVICE_ID = "ec452582-a7a9-11e5-87d3-34363bd464b2"
if __name__ == "__main__":
    #调用app自身的服务器获取连接im服务必须的access token
    url = "http://demo.gobelieve.io/auth/token";
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    uid = 1
    name = "测试用户%d"%uid
    obj = {"uid":uid, "user_name":name}
    resp = requests.post(url, data=json.dumps(obj), headers=headers)
    if resp.status_code != 200:
        sys.exit(1)
    obj = json.loads(resp.text)
    token = obj["token"]
    print "token:", token
    while True:
        try:
            client = Client()
            r = client.connect_server(DEVICE_ID, token)
            if not r:
                continue
            client.send_sync()
            
            while True:
                print "recv message..."
                cmd, s, m = client.recv_message()
                #socket disconnect
                if cmd == 0 and s == 0 and m is None:
                    print "socket disconnect"
                    break

                print "cmd:", cmd
                if cmd == MSG_IM:
                    #handle im
                    print "im:", m.sender, m.receiver, m.timestamp, m.content
                    client.ack_message(s)
                elif cmd == MSG_GROUP_IM:
                    #handle group im
                    print "group im:", m.sender, m.receiver, m.timestamp, m.content
                    client.ack_message(s)
                elif cmd == MSG_SYSTEM:
                    #handle system
                    print "system:", m
                    client.ack_message(s)
                elif cmd == MSG_PONG:
                    print "pong..."
                    continue
                elif cmd == MSG_SYNC_NOTIFY:
                    new_sync_key = m
                    if new_sync_key > client.sync_key:
                        client.send_sync()
                elif cmd == MSG_SYNC_END:
                    new_sync_key = m
                    if new_sync_key > client.sync_key:
                        client.sync_key = new_sync_key
                        client.send_sync_key()
                else:
                    print "unknow message:", cmd
                    continue

        except Exception, e:
            traceback.print_exc()            
            time.sleep(1)
            continue
