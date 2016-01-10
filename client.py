# -*- coding: utf-8 -*-
import struct
import socket
import threading
import time
import requests
import json
import uuid
import base64
import md5

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
MSG_ENTER_ROOM = 18
MSG_LEAVE_ROOM = 19
MSG_ROOM_IM = 20
MSG_SYSTEM = 21
MSG_UNREAD_COUNT = 22
MSG_CUSTOMER_SERVICE = 23

PLATFORM_IOS = 1
PLATFORM_ANDROID = 2

PROTOCOL_VERSION = 1

device_id = "f9d2a7c2-701a-11e5-9c3e-34363bd464b2"
class AuthenticationToken:
    def __init__(self):
        self.token = ""
        self.platform_id = PLATFORM_ANDROID
        self.device_id = device_id

class IMMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.timestamp = 0
        self.msgid = 0
        self.content = ""

class CSMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.timestamp = 0
        self.content = ""
    
#RoomMessage
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
    elif cmd == MSG_CUSTOMER_SERVICE:
        length = 20 + len(msg.content)
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qqi", msg.sender, msg.receiver, msg.timestamp)
        sock.sendall(h+b+msg.content)
    elif cmd == MSG_RT or cmd == MSG_ROOM_IM:
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
    elif cmd == MSG_ENTER_ROOM or cmd == MSG_LEAVE_ROOM:
        h = struct.pack("!iibbbb", 8, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!q", msg)
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
        im.sender, im.receiver, _, _ = struct.unpack("!qqii", content[:24])
        im.content = content[24:]
        return cmd, seq, im
    elif cmd == MSG_CUSTOMER_SERVICE:
        cs = CSMessage()
        cs.sender, cs.receiver, cs.timestamp = struct.unpack("!qqi", content[:20])
        cs.content = content[20:]
        return cmd, seq, cs
    elif cmd == MSG_RT or cmd == MSG_ROOM_IM:
        rt = RTMessage()
        rt.sender, rt.receiver, = struct.unpack("!qq", content[:16])
        rt.content = content[16:]
        return cmd, seq, rt
    elif cmd == MSG_ACK:
        ack, = struct.unpack("!i", content)
        return cmd, seq, ack
    elif cmd == MSG_SYSTEM:
        return cmd, seq, content
    elif cmd == MSG_INPUTING:
        sender, receiver = struct.unpack("!qq", content)
        return cmd, seq, (sender, receiver)
    elif cmd == MSG_PONG:
        return cmd, seq, None
    else:
        return cmd, seq, content

APP_ID = 7
APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
APP_SECRET = '0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1'
HOST = "127.0.0.1"
#URL = "http://127.0.0.1:23002"
URL = "http://192.168.33.10"

def login(uid):
    url = URL + "/auth/grant"
    obj = {"uid":uid, "user_name":str(uid)}
    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
     
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return None
    obj = json.loads(res.text)
    return obj["data"]["token"]


task = 0

def connect_server(uid, port):
    token = login(uid)
    if not token:
        return None, 0
    seq = 0
    address = (HOST, port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    sock.connect(address)
    auth = AuthenticationToken()
    auth.token = token
    seq = seq + 1
    send_message(MSG_AUTH_TOKEN, seq, auth, sock)
    cmd, _, msg = recv_message(sock)
    if cmd != MSG_AUTH_STATUS or msg != 0:
        return None, 0
    return sock, seq

def recv_rst(uid):
    global task
    sock1, seq1 = connect_server(uid, 23000)
    sock2, seq2 = connect_server(uid, 23000)

    while True:
        cmd, s, msg = recv_message(sock1)
        if cmd == MSG_RST:
            task += 1
            break

def recv_login_point(uid):
    global task
    sock1, seq1 = connect_server(uid, 23000)

    time.sleep(1)
    sock2, seq2 = connect_server(uid, 23000)

    while True:
        cmd, s, msg = recv_message(sock1)
        if cmd == MSG_LOGIN_POINT:
            print "up timestamp:", msg[0], " platform id:", msg[1], " device_id", msg[2]
            break


count = 1

def recv_client(uid, port, handler):
    global task
    sock, seq =  connect_server(uid, port)
    while True:
        cmd, s, msg = recv_message(sock)
        seq += 1
        send_message(MSG_ACK, seq, s, sock)
        if handler(cmd, s, msg):
            break
    task += 1

def recv_inputing(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_INPUTING:
            print "inputting cmd:", cmd, msg
            return True
        else:
            return False

    recv_client(uid, port, handle_message)
    print "recv inputing success"
    
def recv_message_client(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_IM:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver
            return True
        else:
            print "cmd:", cmd
            return False

    recv_client(uid, port, handle_message)
    print "recv message success"

def recv_customer_service_message_client(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_CUSTOMER_SERVICE:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver, msg.timestamp
            return True
        else:
            print "cmd:", cmd
            return False

    recv_client(uid, port, handle_message)
    print "recv customer service message success"
    
def recv_rt_message_client(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_RT:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver
            return True
        else:
            print "cmd:", cmd
            return False

    recv_client(uid, port, handle_message)
    print "recv rt message success"

def recv_group_message_client(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_GROUP_IM:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver
            return True
        elif cmd == MSG_IM:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver
            return False
        else:
            print "cmd:", cmd, "body:", msg
            return False
    recv_client(uid, port, handle_message)
    print "recv group message success"

def notification_recv_client(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_GROUP_NOTIFICATION:
            notification = json.loads(msg)
            print "cmd:", cmd, " ", msg
            if notification.has_key("create"):
                return True
            else:
                return False
        else:
            return False
    recv_client(uid, port, handle_message)
    print "recv notification success"

def recv_room_client(uid, port, room_id, handler):
    global task
    sock, seq =  connect_server(uid, port)

    seq += 1
    send_message(MSG_ENTER_ROOM, seq, room_id, sock)

    while True:
        cmd, s, msg = recv_message(sock)
        seq += 1
        send_message(MSG_ACK, seq, s, sock)
        if handler(cmd, s, msg):
            break
    task += 1

def recv_room_message_client(uid, room_id, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_ROOM_IM:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver
            return True
        else:
            print "cmd:", cmd, msg
            return False

    recv_room_client(uid, port, room_id, handle_message)
    print "recv room message success"


def send_room_message_client(uid, room_id):
    global task
    sock, seq =  connect_server(uid, 23000)

    seq += 1
    send_message(MSG_ENTER_ROOM, seq, room_id, sock)
    
    im = RTMessage()
    im.sender = uid
    im.receiver = room_id
    im.content = "test room im"
    seq += 1
    send_message(MSG_ROOM_IM, seq, im, sock)
    task += 1
    print "send success"

def send_customer_service_message_client(uid):
    global task
    sock, seq =  connect_server(uid, 23000)

    cs = CSMessage()
    cs.sender = uid
    cs.receiver = 0
    cs.content = "test customer service"
    seq += 1
    send_message(MSG_CUSTOMER_SERVICE, seq, cs, sock)
    task += 1
    print "send customer service success"

def send_rt_client(uid, receiver):
    global task
    sock, seq =  connect_server(uid, 23000)
    im = RTMessage()
    im.sender = uid
    im.receiver = receiver
    im.content = "test rt"
    seq += 1
    send_message(MSG_RT, seq, im, sock)
    task += 1
    print "send success"


def send_client(uid, receiver, msg_type):
    global task
    sock, seq =  connect_server(uid, 23000)
    im = IMMessage()
    im.sender = uid
    im.receiver = receiver
    if msg_type == MSG_IM:
        im.content = "test im"
    else:
        im.content = "test group im"
    seq += 1
    send_message(msg_type, seq, im, sock)
    msg_seq = seq
    while True:
        cmd, s, msg = recv_message(sock)
        if cmd == MSG_ACK and msg == msg_seq:
            break
        elif cmd == MSG_GROUP_NOTIFICATION:
            print "send ack..."
            seq += 1
            send_message(MSG_ACK, seq, s, sock)
        else:
            print "cmd:", cmd, " ", msg
    task += 1
    print "send success"

def send_inputing(uid, receiver):
    global task
    sock, seq =  connect_server(uid, 23000)

    m = (uid, receiver)
    seq += 1
    send_message(MSG_INPUTING, seq, m, sock)
    task += 1
    
def send_http_peer_message(uid, receiver):
    global task
    url = URL + "/messages/peers"
    content = json.dumps({"text":"test"})
    obj = {"sender":uid, "receiver":receiver, "content":content}
    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
     
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return
    print "send http peer message:", res.status_code
    task += 1


def send_http_group_message(uid, receiver):
    global task
    url = URL + "/messages/groups"
    content = json.dumps({"text":"test"})
    obj = {"sender":uid, "receiver":receiver, "content":content}
    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
     
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return
    print "send http group message:", res.status_code
    task += 1
    
def send_system_message(uid):
    global task
    url = URL + "/messages/systems"
    obj = {
        "receiver":uid,
        "content":"system message content"
    }
    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
     
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return
    print "send system message:", res.status_code
    task += 1
    
def recv_system_message_client(uid):
    def handle_message(cmd, s, msg):
        if cmd == MSG_SYSTEM:
            print "cmd:", cmd, msg
            return True
        else:
            print "cmd:", cmd, msg
            return False

    recv_client(uid, 23000, handle_message)
    print "recv system message success"

    
def TestCluster():
    global task
    task = 0
    t3 = threading.Thread(target=recv_message_client, args=(13635273142, 24000))
    t3.setDaemon(True)
    t3.start()
    
    time.sleep(1)

    t2 = threading.Thread(target=send_client, args=(13635273143,13635273142, MSG_IM))
    t2.setDaemon(True)
    t2.start()

    while task < 2:
        time.sleep(1)

    print "test cluster completed"

def TestRTSendAndRecv():
    global task
    task = 0
 
    t3 = threading.Thread(target=recv_rt_message_client, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_rt_client, args=(13635273143,13635273142))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)
    print "test rt  completed"

def TestSendAndRecv():
    global task
    task = 0
 
    t3 = threading.Thread(target=recv_message_client, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()
    
    time.sleep(1)
    
    t2 = threading.Thread(target=send_client, args=(13635273143,13635273142, MSG_IM))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)
    print "test single completed"

def TestHttpSendAndRecv():
    global task
    task = 0
    t3 = threading.Thread(target=recv_message_client, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()
    
    time.sleep(1)
    
    t2 = threading.Thread(target=send_http_peer_message, args=(13635273143,13635273142))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

    print "test http peer message completed"

    
def TestOffline():
    global task
    task = 0
    t2 = threading.Thread(target=send_client, args=(13635273143,13635273142, MSG_IM))
    t2.setDaemon(True)
    t2.start()
    
    time.sleep(1)

    t3 = threading.Thread(target=recv_message_client, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()

    while task < 2:
        time.sleep(1)

    print "test offline completed"

def TestInputing():
    global task
    task = 0

    t3 = threading.Thread(target=recv_inputing, args=(13635273142,23000))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)

    t2 = threading.Thread(target=send_inputing, args=(13635273143,13635273142))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)

    print "test inputting completed"

def TestLoginPoint():
    recv_login_point(13635273142)
    print "test login point completed"

def TestTimeout():
    sock, seq = connect_server(13635273142, 23000)
    print "waiting timeout"
    while True:
        r = sock.recv(1024)
        if len(r) == 0:
            print "test timeout completed"
            break

def TestPingPong():
    uid = 13635273142
    sock, seq =  connect_server(uid, 23000)
    seq += 1
    send_message(MSG_PING, seq, None, sock)
    while True:
        cmd, _, msg = recv_message(sock)
        if cmd == MSG_PONG:
            print "test ping/pong completed"
            return
        else:
            continue


def TestBindToken():
    access_token = login(13635273142)
    url = URL + "/device/bind"

    obj = {"apns_device_token":"11"}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"

    r = requests.post(url, data=json.dumps(obj), headers = headers)
    print "bind device token:", r.status_code

    url = URL + "/device/unbind"
    r = requests.post(url, data=json.dumps(obj), headers = headers)
    print "unbind device token:", r.status_code
    
    
def TestGroup():
    access_token = login(13635273142)
    url = URL + "/groups"

    group = {"master":13635273142,"members":[13635273142], "name":"test", "super":True}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"

    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]


    url = URL + "/groups/%s"%str(group_id)
    r = requests.patch(url, data=json.dumps({"name":"test_new"}), headers = headers)
    assert(r.status_code == 200)

    url = URL + "/groups/%s/members"%str(group_id)
    r = requests.post(url, data=json.dumps({"uid":13635273143}), headers = headers)
    assert(r.status_code == 200)


    url = URL + "/groups/%s/members"%str(group_id)
    r = requests.post(url, data=json.dumps([13635273144,13635273145]), headers = headers)
    assert(r.status_code == 200)


    url = URL + "/groups/%s/members/13635273143"%str(group_id)
    r = requests.delete(url, headers = headers)

    assert(r.status_code == 200)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers = headers)


    print "test group completed"


def TestSendHttpGroupMessage():
    global task
    task = 0

    t3 = threading.Thread(target=recv_group_message_client, args=(13635273143,23000))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)

    #create group
    is_super = False
    access_token = login(13635273142)
    url = URL + "/groups"
    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test", "super":is_super}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"
    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]
    print "group id:", group_id
    time.sleep(1)

    t2 = threading.Thread(target=send_http_group_message, args=(13635273142, group_id))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers=headers)
    assert(r.status_code == 200)

    print "test send http group message completed"


def TestSuperGroupOffline():
    _TestGroupOffline(True)
    print "test super group offline message completed"

def TestGroupOffline():
    _TestGroupOffline(False)

    print "test group offline message completed"

def _TestGroupOffline(is_super):
    access_token = login(13635273142)

    url = URL + "/groups"

    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test", "super":is_super}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"
    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]
    
    print "group id:", group_id

    global task
    task = 0

    t2 = threading.Thread(target=send_client, args=(13635273142, group_id, MSG_GROUP_IM))
    t2.setDaemon(True)
    t2.start()
    
    time.sleep(1)

    t3 = threading.Thread(target=recv_group_message_client, args=(13635273143,23000))
    t3.setDaemon(True)
    t3.start()

    while task < 2:
        time.sleep(1)


    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers=headers)
    assert(r.status_code == 200)

    
def _TestGroupMessage(is_super, port):
    global task
    task = 0

    t3 = threading.Thread(target=recv_group_message_client, args=(13635273143,port))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)

    #create group
    access_token = login(13635273142)
    url = URL + "/groups"
    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test", "super":is_super}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"
    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]
    print "group id:", group_id
    time.sleep(1)

    t2 = threading.Thread(target=send_client, args=(13635273142, group_id, MSG_GROUP_IM))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers=headers)
    assert(r.status_code == 200)


def TestSuperGroupMessage():
    _TestGroupMessage(True, 23000)
    print "test super group message completed"    

def TestGroupMessage():
    _TestGroupMessage(False, 23000)
    print "test group message completed"    

def TestClusterSuperGroupMessage():
    _TestGroupMessage(True, 24000)
    print "test cluster super group message completed"    

def TestClusterGroupMessage():
    _TestGroupMessage(False, 24000)
    print "test cluster group message completed"    

def _TestGroupNotification(is_super):
    global task
    task = 0

    t3 = threading.Thread(target=notification_recv_client, args=(13635273143,))
    t3.setDaemon(True)
    t3.start()
    time.sleep(1)


    access_token = login(13635273142)

    url = URL + "/groups"

    group = {"master":13635273142,"members":[13635273142,13635273143], 
             "super":is_super, "name":"test"}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"
    r = requests.post(url, data=json.dumps(group), headers=headers)
    print r.status_code
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]

    while task < 1:
        time.sleep(1)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers=headers)
    print r.status_code, r.text



def TestGroupNotification():
    _TestGroupNotification(False)
    print "test group notification completed"  

def TestSuperGroupNotification():
    _TestGroupNotification(True)
    print "test super group notification completed"  


def _TestRoomMessage(port):
    global task
    task = 0
 
    room_id = 1
    t3 = threading.Thread(target=recv_room_message_client, args=(13635273143, room_id, port))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_room_message_client, args=(13635273142, room_id))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

def TestRoomMessage():
    _TestRoomMessage(23000)
    print "test room message completed"

def TestClusterRoomMessage():
    _TestRoomMessage(24000)
    print "test cluster room message completed"


def TestSystemMessage():
    global task
    task = 0
 
    room_id = 1
    t3 = threading.Thread(target=recv_system_message_client, args=(13635273142, ))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_system_message, args=(13635273142, ))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

    print "test system message completed"
    

def add_customer_service_staff(staff_uid):
    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}

    url = URL + "/applications/%s"%APP_ID

    obj = {"customer_service":True}
    r = requests.patch(url, data=json.dumps(obj), headers=headers)
    assert(r.status_code == 200)
    print "enable customer service success"

    obj = {"customer_service_mode":3}
    r = requests.patch(url, data=json.dumps(obj), headers=headers)
    assert(r.status_code == 200)
    print "set customer service mode:3 success"

    url = URL + "/applications/%s/staffs"%APP_ID
    obj = {"staff_uid":staff_uid, "staff_name":"客服"}
    r = requests.post(url, data=json.dumps(obj), headers=headers)
    assert(r.status_code == 200)
    print "add customer service staff success"



def TestCustomerServiceMessage():
    global task
    task = 0
    t3 = threading.Thread(target=recv_customer_service_message_client, args=(100, ))
    t3.setDaemon(True)
    t3.start()
    

    add_customer_service_staff(100)
    
    time.sleep(1)

    t2 = threading.Thread(target=send_customer_service_message_client, args=(13635273142, ))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

    print "test customer service message completed"

def main():
    cluster = True

    TestBindToken()
    time.sleep(1)
     
    TestGroup()
    time.sleep(1)
     
    TestGroupNotification()
    time.sleep(1)
     
    TestSuperGroupNotification()
    time.sleep(1)
     
    TestGroupMessage()
    time.sleep(1)
     
    TestSuperGroupMessage()
    time.sleep(1)
     
    TestGroupOffline()
    time.sleep(1)
     
    TestSuperGroupOffline()
    time.sleep(1)
     
    if cluster:
        TestClusterGroupMessage()
        time.sleep(1)
     
        TestClusterSuperGroupMessage()
        time.sleep(1)
     
    TestInputing()
    time.sleep(1)
     
    TestRTSendAndRecv()
    time.sleep(1)
     
    TestRoomMessage()
    time.sleep(1)
     
    if cluster:
        TestClusterRoomMessage()
     
    TestSendAndRecv()
    time.sleep(1)
     
    TestOffline()
    time.sleep(1)
     
    if cluster:
        TestCluster()
        time.sleep(1)
     
    TestHttpSendAndRecv()
    time.sleep(1)
     
     
    TestSendHttpGroupMessage()
    time.sleep(1)
     
    TestSystemMessage()
    time.sleep(1)

    TestCustomerServiceMessage()
    time.sleep(1)

    TestLoginPoint()
    time.sleep(1)
    TestPingPong()
    time.sleep(1)
    TestTimeout()
    time.sleep(1)
    


if __name__ == "__main__":
    main()
