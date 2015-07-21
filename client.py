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

PLATFORM_IOS = 1
PLATFORM_ANDROID = 2

PROTOCOL_VERSION = 1

class AuthenticationToken:
    def __init__(self):
        self.token = ""
        self.platform_id = PLATFORM_ANDROID
        self.device_id = str(uuid.uuid1())

class IMMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.timestamp = 0
        self.msgid = 0
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
    elif cmd == MSG_RT or cmd == MSG_ROOM_IM:
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

APP_ID = 7
APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
APP_SECRET = '0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1'
HOST = "127.0.0.1"
URL = "http://127.0.0.1:23002"

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
            return False

    recv_client(uid, port, handle_message)
    print "recv message success"

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
            print "cmd:", cmd, msg
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
        elif cmd == MSG_PEER_ACK or cmd == MSG_GROUP_NOTIFICATION:
            print "send ack..."
            seq += 1
            send_message(MSG_ACK, seq, s, sock)
        else:
            print "cmd:", cmd, " ", msg
    task += 1
    print "send success"

def send_wait_peer_ack_client(uid, receiver):
    global task
    sock, seq =  connect_server(uid, 23000)
    im = IMMessage()
    im.sender = uid
    im.receiver = receiver
    im.content = "test im"
    seq += 1
    send_message(MSG_IM, seq, im, sock)
    while True:
        cmd, s, msg = recv_message(sock)
        if cmd == MSG_PEER_ACK:
            seq += 1
            send_message(MSG_ACK, seq, s, sock)
            break
        else:
            print "cmd:", cmd, " ", msg
    task += 1
    print "peer ack received"

def send_inputing(uid, receiver):
    global task
    sock, seq =  connect_server(uid, 23000)

    m = (uid, receiver)
    seq += 1
    send_message(MSG_INPUTING, seq, m, sock)
    task += 1
    

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

    
    t2 = threading.Thread(target=send_client, args=(13635273143,13635273142, MSG_IM))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)
    print "test single completed"

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

def TestPeerACK():
    global task
    task = 0

    t3 = threading.Thread(target=recv_message_client, args=(13635273142,23000))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)

    t2 = threading.Thread(target=send_wait_peer_ack_client, args=(13635273143,13635273142))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)

    print "test peer ack completed"

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

    group = {"master":13635273142,"members":[13635273142], "name":"test"}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"

    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]

    url = URL + "/groups/%s/members"%str(group_id)
    r = requests.post(url, data=json.dumps({"uid":13635273143}), headers = headers)
    assert(r.status_code == 200)

    url = URL + "/groups/%s/members/13635273143"%str(group_id)
    r = requests.delete(url, headers = headers)

    assert(r.status_code == 200)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers = headers)


    print "test group completed"


def TestGroupOffline():
    access_token = login(13635273142)

    url = URL + "/groups"

    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test"}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"
    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]
    
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

    print "test group offline message completed"
    
def _TestGroupMessage(port):
    access_token = login(13635273142)

    url = URL + "/groups"

    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test"}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"
    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]
    
    global task
    task = 0


    t3 = threading.Thread(target=recv_group_message_client, args=(13635273143,port))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_client, args=(13635273142, group_id, MSG_GROUP_IM))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url, headers=headers)
    assert(r.status_code == 200)

    print "test group message completed"

def TestGroupMessage():
    _TestGroupMessage(23000)

def TestClusterGroupMessage():
    _TestGroupMessage(24000)

def TestGroupNotification():
    global task
    task = 0

    t3 = threading.Thread(target=notification_recv_client, args=(13635273143,))
    t3.setDaemon(True)
    t3.start()
    time.sleep(1)


    access_token = login(13635273142)

    url = URL + "/groups"

    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test"}
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

    print "test group notification completed"  

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

    print "test room message completed"

def TestRoomMessage():
    _TestRoomMessage(23000)

def TestClusterRoomMessage():
    _TestRoomMessage(24000)

def main():
    cluster = False
     
    TestBindToken()
    time.sleep(1)
     
    TestGroup()
    time.sleep(1)
    TestGroupNotification()
    time.sleep(1)
    TestGroupMessage()
    time.sleep(1)
    TestGroupOffline()
    time.sleep(1)

    if cluster:
        TestClusterGroupMessage()
        time.sleep(1)
     
    TestPeerACK()
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
     
    TestLoginPoint()
    time.sleep(1)
    TestPingPong()
    time.sleep(1)
    TestTimeout()
    time.sleep(1)
    


if __name__ == "__main__":
    main()
