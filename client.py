import struct
import socket
import threading
import time
import requests
import json

MSG_HEARTBEAT = 1
MSG_AUTH = 2
MSG_AUTH_STATUS = 3
MSG_IM = 4
MSG_ACK = 5
MSG_RST = 6
MSG_GROUP_NOTIFICATION = 7
MSG_GROUP_IM = 8

class Authentication:
    def __init__(self):
        self.uid = 0

class IMMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.content = ""

def send_message(cmd, seq, msg, sock):
    if cmd == MSG_AUTH:
        h = struct.pack("!iibbbb", 8, seq, cmd, 0, 0, 0)
        b = struct.pack("!q", msg.uid)
        sock.sendall(h + b)
    elif cmd == MSG_IM or cmd == MSG_GROUP_IM:
        length = 16 + len(msg.content)
        h = struct.pack("!iibbbb", length, seq, cmd, 0, 0, 0)
        b = struct.pack("!qq", msg.sender, msg.receiver)
        sock.sendall(h+b+msg.content)
    elif cmd == MSG_ACK:
        h = struct.pack("!iibbbb", 4, seq, cmd, 0, 0, 0)
        b = struct.pack("!i", msg)
        sock.sendall(h + b)
    else:
        print "eeeeee"

def recv_message(sock):
    buf = sock.recv(12)
    if len(buf) != 12:
        return 0, 0, None
    length, seq, cmd = struct.unpack("!iib", buf[:9])
    content = sock.recv(length)
    if len(content) != length:
        return 0, 0, None

    if cmd == MSG_AUTH_STATUS:
        status, = struct.unpack("!i", content)
        return cmd, seq, status
    elif cmd == MSG_IM or cmd == MSG_GROUP_IM:
        im = IMMessage()
        im.sender, im.receiver = struct.unpack("!qq", content[:16])
        im.content = content[16:]
        return cmd, seq, im
    else:
        return cmd, seq, content


task = 0

def connect_server(uid, port):
    seq = 0
    address = ("127.0.0.1", 23000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    sock.connect(address)
    auth = Authentication()
    auth.uid = uid
    seq = seq + 1
    send_message(MSG_AUTH, seq, auth, sock)
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

def recv_cluster_rst(uid):
    global task
    sock1, seq1 = connect_server(uid, 23000)
    time.sleep(2)
    sock2, seq2 = connect_server(uid, 24000)

    while True:
        cmd, s, msg = recv_message(sock1)
        if cmd == MSG_RST:
            task += 1
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

    
def recv_message_client(uid, port=23000):
    def handle_message(cmd, s, msg):
        if cmd == MSG_IM:
            print "cmd:", cmd, msg.content, msg.sender, msg.receiver
            return True
        else:
            return False

    recv_client(uid, port, handle_message)
    print "recv message success"

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
            return True
        else:
            return False
    recv_client(uid, port, handle_message)
    print "recv notification success"

def send_client(uid, receiver, msg_type):
    global task
    sock, seq =  connect_server(uid, 23000)
    for i in range(count):
        im = IMMessage()
        im.sender = uid
        im.receiver = receiver
        if msg_type == MSG_IM:
            im.content = "test%d"%(i,)
        else:
            im.content = "test group %d"%(i,)
        seq += 1
        send_message(msg_type, seq, im, sock)
        recv_message(sock)
    task += 1
    print "send success"

def TestCluster():
    global task
    task = 0
    t3 = threading.Thread(target=recv_message_client, args=(13635273142, 24000))
    t3.setDaemon(True)
    t3.start()
    
    time.sleep(2)
    t2 = threading.Thread(target=send_client, args=(13635273143,13635273142, MSG_IM))
    t2.setDaemon(True)
    t2.start()

    while task < 2:
        time.sleep(1)

    print "test cluster completed"

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
    
def TestMultiLogin():
    global task
    task = 0
    t3 = threading.Thread(target=recv_rst, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()

    while task < 1:
        time.sleep(1)
    print "test multi login completed"

def TestClusterMultiLogin():
    global task
    task = 0
    t3 = threading.Thread(target=recv_cluster_rst, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()

    while task < 1:
        time.sleep(1)
    print "test cluster multi login completed"
    
def TestTimeout():
    sock, seq = connect_server(13635273142, 23000)
    print "waiting timeout"
    r = sock.recv(1024)
    if len(r) == 0:
        print "test timeout completed"

def TestGroup():
    URL = "http://127.0.0.1:23002"

    url = URL + "/groups/"

    group = {"master":100,"members":[1,2], "name":"test"}
    r = requests.post(url, data=json.dumps(group))
    print r.status_code, r.text
    obj = json.loads(r.content)
    group_id = obj["group_id"]

    url = URL + "/groups/%s"%str(group_id)
    r = requests.post(url, data=json.dumps({"uid":1000}))
    print r.status_code, r.text

    url = URL + "/groups/%s/1000"%str(group_id)
    r = requests.delete(url)
    print r.status_code, r.text

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url)
    print r.status_code, r.text
    print "test group completed"

def TestGroupMessage():
    URL = "http://127.0.0.1:23002"

    url = URL + "/groups/"

    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test"}
    r = requests.post(url, data=json.dumps(group))
    print r.status_code
    obj = json.loads(r.content)
    group_id = obj["group_id"]
    
    global task
    task = 0
 
    t3 = threading.Thread(target=recv_group_message_client, args=(13635273143,))
    t3.setDaemon(True)
    t3.start()

    
    t2 = threading.Thread(target=send_client, args=(13635273142, group_id, MSG_GROUP_IM))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url)
    print r.status_code, r.text

    print "test group message completed"

def TestGroupNotification():
    global task
    task = 0

    t3 = threading.Thread(target=notification_recv_client, args=(13635273143,))
    t3.setDaemon(True)
    t3.start()
    time.sleep(1)

    URL = "http://127.0.0.1:23002"

    url = URL + "/groups/"

    group = {"master":13635273142,"members":[13635273142,13635273143], "name":"test"}
    r = requests.post(url, data=json.dumps(group))
    print r.status_code
    obj = json.loads(r.content)
    group_id = obj["group_id"]

    while task < 1:
        time.sleep(1)

    url = URL + "/groups/%s"%str(group_id)
    r = requests.delete(url)
    print r.status_code, r.text

    print "test group notification completed"    
def main():
    TestMultiLogin()
    time.sleep(1)
    TestClusterMultiLogin()
    time.sleep(1)
    TestSendAndRecv()
    time.sleep(1)
    TestOffline()
    time.sleep(1)
    TestCluster()
    time.sleep(1)
    TestTimeout()
    time.sleep(1)
    TestGroup()
    time.sleep(1)
    TestGroupMessage()
    time.sleep(1)
    TestGroupNotification()
    time.sleep(1)

if __name__ == "__main__":
    main()
