import struct
import socket
import threading
import time
MSG_HEARTBEAT = 1
MSG_AUTH = 2
MSG_AUTH_STATUS = 3
MSG_IM = 4
MSG_ACK = 5
MSG_RST = 6
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
    elif cmd == MSG_IM:
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
        return 0, None
    length, seq, cmd = struct.unpack("!iib", buf[:9])
    content = sock.recv(length)
    if len(content) != length:
        return 0, None

    if cmd == MSG_AUTH_STATUS:
        status, = struct.unpack("!i", content)
        return cmd, seq, status
    elif cmd == MSG_IM:
        im = IMMessage()
        im.sender, im.receiver = struct.unpack("!qq", content[:16])
        im.content = content[16:]
        return cmd, seq, im
    else:
        return cmd, seq, content



def recv_rst(uid):
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
        return
    print "auth success"

    cmd, s, msg = recv_message(sock)
    print "cmd", cmd
    assert(cmd == MSG_RST)

count = 1000000
    
def recv_client(uid):
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
        return
    print "auth success"
    for _ in range(count):
        cmd, s, msg = recv_message(sock)
        print "cmd:", cmd, msg.content, msg.sender, msg.receiver
        seq += 1
        send_message(MSG_ACK, seq, s, sock)

    print "recv success"
    
def send_client(uid, receiver):
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
        return
    print "auth success"
    for i in range(count):
        im = IMMessage()
        im.sender = uid
        im.receiver = receiver
        im.content = "test%d"%(i,)
        seq += 1
        send_message(MSG_IM, seq, im, sock)
        recv_message(sock)

    print "send success"

def TestSendAndRecv():
    t3 = threading.Thread(target=recv_client, args=(13635273142,))
    t2.setDaemon(True)
    t3.start()

    t2 = threading.Thread(target=send_client, args=(13635273143,13635273142))
    t2.setDaemon(True)
    t2.start()

    while True:
        time.sleep(1)
    
def TestMultiLogin():
    t3 = threading.Thread(target=recv_rst, args=(13635273142,))
    t3.setDaemon(True)
    t3.start()

    t2 = threading.Thread(target=recv_rst, args=(13635273142,))
    t2.setDaemon(True)
    t2.start()
    while True:
        time.sleep(1)

    
def main():
    TestMultiLogin()

if __name__ == "__main__":
    main()
