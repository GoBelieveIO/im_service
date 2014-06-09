import struct
import socket
import threading
import time
MSG_HEARTBEAT = 1
MSG_AUTH = 2
MSG_IM = 3

class Authentication:
    def __init__(self):
        self.uid = 0

class IMMessage:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.content = ""

def send_message(cmd, msg, sock):
    if cmd == MSG_AUTH:
        h = struct.pack("!ibbbb", 8, cmd, 0, 0, 0)
        b = struct.pack("!q", msg.uid)
        sock.sendall(h + b)
    elif cmd == MSG_IM:
        length = 16 + len(msg.content)
        print "msg len:", len(msg.content)
        h = struct.pack("!ibbbb", length, cmd, 0, 0, 0)
        b = struct.pack("!qq", msg.sender, msg.receiver)
        sock.sendall(h+b+msg.content)
    else:
        print "eeeeee"

def recv_message(sock):
    buf = sock.recv(8)
    if len(buf) != 8:
        return 0, None
    length, cmd = struct.unpack("!ib", buf[:5])
    content = sock.recv(length)
    if len(content) != length:
        return 0, None

    if cmd == MSG_AUTH:
        status, = struct.unpack("!i", content)
        return cmd, status
    elif cmd == MSG_IM:
        im = IMMessage()
        im.sender, im.receiver = struct.unpack("!qq", content[:16])
        im.content = content[16:]
        return cmd, im
    else:
        return cmd, content
    
    
def recv_client():
    address = ("127.0.0.1", 23000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    sock.connect(address)
    auth = Authentication()
    auth.uid = 13635273142
    send_message(MSG_AUTH, auth, sock)
    cmd, msg = recv_message(sock)
    if cmd != MSG_AUTH or msg != 0:
        return
    print "auth success"
    for _ in range(1):
        cmd, msg = recv_message(sock)
        print cmd, msg.content, msg.sender, msg.receiver
    
def send_client():
    address = ("127.0.0.1", 23000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
    sock.connect(address)
    auth = Authentication()
    auth.uid = 13635273143
    send_message(MSG_AUTH, auth, sock)
    cmd, msg = recv_message(sock)
    if cmd != MSG_AUTH or msg != 0:
        return
    print "auth success"
    for i in range(1):
        im = IMMessage()
        im.sender = 13635273143
        im.receiver = 13635273142
        im.content = "test%d"%(i,)
        send_message(MSG_IM, im, sock)
    
def main():
    t1 = threading.Thread(target=recv_client)
    t1.start()
    time.sleep(2)
    t2 = threading.Thread(target=send_client)
    t2.start()
    

if __name__ == "__main__":
    main()
