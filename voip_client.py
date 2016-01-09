import struct
import socket
import threading
import time
import requests
import json
import sys
import os
import select
import md5
import base64

MSG_HEARTBEAT = 1
MSG_AUTH = 2
MSG_AUTH_STATUS = 3
MSG_ACK = 5
MSG_PING = 13
MSG_PONG = 14
MSG_AUTH_TOKEN = 15

MSG_VOIP_CONTROL = 64
MSG_VOIP_DATA = 65


VOIP_COMMAND_DIAL = 1
VOIP_COMMAND_ACCEPT = 2
VOIP_COMMAND_CONNECTED = 3
VOIP_COMMAND_REFUSE = 4
VOIP_COMMAND_REFUSED = 5
VOIP_COMMAND_HANG_UP = 6
VOIP_COMMAND_RESET = 7
VOIP_COMMAND_TALKING = 8


PLATFORM_IOS = 1
PLATFORM_ANDROID = 2

PROTOCOL_VERSION = 1

APP_ID = 7
APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
APP_SECRET = '0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1'

HOST = "127.0.0.1"
PORT = 23000
URL = "http://192.168.33.10"

device_id = "f9d2a7c2-701a-11e5-9c3e-34363bd464b2"
class AuthenticationToken:
    def __init__(self):
        self.token = ""
        self.platform_id = PLATFORM_ANDROID
        self.device_id = device_id


class VOIPControl:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.cmd = 0
        self.dial_count = 1

class VOIPData:
    def __init__(self):
        self.sender = 0
        self.receiver = 0
        self.content = ""

def send_message(cmd, seq, msg, sock):
    if cmd == MSG_AUTH:
        h = struct.pack("!iibbbb", 8, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!q", msg.uid)
        sock.sendall(h + b)
    elif cmd == MSG_VOIP_CONTROL:
        if msg.cmd == VOIP_COMMAND_DIAL:
            length = 24
        else:
            length = 20
            
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qqi", msg.sender, msg.receiver, msg.cmd)
        t = ""
        if msg.cmd == VOIP_COMMAND_DIAL:
            t = struct.pack("!i", msg.dial_count)
        sock.sendall(h+b+t)
    elif cmd == MSG_VOIP_DATA:
        length = 16 + len(msg.content)
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        b = struct.pack("!qq", msg.sender, msg.receiver)
        sock.sendall(h+b+msg.content)
    elif cmd == MSG_AUTH_TOKEN:
        b = struct.pack("!BB", msg.platform_id, len(msg.token)) + msg.token + struct.pack("!B", len(msg.device_id)) + msg.device_id
        length = len(b)
        h = struct.pack("!iibbbb", length, seq, cmd, PROTOCOL_VERSION, 0, 0)
        sock.sendall(h+b)
    else:
        print "eeeeee:", cmd

def recv_message(sock):
    buf = sock.recv(12)
    if len(buf) != 12:
        return 0, 0, None
    length, seq, cmd = struct.unpack("!iib", buf[:9])
    content = sock.recv(length)
    if len(content) != length:
        return 0, 0, None

    if cmd == MSG_AUTH_STATUS:
        print len(content), "...."
        status, = struct.unpack("!i", content)
        return cmd, seq, status
    elif cmd == MSG_VOIP_CONTROL:
        ctl = VOIPControl()
        ctl.sender, ctl.receiver, ctl.cmd = struct.unpack("!qqi", content[:20])
        if ctl.cmd == VOIP_COMMAND_DIAL:
            ctl.dial_count = struct.unpack("!i", content[20:24])
        return cmd, seq, ctl
    elif cmd == MSG_VOIP_DATA:
        d = VOIPData()
        d.sender, d.receiver = struct.unpack("!qq", content[:16])
        d.content = content[16:]
        return cmd, seq, d
    else:
        return cmd, seq, content


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

def send_control(sock, seq, sender, receiver, cmd):
    ctl = VOIPControl()
    ctl.sender = sender
    ctl.receiver = receiver
    ctl.cmd = cmd
    send_message(MSG_VOIP_CONTROL, seq, ctl, sock)
    
def send_dial(sock, seq, sender, receiver):
    send_control(sock, seq, sender, receiver, VOIP_COMMAND_DIAL)
    
def send_accept(sock, seq, sender, receiver):
    send_control(sock, seq, sender, receiver, VOIP_COMMAND_ACCEPT)
    
def send_refuse(sock, seq, sender, receiver):
    send_control(sock, seq, sender, receiver, VOIP_COMMAND_REFUSE)
    
def send_refused(sock, seq, sender, receiver):
    send_control(sock, seq, sender, receiver, VOIP_COMMAND_REFUSED)

def send_connected(sock, seq, sender, receiver):
    send_control(sock, seq, sender, receiver, VOIP_COMMAND_CONNECTED)
    
def simultaneous_dial():
    caller = 86013800000000
    called = 86013800000009

    sock, seq = connect_server(caller, PORT)
    seq = seq + 1
    send_dial(sock, seq, caller, called)

    cmd, _, msg = recv_message(sock)
    if cmd != MSG_VOIP_CONTROL:
        return
    if msg.cmd == VOIP_COMMAND_ACCEPT:
        seq = seq + 1
        send_connected(sock, seq, caller, called)
        print "voip connected"
    elif msg.cmd == VOIP_COMMAND_DIAL:
        seq = seq + 1
        send_accept(sock, seq, caller, called)
        cmd, _, msg = recv_message(sock)
        if cmd != MSG_VOIP_CONTROL:
            return

        if msg.cmd == VOIP_COMMAND_CONNECTED:
            print "voip connected"
        elif msg.cmd == VOIP_COMMAND_ACCEPT:
            print "voip connected"
        else:
            return
    elif msg.cmd == VOIP_COMMAND_REFUSE:
        print "dial refused"
        seq = seq + 1
        send_refused(sock, seq, caller, called)
        return
    else:
        print "unknow:", msg.content
        return

    while True:
        cmd, _, msg = recv_message(sock)
        if cmd == MSG_VOIP_CONTROL:
            print "recv voip control:", msg.cmd
            if msg.cmd == VOIP_COMMAND_HANG_UP:
                print "peer hang up"
                break
        else:
            print "unknow command:", cmd
        
def listen():
    caller = 0
    called = 86013800000009
    sock, seq = connect_server(called, PORT)
    while True:
        print "recv..."
        cmd, _, msg = recv_message(sock)
        print cmd, msg
        if cmd != MSG_VOIP_CONTROL:
            continue
        if msg.cmd != VOIP_COMMAND_DIAL:
            continue
        caller = msg.sender
        break

    is_accept = query_yes_no("accept incoming dial")
    if is_accept:
        seq = seq + 1
        send_accept(sock, seq, called, caller)
        while True:
            cmd, _, msg = recv_message(sock)
            if cmd != MSG_VOIP_CONTROL:
                continue

            if msg.cmd != VOIP_COMMAND_CONNECTED:
                continue
            else:
                print "voip control:", msg.cmd
            print "voip connected caller:%d called:%d"%(caller, called)
            break
    else:
        seq = seq + 1
        send_refuse(sock, seq, called, caller)
        return

    address = ('0.0.0.0', 20001)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
    b = struct.pack("!qq", called, caller)
    b += "\x00\x00"
    s.sendto(b, (HOST, 20001))

    while True:
        rs, _, _ = select.select([s, sock], [], [])
        if s in rs:
            data, addr = s.recvfrom(64*1024)
            sender, receiver = struct.unpack("!qq", data[:16])
            print "sender:", sender, "receiver:", receiver, " size:", len(data[16:])
        if sock in rs:
            cmd, _, msg = recv_message(sock)
            if cmd == MSG_VOIP_CONTROL:
                print "recv voip control:", msg.cmd
                if msg.cmd == VOIP_COMMAND_HANG_UP:
                    print "peer hang up"
                    break
            elif cmd == 0:
                print "voip control socket closed"
                break
            else:
                print "unknow command:", cmd

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is one of "yes" or "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "usage:voip_client.py call/wait"
        sys.exit(0)
    if sys.argv[1] == "call":
        simultaneous_dial()
    elif sys.argv[1] == "wait":
        listen()
    else:
        print "usage:voip_client.py call/wait"
