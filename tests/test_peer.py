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
import sys
from protocol import *
from client import *

task = 0

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
            pass
        
    sock.close()    
    task += 1
    print "send success"



def recv_room_client(uid, port, room_id, handler):
    sock, seq =  connect_server(uid, port)

    seq += 1
    send_message(MSG_ENTER_ROOM, seq, room_id, sock)

    while True:
        cmd, s, msg = recv_message(sock)
        seq += 1
        send_message(MSG_ACK, seq, s, sock)
        if handler(cmd, s, msg):
            break


def recv_room_message_client(uid, room_id, port=23000):
    global task    
    def handle_message(cmd, s, msg):
        if cmd == MSG_ROOM_IM:
            return True
        else:
            return False

    recv_room_client(uid, port, room_id, handle_message)
    task += 1
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

    print "send rt...."
    send_message(MSG_RT, seq, im, sock)
    task += 1
    print "send success"


    
def recv_message_client(uid, port=23000):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_IM:
            return True
        else:
            return False

    recv_client(uid, port, handle_message)
    task += 1
    print "recv message success"

    
def recv_rt_message_client(uid, port=23000):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_RT:
            return True
        else:
            return False

    recv_client(uid, port, handle_message)
    task += 1
    print "recv rt message success"



    
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



def send_notificaton(uid):
    global task
    url = URL + "/messages/notifications"
    obj = {
        "receiver":uid,
        "content":"notification content"
    }
    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
     
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return
    print "send notification:", res.status_code
    task += 1
    
def recv_notification_client(uid):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_NOTIFICATION:
            print "cmd:", cmd, msg
            return True
        else:
            print "cmd:", cmd, msg
            return False

    recv_client(uid, 23000, handle_message)
    task += 1
    print "recv notification success"

    

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
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_SYSTEM:
            print "cmd:", cmd, msg
            return True
        else:
            print "cmd:", cmd, msg
            return False

    recv_client(uid, 23000, handle_message)
    task += 1
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
    
def TestNotification():
    global task
    task = 0
 
    room_id = 1
    t3 = threading.Thread(target=recv_notification_client, args=(13635273142, ))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_notificaton, args=(13635273142, ))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

    print "test notification completed"

    
def main():
    cluster = False
     
    TestRTSendAndRecv()
    time.sleep(1)

    print "test room message"
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
    
    TestNotification()
    time.sleep(1)

    TestSystemMessage()
    time.sleep(1)

    TestPingPong()
    time.sleep(1)
    TestTimeout()
    time.sleep(1)
    


if __name__ == "__main__":
    main()

