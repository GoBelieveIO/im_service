#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import struct
import socket
import threading
import time
import requests
import json
import uuid
import base64
import hashlib
import sys
import datetime
from protocol import *
from client import *
import random
import rpc


GROUP_ID = 3110
SUPER_GROUP_ID = 3111
GROUP_MEMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

task = 0
def send_client(uid, receiver, msg_type):
    global task
    sock, seq =  connect_server(uid, 23000)
    im = IMMessage()
    im.sender = uid
    im.receiver = receiver
    if msg_type == MSG_IM:
        im.content = json.dumps({"uuid":str(uuid.uuid1()), "text":"test im " + str(datetime.datetime.now())})
    else:
        im.content = json.dumps({"uuid":str(uuid.uuid1()), "text":"test group im " + str(datetime.datetime.now())})
    seq += 1
    send_message(msg_type, seq, im, sock)
    msg_seq = seq
    while True:
        cmd, s, flag, msg = recv_message(sock)
        if cmd == MSG_ACK and msg == msg_seq:
            break
        else:
            pass
        
    sock.close()    
    task += 1
    print("send success")



    

def send_http_group_message(uid, receiver):
    global task
    content = json.dumps({"uuid":str(uuid.uuid1()), "text":"test"})
    res = rpc.post_group_message(APP_ID, uid, receiver, content)
    if res.status_code != 200:
        print(res.status_code, res.content)
        return
    print("send http group message:", res.status_code)
    task += 1

def recv_group_message_client(uid, port=23000, group_id = 0):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_GROUP_IM:
            if group_id == 0 or msg.receiver == group_id:
                return True
            else:
                return False
        elif cmd == MSG_IM:
            return False
        else:
            return False
    recv_group_client(uid, group_id, port, handle_message)
    task += 1
    print("recv group message success")

def notification_recv_client(uid, port=23000):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_GROUP_NOTIFICATION:
            notification = json.loads(msg)
            if 'create' in notification:
                return True
            else:
                return False
        else:
            return False
    recv_client(uid, port, handle_message)
    task += 1
    print("recv notification success")


def TestSendHttpGroupMessage():
    global task
    task = 0

    group_id = GROUP_ID

    receiver = GROUP_MEMBERS[0]
    sender = GROUP_MEMBERS[1]
    
    t3 = threading.Thread(target=recv_group_message_client, args=(receiver,23000))
    t3.setDaemon(True)
    t3.start()


    time.sleep(1)

    t2 = threading.Thread(target=send_http_group_message, args=(sender, group_id))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)

    print("test send http group message completed")

def TestSuperGroupOffline():
    _TestGroupOffline(SUPER_GROUP_ID, True)
    print("test super group offline message completed")

def TestGroupOffline():
    _TestGroupOffline(GROUP_ID, False)

    print("test group offline message completed")

def _TestGroupOffline(group_id, is_super):
  
    global task
    task = 0

    receiver = GROUP_MEMBERS[0]
    sender = GROUP_MEMBERS[1]
    
    t2 = threading.Thread(target=send_client, args=(sender, group_id, MSG_GROUP_IM))
    t2.setDaemon(True)
    t2.start()
    
    time.sleep(1)

    t3 = threading.Thread(target=recv_group_message_client, args=(receiver, 23000, group_id))
    t3.setDaemon(True)
    t3.start()

    while task < 2:
        time.sleep(1)


    
def _TestGroupMessage(group_id, is_super, port):
    global task
    task = 0

    receiver = GROUP_MEMBERS[0]
    sender = GROUP_MEMBERS[1]
    
    t3 = threading.Thread(target=recv_group_message_client, args=(receiver,port))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)


    t2 = threading.Thread(target=send_client, args=(sender, group_id, MSG_GROUP_IM))
    t2.setDaemon(True)
    t2.start()


    while task < 2:
        time.sleep(1)



def TestSuperGroupMessage():
    _TestGroupMessage(SUPER_GROUP_ID, True, 23000)
    print("test super group message completed")

def TestGroupMessage():
    _TestGroupMessage(GROUP_ID, False, 23000)
    print("test group message completed")

def TestClusterSuperGroupMessage():
    _TestGroupMessage(SUPER_GROUP_ID, True, 24000)
    print("test cluster super group message completed")

def TestClusterGroupMessage():
    _TestGroupMessage(GROUP_ID, False, 24000)
    print("test cluster group message completed")

def _TestGroupNotification(group_id):
    global task
    task = 0
    receiver = GROUP_MEMBERS[0]
    
    t3 = threading.Thread(target=notification_recv_client, args=(receiver,))
    t3.setDaemon(True)
    t3.start()
    time.sleep(1)

    v = {
        "group_id":group_id, 
        "master":GROUP_MEMBERS[0], 
        "name":"test", 
        "members":GROUP_MEMBERS,
        "timestamp":int(time.time())
    }
    op = {"create":v}
    
    rpc.post_group_notification(APP_ID, group_id, op, GROUP_MEMBERS)

    while task < 1:
        time.sleep(1)


def TestGroupNotification():
    _TestGroupNotification(GROUP_ID)
    print("test group notification completed")

def TestSuperGroupNotification():
    _TestGroupNotification(SUPER_GROUP_ID)
    print("test super group notification completed")

    
def main():
    cluster = True
     
     
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
     

if __name__ == "__main__":
    main()

