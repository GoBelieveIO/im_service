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
    print "recv group message success"

def notification_recv_client(uid, port=23000):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_GROUP_NOTIFICATION:
            notification = json.loads(msg)
            if notification.has_key("create"):
                return True
            else:
                return False
        else:
            return False
    recv_client(uid, port, handle_message)
    task += 1
    print "recv notification success"

    
def TestGroup():
    access_token = login(13635273142)
    url = URL + "/client/groups"

    group = {"master":13635273142,"members":[13635273142], "name":"test", "super":True}
    headers = {}
    headers["Authorization"] = "Bearer " + access_token
    headers["Content-Type"] = "application/json; charset=UTF-8"

    r = requests.post(url, data=json.dumps(group), headers = headers)
    assert(r.status_code == 200)
    obj = json.loads(r.content)
    group_id = obj["data"]["group_id"]


    url = URL + "/client/groups/%s"%str(group_id)
    r = requests.patch(url, data=json.dumps({"name":"test_new"}), headers = headers)
    assert(r.status_code == 200)

    url = URL + "/client/groups/%s/members"%str(group_id)
    r = requests.post(url, data=json.dumps({"uid":13635273143}), headers = headers)
    assert(r.status_code == 200)


    url = URL + "/client/groups/%s/members"%str(group_id)
    r = requests.post(url, data=json.dumps([13635273144,13635273145]), headers = headers)
    assert(r.status_code == 200)


    url = URL + "/client/groups/%s/members"%str(group_id)
    data = json.dumps([{"uid":13635273143}])
    r = requests.delete(url, data=data, headers = headers)
    print r.content
    assert(r.status_code == 200)


    secret = md5.new(APP_SECRET).digest().encode("hex")
    basic = base64.b64encode(str(APP_ID) + ":" + secret)
    app_headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
    
    
    url = URL + "/client/groups/%s"%str(group_id)
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
    url = URL + "/client/groups"
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

    url = URL + "/client/groups/%s"%str(group_id)
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

    url = URL + "/client/groups"

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

    t3 = threading.Thread(target=recv_group_message_client, args=(13635273143, 23000, group_id))
    t3.setDaemon(True)
    t3.start()

    while task < 2:
        time.sleep(1)


    url = URL + "/client/groups/%s"%str(group_id)
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
    url = URL + "/client/groups"
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

    url = URL + "/client/groups/%s"%str(group_id)
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

    url = URL + "/client/groups"

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

    url = URL + "/client/groups/%s"%str(group_id)
    r = requests.delete(url, headers=headers)
    print r.status_code, r.text



def TestGroupNotification():
    _TestGroupNotification(False)
    print "test group notification completed"  

def TestSuperGroupNotification():
    _TestGroupNotification(True)
    print "test super group notification completed"  


    
def main():
    cluster = False
     
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
     

if __name__ == "__main__":
    main()

