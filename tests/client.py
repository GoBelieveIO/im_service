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
import ssl
from protocol import *

KEFU_APP_ID = 1453
KEFU_APP_KEY = "xQrfaJPgfc5DsWuNUKcn4DMSWJUR4fcr"
KEFU_APP_SECRET = "ozj9rROFg3GmiqSa8kRBagNubf52BHlz"

APP_ID = 7
APP_KEY = "sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44"
APP_SECRET = '0WiCxAU1jh76SbgaaFC7qIaBPm2zkyM1'
HOST = "127.0.0.1"
URL = "http://dev.api.gobelieve.io"

SSL = True

def _login(appid, app_secret, uid):
    url = URL + "/auth/grant"
    obj = {"uid":uid, "user_name":str(uid)}
    secret = md5.new(app_secret).digest().encode("hex")
    basic = base64.b64encode(str(appid) + ":" + secret)
    headers = {'Content-Type': 'application/json; charset=UTF-8',
               'Authorization': 'Basic ' + basic}
     
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return None
    obj = json.loads(res.text)
    return obj["data"]["token"]

def login(uid):
    return _login(APP_ID, APP_SECRET, uid)

def kefu_login(uid):
    return _login(KEFU_APP_ID, KEFU_APP_SECRET, uid)

def _connect_server(token, port):
    seq = 0
    if SSL:
        address = (HOST, 24430)
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = ssl.wrap_socket(sock_fd)
    else:
        address = (HOST, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
    print "connect address:", address
    sock.connect(address)
    auth = AuthenticationToken()
    auth.token = token
    seq = seq + 1
    send_message(MSG_AUTH_TOKEN, seq, auth, sock)
    cmd, _, msg = recv_message(sock)
    if cmd != MSG_AUTH_STATUS or msg != 0:
        return None, 0
    return sock, seq


def connect_server(uid, port):
    token = login(uid)
    if not token:
        return None, 0    
    return _connect_server(token, port)

def kefu_connect_server(uid, port):
    token = kefu_login(uid)
    if not token:
        return None, 0    
    return _connect_server(token, port)


def recv_client_(uid, port, handler, group_id=None, is_kefu=False):
    if is_kefu:
        sock, seq = kefu_connect_server(uid, port)
    else:
        sock, seq = connect_server(uid, port)

    group_sync_keys = {}
    sync_key = 0

    seq += 1
    send_message(MSG_SYNC, seq, sync_key, sock)
    if group_id:
        group_sync_keys[group_id] = 0
        seq += 1
        send_message(MSG_SYNC_GROUP, seq, (group_id, sync_key), sock)
    quit = False
    begin = False
    while True:
        cmd, s, msg = recv_message(sock)
        print "cmd:", cmd, "msg:", msg
        if cmd == MSG_SYNC_BEGIN:
            begin = True
        elif cmd == MSG_SYNC_END:
            begin = False
            new_sync_key = msg
            if new_sync_key > sync_key:
                sync_key = new_sync_key
                seq += 1
                send_message(MSG_SYNC_KEY, seq, sync_key, sock)
            if quit:
                break
        elif cmd == MSG_SYNC_NOTIFY:
            new_sync_key = msg
            if new_sync_key > sync_key:
                seq += 1
                send_message(MSG_SYNC, seq, sync_key, sock)
        elif cmd == MSG_SYNC_GROUP_NOTIFY:
            group_id, new_sync_key = msg
            skey = group_sync_keys.get(group_id, 0)
            if new_sync_key > skey:
                seq += 1
                send_message(MSG_SYNC_GROUP, seq, (group_id, skey), sock)
        elif cmd == MSG_SYNC_GROUP_BEGIN:
            begin = True
        elif cmd == MSG_SYNC_GROUP_END:
            begin = False
            group_id, new_sync_key = msg
            skey = group_sync_keys.get(group_id, 0)
            if new_sync_key > skey:
                group_sync_keys[group_id] = new_sync_key
                skey = group_sync_keys.get(group_id, 0)
                seq += 1
                send_message(MSG_GROUP_SYNC_KEY, seq, (group_id, skey), sock)
            if quit:
                break

        elif handler(cmd, s, msg):
            quit = True
            if not begin:
                break


    sock.close()


def kefu_recv_client(uid, port, handler):
    recv_client_(uid, port, handler, is_kefu=True)    

def recv_group_client(uid, group_id, port, handler):
    recv_client_(uid, port, handler, group_id=group_id)

def recv_client(uid, port, handler):
    recv_client_(uid, port, handler)
    
