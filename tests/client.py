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
import ssl
import random
import redis
import config
from protocol import *

KEFU_APP_ID = config.KEFU_APP_ID
APP_ID = config.APP_ID
HOST = config.HOST
SSL = config.SSL



rds = redis.StrictRedis(host=config.REDIS_HOST, password=config.REDIS_PASSWORD,
                        port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)


UNICODE_ASCII_CHARACTER_SET = ('abcdefghijklmnopqrstuvwxyz'
                               'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                               '0123456789')

def random_token_generator(length=30, chars=UNICODE_ASCII_CHARACTER_SET):
    rand = random.SystemRandom()
    return ''.join(rand.choice(chars) for x in range(length))

def create_access_token():
    return random_token_generator()


class User(object):
    @staticmethod
    def get_user_access_token(rds, appid, uid):
        key = "users_%d_%d"%(appid, uid)
        token = rds.hget(key, "access_token")
        return token

    @staticmethod
    def load_user_access_token(rds, token):
        key = "access_token_%s"%token
        exists = rds.exists(key)
        if not exists:
            return 0, 0, ""
        uid, appid, name = rds.hget(key, "user_id", "app_id", "user_name")
        return uid, appid, name


    @staticmethod
    def save_user(rds, appid, uid, name, avatar, token):
        key = "users_%d_%d"%(appid, uid)
        obj = {
            "access_token":token,
            "name":name,
            "avatar":avatar
        }
        rds.hmset(key, obj)
        
    @staticmethod
    def save_token(rds, appid, uid, token):
        key = "access_token_%s"%token
        obj = {
            "user_id":uid,
            "app_id":appid
        }
        rds.hmset(key, obj)

    @staticmethod
    def save_user_access_token(rds, appid, uid, name, token):
        pipe = rds.pipeline()

        key = "access_token_%s"%token
        obj = {
            "user_id":uid,
            "user_name":name,
            "app_id":appid
        }
        
        pipe.hmset(key, obj)

        key = "users_%d_%d"%(appid, uid)
        obj = {
            "access_token":token,
            "name":name
        }

        pipe.hmset(key, obj)
        pipe.execute()

        return True        
        
def _login(appid, uid):
    token = User.get_user_access_token(rds, appid, uid)
    if not token:
        token = create_access_token()
        User.save_user_access_token(rds, appid, uid, '', token)
    return token


def _connect_server(token, port):
    seq = 0
    if SSL:
        address = (HOST, 24430)
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = ssl.wrap_socket(sock_fd)
    else:
        address = (HOST, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
    print("connect address:", address)
    sock.connect(address)
    auth = AuthToken()
    auth.token = token
    seq = seq + 1
    send_message(MSG_AUTH, seq, auth, sock)
    cmd, _, _, msg = recv_message(sock)
    if cmd != MSG_AUTH_STATUS or msg != 0:
        raise Exception("auth failure:" + token)
    return sock, seq



def recv_client_(uid, sock, seq, handler, group_id=None):
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
        cmd, s, flag, msg = recv_message(sock)
        print("cmd:", cmd, "msg:", msg)
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

        elif not (flag & MESSAGE_FLAG_PUSH) and handler(cmd, s, msg):
            quit = True
            if not begin:
                break


    sock.close()



def connect_server(uid, port, appid=None):
    if appid is None:
        token = _login(APP_ID, uid)
    else:
        token = _login(appid, uid)
    if not token:
        raise Exception("login failure")
    return _connect_server(token, port)

def kefu_connect_server(uid, port):
    return connect_server(uid, port, KEFU_APP_ID)    

def kefu_recv_client(uid, port, handler):
    sock, seq = kefu_connect_server(uid, port)    
    recv_client_(uid, sock, seq, handler)    

def recv_group_client(uid, group_id, port, handler):
    sock, seq = connect_server(uid, port)
    recv_client_(uid, sock, seq, handler, group_id=group_id)

def recv_client(uid, port, handler, appid=None):
    sock, seq = connect_server(uid, port, appid)
    recv_client_(uid, sock, seq, handler)
    
