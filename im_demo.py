# -*- coding: utf-8 -*-
import struct
import socket
import threading
import time
import requests
import json
import uuid
import base64
import select

from im_client import Client


#调用app自身的登陆接口获取im服务必须的access token
def login(uid):
#    URL = "http://demo.gobelieve.io"
    URL = "http://192.168.33.10"
    url = URL + "/auth/token"
    obj = {"uid":uid, "user_name":str(uid)}
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print res.status_code, res.content
        return None
    obj = json.loads(res.text)
    return obj["token"]

def post_message(token, content):
    #URL = "http://127.0.0.1:23002"
    URL = "http://api.gobelieve.io"
    url = URL + "/messages"
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    headers["Authorization"] = "Bearer " + token
    obj = {"content":content, "receiver":1, "msgid":0}
    res = requests.post(url, data=json.dumps(obj), headers=headers)
    if res.status_code != 200:
        print "send fail:", res.status_code, res.content
        return
    print "send success"
    
#现在只能获得最近接受到得消息，不能获得发出去得消息
def load_latest_message(token):
    #URL = "http://127.0.0.1:23002"
    URL = "http://api.gobelieve.io"
    url = URL + "/messages?limit=10"
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    headers["Authorization"] = "Bearer " + token

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print "send fail:", res.status_code, res.content
        return
    print res.content
    
def send():
    token = login(2)
    if not token:
        return
    post_message(token, "test")
    
def recv():
    token = login(1)
    if not token:
        return
    load_latest_message(token)
    while True:
        client = Client()
        try:
            r = client.connect_server(token)
            if not r:
                print "connect fail"
                time.sleep(1)
                continue

            while True:
                print "recv..."
                m = client.recv_message()
                if not m:
                    print "connection closed"
                    break
                print "im content:", m.content
        except Exception as e:
            print "exception:", e

def main():
    send()
    recv()

if __name__ == "__main__":
    main()
