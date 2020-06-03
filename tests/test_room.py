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
import sys
from protocol import *
from client import *

task = 0


def recv_room_client(uid, port, room_id, handler):
    sock, seq =  connect_server(uid, port)

    seq += 1
    send_message(MSG_ENTER_ROOM, seq, room_id, sock)

    while True:
        cmd, s, _, msg = recv_message(sock)
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
    print("recv room message success")


def send_room_message_client(uid, room_id):
    global task
    sock, seq =  connect_server(uid, 23000)

    seq += 1
    send_message(MSG_ENTER_ROOM, seq, room_id, sock)
 
    im = RTMessage()
    im.sender = uid
    im.receiver = room_id
    im.content = "test room im"

    for _ in range(1000):
        seq += 1
        send_message(MSG_ROOM_IM, seq, im, sock)

    task += 1
    print("send success")

    

    
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
    
    while task < 1:
        time.sleep(1)

def TestRoomMessage():
    _TestRoomMessage(23000)
    print("test room message completed")

def TestClusterRoomMessage():
    _TestRoomMessage(24000)
    print("test cluster room message completed")


    
def main():
    cluster = False
     
   
    print("test room message")
    TestRoomMessage()
    time.sleep(1)
     
    if cluster:
        TestClusterRoomMessage()
     
    


if __name__ == "__main__":
    main()

