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

def recv_customer_message_client(uid, port=23000):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_CUSTOMER:
            return True
        else:
            return False

    kefu_recv_client(uid, port, handle_message)
    task += 1
    print "recv customer message success"

    
def send_customer_message(uid, seller_id):
    global task
    sock, seq = connect_server(uid, 23000)

    m = CustomerMessage()
    m.customer_appid = APP_ID
    m.customer_id = uid
    m.store_id = 1
    m.seller_id = seller_id
    m.content = json.dumps({"text":"test"})
    m.persistent = True
    seq += 1
    send_message(MSG_CUSTOMER, seq, m, sock)
    print "send customer message success"
    task += 1

    
def recv_customer_support_message_client(uid, port=23000):
    global task
    def handle_message(cmd, s, msg):
        if cmd == MSG_CUSTOMER_SUPPORT:
            print "mmm:", msg
            return True
        else:
            return False

    recv_client(uid, port, handle_message)
    task += 1
    print "recv customer support message success"
    

def send_customer_support_message(seller_id, customer_id):
    global task
    sock, seq = kefu_connect_server(seller_id, 23000)

    m = CustomerMessage()
    m.customer_appid = APP_ID
    m.customer_id = customer_id
    m.store_id = 1
    m.seller_id = seller_id
    m.content = json.dumps({"text":"test"})
    m.persistent = True
    seq += 1
    send_message(MSG_CUSTOMER_SUPPORT, seq, m, sock)
    print "send customer support message success"
    task += 1

    
def TestCustomerSupportMessage():
    global task
    task = 0

    t3 = threading.Thread(target=recv_customer_support_message_client, args=(1, ))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_customer_support_message, args=(2, 1))
    t2.setDaemon(True)
    t2.start()
     
    while task < 2:
        time.sleep(1)

    print "test customer support message completed"

    
def TestCustomerMessage():
    global task
    task = 0

    t3 = threading.Thread(target=recv_customer_message_client, args=(2, ))
    t3.setDaemon(True)
    t3.start()

    time.sleep(1)
    
    t2 = threading.Thread(target=send_customer_message, args=(1, 2))
    t2.setDaemon(True)
    t2.start()
    
    while task < 2:
        time.sleep(1)

    print "test customer message completed"


def main():
    TestCustomerMessage()
    TestCustomerSupportMessage()
    
if __name__ == "__main__":
    main()

