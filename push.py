# -*- coding: utf-8 -*-
import time
import logging
import sys
import redis
import json
import traceback
import requests
import base64
import config

rds = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)

def get_user_platform(rds, uid):
    key = "users_%d"%uid
    return rds.hget(key, "platform")

def ios_push(uid):
    try:
        data = {
            "alert":config.CONTENT
        }

        basic = base64.b64encode(str(config.IOS_APP_ID) + ":" + config.IOS_APP_SECRET)
        headers = {'Content-Type': 'application/json; charset=UTF-8',
                   'Authorization': 'Basic ' + basic}
        resp = requests.post(config.URL + '/push/ios/sandbox/p2p/%s'%uid, 
                             data=json.dumps(data), headers=headers, timeout=60)
        if resp.status_code != 200:
            logging.debug("push ios p2p message fail:%d", resp.status_code)
    except Exception:
        logging.debug("push ios p2p message exception")
        print_exception_traceback()

def android_push(uid):
    try:
        data = {
            "push_type":1,
            "title":config.NAME,
            "content":config.CONTENT
        }

        basic = base64.b64encode(str(config.ANDROID_APP_ID) + ":" + config.ANDROID_APP_SECRET)
        headers = {'Content-Type': 'application/json; charset=UTF-8',
                   'Authorization': 'Basic ' + basic}
        resp = requests.post(config.URL + '/push/android/p2p/%s'%uid, 
                             data=json.dumps(data), headers=headers, timeout=60)
        if resp.status_code != 200:
            logging.debug("push android p2p message fail:%d", resp.status_code)
    except Exception:
        logging.debug("push android p2p message exception")
        print_exception_traceback()

def receive_offline_message():
    while True:
        item = rds.blpop("push_queue")
        if not item:
            continue
        _, msg = item
        obj = json.loads(msg)
        platform = get_user_platform(rds, obj['receiver'])
        if platform == "ios":
            ios_push(obj['receiver'])
        elif platform == "android":
            android_push(obj['receiver'])
        else:
            logging.info("invalid platform:%s", platform)
            continue

def main():
    while True:
        try:
            receive_offline_message()
        except Exception, e:
            print_exception_traceback()
            time.sleep(1)
            continue

def print_exception_traceback():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    logging.warn("exception traceback:%s", traceback.format_exc())

def init_logger(logger):
    root = logger
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(filename)s:%(lineno)d -  %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

if __name__ == "__main__":
    init_logger(logging.getLogger(''))
    main()
