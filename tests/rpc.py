# -*- coding: utf-8 -*-
import config
import logging
import json
import requests

im_url = "http://127.0.0.1:6666"


def post_peer_message(appid, sender, receiver, content):
    params = {
        "appid":appid,
        "receiver":receiver,
        "sender":sender
    }

    url = im_url + "/post_peer_message"
    res = requests.post(url, data=content.encode("utf-8"), params=params)
    return res


def post_group_message(appid, sender, receiver, content):
    params = {
        "appid":appid,
        "sender":sender,        
        "receiver":receiver,
    }
    url = im_url + "/post_group_message"
    res = requests.post(url, data=content.encode("utf-8"), params=params)
    return res
    

def post_group_notification_s(appid, gid, notification, members):
    url = im_url + "/post_group_notification"

    obj = {
        "appid": appid,
        "group_id": gid,
        "notification":notification
    }
    if members:
        obj["members"] = members

    headers = {"Content-Type":"application/json"}

    data = json.dumps(obj)
    resp = requests.post(url, data=data, headers=headers)
    if resp.status_code != 200:
        logging.warning("send group notification error:%s", resp.content)
    else:
        logging.debug("send group notification success:%s", data)
    return resp


def post_group_notification(appid, gid, op, members):
    try:
        return post_group_notification_s(appid, gid, json.dumps(op), members)
    except Exception as e:
        logging.warning("send group notification err:%s", e)
        return None


def send_group_notification(appid, gid, op, members):
    return post_group_notification(appid, gid, op, members)


def post_peer_notification(appid, uid, content):
    params = {
        "appid":appid,
        "uid":uid
    }    
    url = im_url + "/post_notification"

    headers = {"Content-Type":"text/plain; charset=UTF-8"}
    resp = requests.post(url, data=content.encode("utf8"), headers=headers, params=params)
    return resp


def post_system_message(appid, uid, content):
    params = {
        "appid":appid,
        "uid":uid
    }
    url = im_url + "/post_system_message"

    headers = {"Content-Type":"text/plain; charset=UTF-8"}
    resp = requests.post(url, data=content.encode("utf8"), headers=headers, params=params)    
    return resp


def post_room_message(appid, uid, room_id, content):
    params = {
        "appid":appid,
        "uid":uid,
        "room":room_id
    }
    url = im_url + "/post_room_message"
    headers = {"Content-Type":"text/plain; charset=UTF-8"}
    resp = requests.post(url, data=content.encode("utf8"), headers=headers, params=params)
    return resp

