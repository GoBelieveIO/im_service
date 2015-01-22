# -*- coding: utf-8 -*-

from flask import request
from flask import Flask
from flask import g
from functools import wraps
import flask
import random
import md5
import json
import logging
import sys
import os
import redis
import config

app = Flask(__name__)
app.debug = True

rds = redis.StrictRedis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)

def INVALID_PARAM():
    e = {"error":"非法输入"}
    return make_response(400, e)

def  FORBIDDEN():
    e = {"error":"forbidden"}
    return make_response(403, e)

def make_response(status_code, data = None):
    if data:
        res = flask.make_response(json.dumps(data), status_code)
        res.headers['Content-Type'] = "application/json"
    else:
        res = flask.make_response("", status_code)

    return res

def token_key(token):
    return "tokens_" + token

def user_key(appid, uid):
    return "users_%s_%s"%(str(appid), str(uid))

def get_token(rds, appid, uid):
    key = user_key(appid, uid)
    token = rds.get(key)
    return token

def set_token(rds, appid, uid, token):
    key = user_key(appid, uid)
    rds.set(key, token)

def save_token(rds, token, app_id, uid, user_name):
    key = token_key(token)
    mapping = {
        "app_id":app_id, 
        "uid":uid, 
        "user_name":user_name
    }
    rds.hmset(key, mapping)

def basic_authorization(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        header = request.headers.get('Authorization')
        if header:
            header = header.split()
            if len(header) > 1 and header[0] == 'Basic':
                basic = header[1].decode("base64")
                client_id, client_secret = basic.split(':')
                client_id = int(client_id)
                g.app_id = client_id
                return f(*args, **kwargs)
        return FORBIDDEN()

    return decorated_function


@app.route("/auth/token", methods=["POST"])
@basic_authorization
def access_token():
    if not request.data:
        return INVALID_PARAM()

    obj = json.loads(request.data)
    uid = obj["uid"] if obj.has_key("uid") else None
    user_name = obj["user_name"] if obj.has_key("user_name") else ""
    if not uid:
        return INVALID_PARAM()

    token = get_token(rds, g.app_id, uid)
    if not token:
        token = random_token_generator()
        save_token(rds, token, g.app_id, uid, user_name)
        set_token(rds, g.app_id, uid, token)

    resp = { "token":token }
    return make_response(200, resp)


UNICODE_ASCII_CHARACTER_SET = ('abcdefghijklmnopqrstuvwxyz'
                               'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                               '0123456789')

def random_token_generator(length=30, chars=UNICODE_ASCII_CHARACTER_SET):
    rand = random.SystemRandom()
    return ''.join(rand.choice(chars) for x in range(length))


def init_logger(logger):
    root = logger
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(filename)s:%(lineno)d -  %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)    

log = logging.getLogger('')
init_logger(log)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8888)
