import requests
import base64
import json
APP_ID = 8
APP_SECRET = 'sVDIlIiDUm7tWPYWhi6kfNbrqui3ez44'
URL = "http://127.0.0.1:8888/auth/token"

obj = {"uid":111, "user_name":"111"}
basic = base64.b64encode(str(APP_ID) + ":" + APP_SECRET)
headers = {'Content-Type': 'application/json; charset=UTF-8',
           'Authorization': 'Basic ' + basic}

res = requests.post(URL, data=json.dumps(obj), headers=headers)
print res.status_code
print res.text
