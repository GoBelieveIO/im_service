#token的格式

    连接im服务器token存储在redis的hash对象中,脱离API服务器测试时，可以手工生成。
    $token就是客户端需要获得的, 用来连接im服务器的认证信息。
    key:access_token_$token
    field:
        user_id:用户id
        app_id:应用id



#群组通知内容格式:

1. 创建群:
v = {
    "group_id":群组id, 
    "master":群组管理员, 
    "name":群组名称, 
    "members":成员列表(long数组),
    "timestamp":时间戳(秒)
}
op = {"create":v}


2. 解散群:
v = {
    "group_id":群组id,
    "timestamp":时间戳
}

op = {"disband":v}


3. 更新群组名称:
v = {
   "group_id":群组id,
   "timestamp":时间戳,
   "name":群组名
}
op = {"update_name":v}

4. 添加群组成员:
v = {
    "group_id":群组id(long类型),
    "member_id":新的成员id,
    "timestamp":时间戳
}
op = {"add_member":v}


5. 离开群:
v = {
    "group_id":群组id,
    "member_id":离开的成员id,
    "timestamp":时间戳
}
op = {"quit_group":v}


6. 普通群升级为超级群:
v = {
    "group_id":群组id,
    "timestamp":时间戳,
    "super":1
}

#群组结构更新广播内容格式:

1. 新建群:
channel名:group_create
内容格式:"群组id(整型),appid(整型),超级群(1/0)"

2. 解散群:
channel名:group_disband
内容格式:"群组id"

3. 新增成员:
channel名:group_member_add
内容格式:"群组id,成员id"

4. 删除成员:
channel名:group_member_remove
内容格式:"群组id,成员id"

5. 普通群升级为超级群:
channel名:group_upgrade
内容格式:"群组id(整型),appid(整型),1"

