

### 创建群组
- 请求地址：**POST /groups**
- 是否认证：是
- 请求头:Content-Type:application/json
- 请求内容:


     {
        "master_id":"管理员id(整型)",
        "name":"群主题名",
        "members":["uid",...]
     }


- 成功响应：200

    {
        "group_id":"群组id(整型)"
    }


- 操作失败:
  400 非法的输入参数

### 解散群组
- 请求地址：**DELETE /groups/{gid}**
- 是否认证：是
- 成功响应：200
- 操作失败：
  400 非法的群id

### 添加群组成员
- 请求地址：**POST /groups/{gid}/members**
- 是否认证：是
- 请求头:Content-Type:application/json
- 请求内容:

    {
        "uid":"群成员id"
    }

- 成功响应：200
- 操作失败：
  400 非法的群成员id

### 离开群
- 请求地址：**DELETE /groups/{gid}/members/{mid}**
- 是否认证：是
- 成功响应：200
- 操作失败：
  400 非法参数


### 第三方应用提供的token校验接口
- 请求方法：**GET **
- 请求的url参数：token=?
- 成功响应：200

        {
            "uid":"用户id"
        }
    
- 操作失败：
  400 非法参数
