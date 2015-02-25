
## 接口规范
### 接口支持4种HTTP方法

- **GET** 获取对象
- **PUT** 替换和更新对象
- **POST** 创建新的对象
- **DELETE** 删除对象

### HTTP返回码

- 200 成功
- 其他 失败

### HTTP 接口返回值结构

    {
        "meta": 
        {
            "code": "业务码",
            "message": "状态信息"
        },
        "data": ...
    }

*若无特殊说明以下接口响应结果指的是此结构的data字段的值。*


### 第三方服务端授权
- 请求地址: 所有可供第三方服务端访问的接口
- 请求头部: Authorization: Basic xxx  //xxx 为 "<client_id>:<client_secret>"字符串的base64编码（编码后的字符串不能含有\n）


### 客户端授权
-请求头部: Authorization: Bearer xxx  //xxx 为 access token

### 创建群组
- 请求地址：**POST /groups**
- 是否认证：服务端授权和客户端授权
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
- 是否认证：服务端授权和客户端授权
- 成功响应：200
- 操作失败：
  400 非法的群id

### 添加群组成员
- 请求地址：**POST /groups/{gid}/members**
- 是否认证：服务端授权和客户端授权
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
- 是否认证：服务端授权和客户端授权
- 成功响应：200
- 操作失败：
  400 非法参数


### 第三方应用获取永久有效的access token
- 请求地址：** POST /auth/grant**
- 是否认证：服务端授权
- 请求内容:

        {
            "uid":"用户id（整型）"
            "user_name":"用户名"
        }
        
- 成功响应：200

        {
            "token":"访问token"
        }
    
- 操作失败：
  400 非法参数


### 绑定用户id和推送token
- 请求地址：**POST /device/bind **
- 是否认证：客户端授权
- 请求内容：

        {
            "apns_device_token": "IOS设备token，16进制字符串(可选)",
            "ng_device_token": "android设备token，16进制字符串(可选)",
        }

- 成功响应 200

- 操作失败:
状态码:400

### 解除用户id和推送token之间的绑定
- 请求地址：**POST /device/unbind **
- 是否认证：客户端授权
- 请求内容：

        {
            "apns_device_token": "IOS设备token，16进制字符串(可选)",
            "ng_device_token": "android设备token，16进制字符串(可选)",
        }

- 成功响应 200

- 操作失败:
状态码:400
