CREATE DATABASE IF NOT EXISTS im DEFAULT CHARACTER SET utf8;
use im;

CREATE TABLE IF NOT EXISTS `group`(
	   id BIGINT AUTO_INCREMENT PRIMARY KEY,
           appid  BIGINT,
	   master BIGINT,
	   name VARCHAR(255));

CREATE TABLE IF NOT EXISTS group_member(group_id BIGINT, uid BIGINT, PRIMARY KEY(group_id, uid));


--
-- Table structure for table `_object`
--

DROP TABLE IF EXISTS `_object`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `_object` (
  `id` bigint(13) unsigned NOT NULL AUTO_INCREMENT,
  `type` tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=11247 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `account`
--

DROP TABLE IF EXISTS `account`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `account` (
  `id` bigint(15) unsigned NOT NULL COMMENT '全局id',
  `name` varchar(64) NOT NULL COMMENT '姓名',
  `email` varchar(128) CHARACTER SET ascii NOT NULL COMMENT '账号（空值为僵尸账号）',
  `email_removed` varchar(128) CHARACTER SET ascii NOT NULL DEFAULT '' COMMENT '删除邮箱账号',
  `email_checked` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '邮箱是否已经校验',
  `password` varchar(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '密码',
  `ctime` int(10) unsigned NOT NULL COMMENT '注册时间',
  `role` tinyint(1) unsigned NOT NULL DEFAULT '1' COMMENT '1：开发者，2：渠道，3：平台管理员',
  `mobile_zone` varchar(5) CHARACTER SET ascii NOT NULL COMMENT '区号',
  `mobile` varchar(18) CHARACTER SET ascii NOT NULL COMMENT '手机号码',
  PRIMARY KEY (`id`),
  UNIQUE KEY `email_unique` (`email`),
  KEY `email` (`email`),
  KEY `role` (`role`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='账号';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `account_developer`
--

DROP TABLE IF EXISTS `account_developer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `account_developer` (
  `account_id` bigint(15) unsigned NOT NULL COMMENT '账号',
  `developer_id` bigint(15) unsigned NOT NULL COMMENT '开发商',
  `developer_type` tinyint(1) unsigned NOT NULL DEFAULT '1' COMMENT '1：个人，2：企业',
  `permission_mask` int(10) unsigned NOT NULL COMMENT '权限掩码',
  `is_creator` tinyint(1) unsigned NOT NULL COMMENT '是否创建者',
  PRIMARY KEY (`account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='账号所属开发商';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `app`
--

DROP TABLE IF EXISTS `app`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app` (
  `id` bigint(15) unsigned NOT NULL COMMENT '全局id',
  `name` varchar(256) NOT NULL COMMENT '名称',
  `developer_id` bigint(15) unsigned NOT NULL COMMENT '开发商',
  `ctime` int(10) unsigned NOT NULL COMMENT '创建时间',
  `type` tinyint(1) unsigned NOT NULL DEFAULT '1' COMMENT '1：public，2：confidential',
  `major_category_id` tinyint(3) unsigned NOT NULL COMMENT '主分类',
  `minor_category_id` tinyint(3) unsigned NOT NULL COMMENT '次分类',
  `intro` text NOT NULL COMMENT '简介',
  `icon_src` varchar(64) CHARACTER SET ascii NOT NULL COMMENT '图标',
  `pub_key` varchar(512) NOT NULL COMMENT '开发商公钥',
  `app_type` tinyint(1) unsigned NOT NULL DEFAULT '1' COMMENT '应用类型, 1:游戏, 2:应用',
  PRIMARY KEY (`id`),
  KEY `developer_id` (`developer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='应用';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `app_category`
--

DROP TABLE IF EXISTS `app_category`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_category` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `pid` int(10) unsigned NOT NULL COMMENT '父级id',
  `name` varchar(128) NOT NULL COMMENT '名称',
  `ord` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '排序值',
  PRIMARY KEY (`id`),
  KEY `pid` (`pid`)
) ENGINE=MyISAM AUTO_INCREMENT=135 DEFAULT CHARSET=utf8 COMMENT='应用分类';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `client`
--

DROP TABLE IF EXISTS `client`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `client` (
  `id` bigint(15) unsigned NOT NULL COMMENT '全局id',
  `app_id` bigint(15) unsigned NOT NULL COMMENT '应用',
  `developer_id` bigint(15) unsigned NOT NULL COMMENT '开发商',
  `platform_type` tinyint(1) unsigned NOT NULL COMMENT '1：android，2：ios',
  `platform_identity` varchar(128) CHARACTER SET ascii NOT NULL COMMENT '客户端唯一标示，android为pakcage name；ios为bundle id',
  `secret` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '秘钥',
  `package_src` varchar(64) NOT NULL COMMENT '安装包地址',
  `length` int(10) unsigned NOT NULL COMMENT '包大小',
  `version_num` int(10) unsigned NOT NULL COMMENT '内部版本号，数字',
  `version_name` varchar(32) NOT NULL COMMENT '显示版本号',
  `intro` text NOT NULL COMMENT '介绍',
  `ctime` int(10) unsigned NOT NULL COMMENT '创建时间',
  `utime` int(10) unsigned NOT NULL COMMENT '更新时间',
  `verify_status` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '0：未审核，1：审核通过，2：审核不通过',
  `is_active` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '是否激活',
  `verify_time` int(10) unsigned NOT NULL COMMENT '审核时间',
  `verify_remark` varchar(512) NOT NULL DEFAULT '' COMMENT '审核备注',
  PRIMARY KEY (`id`),
  KEY `app_id` (`app_id`),
  KEY `developer_id` (`developer_id`),
  KEY `verify_status` (`verify_status`),
  KEY `is_active` (`is_active`),
  KEY `ctime` (`ctime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='客户端';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `client_apns`
--

DROP TABLE IF EXISTS `client_apns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `client_apns` (
  `client_id` bigint(15) unsigned NOT NULL COMMENT '客户端',
  `sandbox_key` blob NOT NULL COMMENT '沙盒模式的证书',
  `sandbox_key_secret` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '沙盒模式的证书秘钥',
  `production_key` blob NOT NULL COMMENT '生产模式的证书',
  `production_key_secret` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '生产模式的证书秘钥',
  `sandbox_key_utime` int(10) unsigned NOT NULL COMMENT '沙盒证书修改时间，0为未上传',
  `production_key_utime` int(10) unsigned NOT NULL COMMENT '生产证书修改时间，0为未上传',
  PRIMARY KEY (`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='iOS客户端apns证书';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `client_certificate`
--

DROP TABLE IF EXISTS `client_certificate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `client_certificate` (
  `client_id` bigint(15) unsigned NOT NULL COMMENT '客户端',
  `pkey` text NOT NULL COMMENT '私钥',
  `cer` text CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '证书',
  `update_time` int(10) unsigned NOT NULL COMMENT '修改时间',
  PRIMARY KEY (`client_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='android证书';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `developer_company`
--

DROP TABLE IF EXISTS `developer_company`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `developer_company` (
  `id` bigint(15) unsigned NOT NULL COMMENT '全局id',
  `name` varchar(256) NOT NULL COMMENT '名称',
  `license_number` varchar(64) CHARACTER SET ascii NOT NULL COMMENT '营业执照注册号',
  `code` varchar(64) CHARACTER SET ascii NOT NULL COMMENT '组织机构代码',
  `country` char(2) CHARACTER SET ascii NOT NULL COMMENT '国家编号',
  `province_id` int(10) unsigned NOT NULL COMMENT '省编号',
  `city_id` int(10) unsigned NOT NULL COMMENT '市编号',
  `street` varchar(256) NOT NULL COMMENT '公司地址',
  `contact_fullname` varchar(64) NOT NULL COMMENT '联系人姓名',
  `contact_mobile` varchar(16) CHARACTER SET ascii NOT NULL COMMENT '联系人手机',
  `contact_email` varchar(128) CHARACTER SET ascii NOT NULL COMMENT '联系人email',
  `ctime` int(10) unsigned NOT NULL COMMENT '创建时间',
  `verify_status` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '0：未审核，1：审核通过，2：审核不通过',
  `utime` int(10) unsigned NOT NULL COMMENT '修改时间',
  `verify_time` int(10) unsigned NOT NULL COMMENT '审核时间',
  `verify_remark` varchar(512) NOT NULL DEFAULT '' COMMENT '审核备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='企业开发商';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `developer_person`
--

DROP TABLE IF EXISTS `developer_person`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `developer_person` (
  `id` bigint(15) unsigned NOT NULL COMMENT '全局id',
  `full_name` varchar(64) NOT NULL COMMENT '姓名',
  `pin_type` tinyint(1) unsigned NOT NULL COMMENT '1：身份证，2：护照',
  `pin` varchar(32) CHARACTER SET ascii NOT NULL COMMENT '证件号码',
  `country` char(2) CHARACTER SET ascii NOT NULL COMMENT '国家编号',
  `province_id` int(10) unsigned NOT NULL COMMENT '省编号',
  `city_id` int(10) unsigned NOT NULL COMMENT '市编号',
  `street` varchar(256) NOT NULL COMMENT '街道地址',
  `mobile` varchar(16) CHARACTER SET ascii NOT NULL COMMENT '手机号',
  `ctime` int(10) unsigned NOT NULL COMMENT '创建时间',
  `verify_status` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '0：未审核，1：审核通过，2：审核不通过',
  `utime` int(10) unsigned NOT NULL COMMENT '修改时间',
  `verify_time` int(10) unsigned NOT NULL COMMENT '审核时间',
  `verify_remark` varchar(512) NOT NULL DEFAULT '' COMMENT '审核备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='个人账号';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `package`
--

DROP TABLE IF EXISTS `package`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `package` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `filename` varchar(64) NOT NULL COMMENT '文件名',
  `name` varchar(128) NOT NULL COMMENT '应用名称',
  `src` varchar(64) NOT NULL COMMENT '地址',
  `platform_type` tinyint(1) unsigned NOT NULL COMMENT '1：android，2：ios',
  `platform_identity` varchar(64) CHARACTER SET ascii NOT NULL COMMENT '客户端唯一标示，android为pakcage name；ios为bundle id',
  `version_num` int(10) unsigned NOT NULL COMMENT '内部版本号，数字',
  `version_name` varchar(32) NOT NULL COMMENT '显示版本号',
  `min_os_version` varchar(32) NOT NULL COMMENT '固件最小适配版本',
  `length` int(10) unsigned NOT NULL COMMENT '包大小',
  `ctime` int(10) unsigned NOT NULL COMMENT '上传时间',
  `meta` blob COMMENT '元数据',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=191 DEFAULT CHARSET=utf8 COMMENT='安装包';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `photo`
--

DROP TABLE IF EXISTS `photo`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `photo` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `filename` varchar(128) NOT NULL COMMENT '文件名',
  `src` varchar(64) CHARACTER SET ascii NOT NULL COMMENT '地址',
  `format` varchar(16) CHARACTER SET ascii NOT NULL COMMENT '图片格式',
  `width` int(10) unsigned NOT NULL COMMENT '宽',
  `height` int(10) unsigned NOT NULL COMMENT '高',
  `ctime` int(10) unsigned NOT NULL COMMENT '上传时间',
  `meta` blob COMMENT '元数据',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=18924 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='图片';
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `verify_email`
--

DROP TABLE IF EXISTS `verify_email`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `verify_email` (
  `email` varchar(128) CHARACTER SET ascii NOT NULL COMMENT '邮件',
  `usage_type` tinyint(3) unsigned NOT NULL COMMENT '1：开发者邮箱验证',
  `code` varchar(128) CHARACTER SET ascii NOT NULL COMMENT '验证码',
  `ctime` int(10) unsigned NOT NULL COMMENT '创建时间',
  `ro_id` bigint(15) unsigned NOT NULL COMMENT '邮箱所有者',
  PRIMARY KEY (`usage_type`,`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT COMMENT='邮箱验证';
/*!40101 SET character_set_client = @saved_cs_client */;



SHOW TABLES;

