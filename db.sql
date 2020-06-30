CREATE DATABASE IF NOT EXISTS gobelieve DEFAULT CHARACTER SET utf8;
use gobelieve;


CREATE TABLE `group` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `appid` bigint(20) DEFAULT NULL,
  `master` bigint(20) DEFAULT NULL,
  `super` tinyint(4) NOT NULL DEFAULT '0',
  `name` varchar(255) DEFAULT NULL,
  `notice` varchar(255) DEFAULT NULL COMMENT '公告',
  `deleted` tinyint(1) NOT NULL COMMENT '删除标志',  
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `group_member` (
  `group_id` bigint(20) NOT NULL DEFAULT '0',
  `uid` bigint(20) NOT NULL DEFAULT '0',
  `timestamp` int(11) DEFAULT NULL COMMENT '入群时间,单位：秒',
  `nickname` varchar(255) DEFAULT NULL COMMENT '群内昵称',
  `mute` tinyint(1) DEFAULT '0' COMMENT '群内禁言',
  `deleted` tinyint(1) NOT NULL COMMENT '删除标志',
  PRIMARY KEY (`group_id`,`uid`),
  KEY `idx_group_member_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `friend` (
  `appid` bigint(20) NOT NULL,
  `uid` bigint(20) NOT NULL,
  `friend_uid` bigint(20) NOT NULL,
  `timestamp` int(11) NOT NULL COMMENT '创建时间',
  PRIMARY KEY (`appid`, `uid`,`friend_uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='好友关系';


CREATE TABLE `blacklist` (
  `appid` bigint(20) NOT NULL,
  `uid` bigint(20) NOT NULL,
  `friend_uid` bigint(20) NOT NULL,
  `timestamp` int(11) NOT NULL COMMENT '创建时间',
  PRIMARY KEY (`appid`, `uid`,`friend_uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='黑名单';


SHOW TABLES;

