CREATE DATABASE IF NOT EXISTS gobelieve DEFAULT CHARACTER SET utf8;
use gobelieve;


CREATE TABLE `group` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `appid` bigint(20) DEFAULT NULL,
  `master` bigint(20) DEFAULT NULL,
  `super` tinyint(4) NOT NULL DEFAULT '0',
  `name` varchar(255) DEFAULT NULL,
  `notice` varchar(255) DEFAULT NULL COMMENT '公告',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `group_member` (
  `group_id` bigint(20) NOT NULL DEFAULT '0',
  `uid` bigint(20) NOT NULL DEFAULT '0',
  `nickname` varchar(255) DEFAULT NULL COMMENT '群内昵称',
  PRIMARY KEY (`group_id`,`uid`),
  KEY `idx_group_member_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


SHOW TABLES;

