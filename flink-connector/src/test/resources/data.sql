CREATE TABLE `user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(64) NOT NULL COMMENT '用户姓名',
  `age` int(10) NOT NULL COMMENT '年龄',
  `point_value` int(11) NOT NULL DEFAULT '0' COMMENT '积分',
  `status` smallint(6) NOT NULL DEFAULT '0' COMMENT '记录可用状态',
  `createTime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建日期',
  PRIMARY KEY (`id`)
);

INSERT INTO `user`(id, user_name, age, point_value, status) VALUE (1, '小王', 25, 3, 1);
INSERT INTO `user`(id, user_name, age, point_value, status) VALUE (3, '小张', 27, 18, 0);
INSERT INTO `user`(id, user_name, age, point_value, status) VALUE (2, '小李', 23, 10, 0);