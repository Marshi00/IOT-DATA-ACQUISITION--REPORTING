CREATE TABLE `last_read_info` (
  `id` int NOT NULL AUTO_INCREMENT,
  `last_read_num` int NOT NULL,
  `date_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idnew_table_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='info of reads and their num + time '