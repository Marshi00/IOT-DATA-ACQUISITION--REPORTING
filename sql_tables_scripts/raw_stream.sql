CREATE TABLE `raw_stream` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tag_name` varchar(45) NOT NULL,
  `min` decimal(21,15) DEFAULT NULL,
  `max` decimal(21,15) DEFAULT NULL,
  `total` decimal(21,15) DEFAULT NULL,
  `time` decimal(16,10) DEFAULT NULL,
  `min_minute` int DEFAULT NULL,
  `max_minute` int DEFAULT NULL,
  `valid` tinyint DEFAULT NULL,
  `datetime_obj` datetime NOT NULL,
  `datetime_str` varchar(45) NOT NULL,
  `read_num` varchar(45) NOT NULL,
  `status` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25661 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Raw incoming data without any change '