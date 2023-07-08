CREATE TABLE `fact_stream` (
  `id` int NOT NULL AUTO_INCREMENT,
  `tag_id` int NOT NULL,
  `date_key` datetime NOT NULL,
  `min` decimal(21,15) DEFAULT NULL,
  `max` decimal(21,15) DEFAULT NULL,
  `total` decimal(21,15) DEFAULT NULL,
  `time` decimal(16,10) DEFAULT NULL,
  `min_minute` int DEFAULT NULL,
  `max_minute` int DEFAULT NULL,
  `valid` tinyint DEFAULT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY (`tag_id`) REFERENCES `dim_tag` (`tag_id`),
  FOREIGN KEY (`date_key`) REFERENCES `dimdate` (`DateTime`)
  CONSTRAINT `uq_tag_name_date_key` UNIQUE (`tag_id`, `date_key`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
