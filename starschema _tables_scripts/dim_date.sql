CREATE TABLE `dimdate` (
  `DateTime` datetime DEFAULT NULL,
  `second` bigint DEFAULT NULL,
  `minute` bigint DEFAULT NULL,
  `hour` bigint DEFAULT NULL,
  `day` bigint DEFAULT NULL,
  `dayofweek` bigint DEFAULT NULL,
  `is_weekend` tinyint(1) DEFAULT NULL,
  `month` bigint DEFAULT NULL,
  `Quarter` bigint DEFAULT NULL,
  `Year` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
