CREATE EXTERNAL TABLE `stedi.accelerometer_landing`(
  `timestamp` bigint COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-human-balance-2026-ej01/accelerometer_landing/'
TBLPROPERTIES (
  'last_modified_by'='hadoop', 
  'last_modified_time'='1793647040', 
  'transient_lastDdlTime'='1793647040')