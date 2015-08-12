CREATE EXTERNAL TABLE presto_test_s3 (
  t_bigint BIGINT,
  t_string STRING
)
COMMENT 'Presto test S3 table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('RETENTION'='-1')
;

ALTER TABLE presto_test_s3
SET LOCATION 's3://presto-test-hive/presto_test_s3'
;
