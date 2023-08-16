CREATE TABLE IF NOT EXISTS `de-07-yehor-holyk.silver.sales`
(
  client_id INT64,
  purchase_date DATE,
  product_name STRING,
  price INT64,
  _id STRING,
  _logical_dt TIMESTAMP,
  _job_start_dt TIMESTAMP
)
PARTITION BY purchase_date;