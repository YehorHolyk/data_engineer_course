CREATE TABLE IF NOT EXISTS `de-07-yehor-holyk.silver.customers`
(
  client_id INT64,
  first_name STRING,
  last_name STRING,
  email STRING,
  registration_date DATE,
  state STRING,
  _id STRING,
  _logical_dt TIMESTAMP,
  _job_start_dt TIMESTAMP
)
PARTITION BY registration_date;