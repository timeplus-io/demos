
CREATE DATABASE IF NOT EXISTS retailer_etl;

-- source from external streams
CREATE STREAM retailer_etl.topic_orderdetails
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'demo.cdc.mysql.retailer.orderdetails';

CREATE STREAM retailer_etl.topic_orders
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'demo.cdc.mysql.retailer.orders';

CREATE STREAM retailer_etl.topic_products
(
  `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'demo.cdc.mysql.retailer.products';


-- stream for products table in retailer database
CREATE MUTABLE STREAM retailer_etl.dim_products
(
  `productCode` string,
  `productName` string,
  `productLine` string,
  `productVendor` string,
  `productDescription` string,
  `quantityInStock` int16,
  `buyPrice` float32,
  `MSRP` float32
)
PRIMARY KEY productCode;

-- external table
CREATE EXTERNAL TABLE retailer_etl.gcs
(
  `orderNumber` uint32,
  `customerNumber` int16,
  `orderDate` int16,
  `status` string,
  `orderTotal` decimal(10, 2),
  `itemCount` uint64,
  `productCodes` array(string),
  `productNames` array(string),
  `productLines` array(string)
)
SETTINGS type = 's3', 
    endpoint = 'https://storage.googleapis.com/timeplus-demo', 
    access_key_id = 'key', 
    secret_access_key = 'secret', 
    data_format = 'JSONEachRow', 
    write_to = 'retailer_cdc/orders.jsonl', 
    s3_min_upload_file_size = 1024, 
    s3_max_upload_idle_seconds = 60;

-- dictionary for products table in retailer database
CREATE DICTIONARY retailer_etl.dict_products
(
  `productCode` string,
  `productName` string,
  `productLine` string,
  `productVendor` string,
  `productDescription` string,
  `quantityInStock` int16,
  `buyPrice` float32,
  `MSRP` float32
)
PRIMARY KEY productCode
SOURCE(MYSQL(DB 'retailer' TABLE 'products' HOST '35.247.93.97' PORT 3306 USER 'admin' PASSWORD 'password' BG_RECONNECT true))
LAYOUT(MUTABLE_CACHE(DB 'retailer_etl' STREAM 'dim_products' UPDATE_FROM_SOURCE false));

-- MV
CREATE MATERIALIZED VIEW retailer_etl.mv_orders
(
  `_tp_time` datetime64(3, 'UTC'),
  `orderNumber` uint32,
  `orderDate` int16,
  `requiredDate` int16,
  `shippedDate` int16,
  `status` string,
  `comments` string,
  `customerNumber` int16,
  `_tp_sn` int64
) AS
SELECT
  _tp_time, to_uint32_or_zero(raw:after.orderNumber) AS orderNumber, to_int16_or_zero(raw:after.orderDate) AS orderDate, to_int16_or_zero(raw:after.requiredDate) AS requiredDate, to_int16_or_zero(raw:after.shippedDate) AS shippedDate, raw:after.status AS status, raw:after.comments AS comments, cast(raw:after.customerNumber, 'int16') AS customerNumber
FROM
  retailer_etl.topic_orders
SETTINGS
  seek_to = 'earliest'

CREATE MATERIALIZED VIEW retailer_etl.mv_load_products INTO retailer_etl.dim_products
(
  `_tp_time` datetime64(3, 'UTC'),
  `productCode` string,
  `productName` string,
  `productLine` string,
  `productVendor` string,
  `productDescription` string,
  `quantityInStock` int16,
  `buyPrice` float32,
  `MSRP` float32,
  `_tp_sn` int64
) AS
SELECT
  _tp_time, 
  raw:after.productCode AS productCode, 
  raw:after.productName AS productName, raw:after.productLine AS productLine, raw:after.productVendor AS productVendor, raw:after.productDescription AS productDescription, cast(raw:after.quantityInStock, 'int16') AS quantityInStock, cast(raw:after.buyPrice, 'float32') AS buyPrice, cast(raw:after.MSRP, 'float32') AS MSRP
FROM
  retailer_etl.topic_products
SETTINGS
  seek_to = 'earliest';

CREATE MATERIALIZED VIEW retailer_etl.mv_orderdetails
(
  `_tp_time` datetime64(3, 'UTC'),
  `orderNumber` uint32,
  `productCode` string,
  `quantityOrdered` int16,
  `priceEach` float32,
  `orderLineNumber` int16,
  `_tp_sn` int64
) AS
SELECT
  _tp_time, 
  to_uint32_or_zero(raw:after.orderNumber) AS orderNumber, 
  raw:after.productCode AS productCode, 
  cast(raw:after.quantityOrdered, 'int16') AS quantityOrdered, 
  cast(raw:after.priceEach, 'float32') AS priceEach, 
  cast(raw:after.orderLineNumber, 'int16') AS orderLineNumber
FROM
  retailer_etl.topic_orderdetails
SETTINGS
  seek_to = 'earliest';

CREATE MATERIALIZED VIEW retailer_etl.mv_mysql_gcs_pipeline INTO retailer_etl.gcs
(
  `orderNumber` uint32,
  `customerNumber` int16,
  `orderDate` int16,
  `status` string,
  `orderTotal` decimal(10, 2),
  `itemCount` uint64,
  `productCodes` array(string),
  `productNames` array(string),
  `productLines` array(string),
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
  `_tp_sn` int64
) AS(
WITH details AS
  (
    SELECT
      _tp_time, *
    FROM
      retailer_etl.mv_orderdetails
    SETTINGS
      seek_to = 'earliest'
  ), orders AS
  (
    SELECT
      _tp_time, *
    FROM
      retailer_etl.mv_orders
    SETTINGS
      seek_to = 'earliest'
  ), enriched_orderdetails AS
  (
    SELECT
      orders._tp_time AS timestamp, *
    FROM
      details
    INNER JOIN orders ON (details.orderNumber = orders.orderNumber) AND date_diff_within(10s)
    SETTINGS
      join_max_buffered_bytes = 2524288000
  )
SELECT
  orderNumber, 
  any(customerNumber) AS customerNumber, 
  any(orderDate) AS orderDate, 
  any(status) AS status, 
  cast(sum(priceEach * quantityOrdered), 'decimal(10, 2)') AS orderTotal, 
  count() AS itemCount, 
  group_uniq_array(productCode) AS productCodes, 
  group_uniq_array(dict_get('retailer_etl.dict_products', 'productName', productCode)) AS productNames, 
  group_uniq_array(dict_get('retailer_etl.dict_products', 'productLine', productCode)) AS productLines
FROM
  tumble(enriched_orderdetails, timestamp, 30s)
GROUP BY
  window_start, orderNumber)
COMMENT 'Combine details for same order & create JSON in GCS, batch every 30s';
