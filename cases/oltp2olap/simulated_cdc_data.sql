-- Unified Random Stream for Order and Order Details CDC Events
-- Both events share the same order_number for proper correlation
CREATE DATABASE IF NOT EXISTS retailer_etl_data;

CREATE RANDOM STREAM retailer_etl_data.order_with_details_cdc_source (
    -- ============================================
    -- SHARED ORDER NUMBER (used by both events)
    -- ============================================
    order_number int32 DEFAULT 136000 + (rand(0) % 10000),
    
    -- ============================================
    -- ORDER FIELDS
    -- ============================================
    customer_number int32 DEFAULT array_element(
        [125,169,206,223,237,247,273,293,303,307,335,348,356,361,369,409,443,459,465,477,480,481,124,129,161,321,450,487,112,205,219,239,347,475,173,204,320,339,379,495,157,198,286,362,363,462,151,168,181,233,424,455,456,131,175,202,260,319,328,447,486,146,172,250,350,353,406,103,119,141,171,209,242,256,145,227,249,278,314,381,382,386,452,473,186,187,201,240,311,324,334,489,121,128,144,167,189,259,299,415,448,114,276,282,333,471,166,323,357,412,496,148,177,211,385,398,216,298,344,376,458,484],
        (rand(1) % 122) + 1
    ),
    
    order_date int32 DEFAULT 20000 + (rand(2) % 500),
    required_date int32 DEFAULT order_date + 2 + (rand(3) % 6),
    
    shipped_date_value int32 DEFAULT multi_if(
        (rand(4) % 100) <= 80, order_date + (rand(5) % 4),
        0
    ),
    shipped_date int32 DEFAULT multi_if(shipped_date_value = 0, 0, shipped_date_value),
    
    status string DEFAULT multi_if(
        shipped_date = 0, 'Pending',
        shipped_date = order_date, 'Shipped',
        shipped_date > order_date, 'Processing',
        'Shipped'
    ),
    
    comments string DEFAULT multi_if(
        (rand(6) % 100) <= 90, '',
        (rand(6) % 100) <= 95, 'Urgent delivery',
        'Customer requested gift wrapping'
    ),
    
    -- ============================================
    -- ORDER DETAILS FIELDS
    -- ============================================
    product_code string DEFAULT array_element(
        ['S10_1949','S10_4757','S10_4962','S12_1099','S12_1108','S12_3148','S12_3380','S12_3891','S12_3990','S12_4675','S18_1129','S18_1589','S18_1889','S18_1984','S18_2238','S18_2870','S18_3232','S18_3233','S18_3278','S18_3482','S18_3685','S18_4027','S18_4721','S18_4933','S24_1046','S24_1444','S24_1628','S24_2766','S24_2840','S24_2887','S24_2972','S24_3191','S24_3371','S24_3432','S24_3856','S24_4048','S24_4620','S700_2824','S10_1678','S10_2016','S10_4698','S12_2823','S18_2625','S18_3782','S24_1578','S24_2000','S24_2360','S32_1374','S32_2206','S32_4485','S50_4713','S18_1662','S18_2581','S24_1785','S24_2841','S24_3949','S24_4278','S700_1691','S700_2466','S700_2834','S700_3167','S700_4002','S72_1253','S18_3029','S24_2011','S700_1138','S700_1938','S700_2047','S700_2610','S700_3505','S700_3962','S72_3212','S18_3259','S32_3207','S50_1514','S12_1666','S12_4473','S18_1097','S18_2319','S18_2432','S18_4600','S24_2300','S32_1268','S32_2509','S32_3522','S50_1392','S18_1342','S18_1367','S18_1749','S18_2248','S18_2325','S18_2795','S18_2949','S18_2957','S18_3136','S18_3140','S18_3320','S18_3856','S18_4409','S18_4522','S18_4668','S24_1937','S24_2022','S24_3151','S24_3420','S24_3816','S24_3969','S24_4258','S32_4289','S50_1341'],
        (rand(11) % 110) + 1
    ),
    
    quantity_ordered int32 DEFAULT multi_if(
        (rand(12) % 100) <= 60, 5 + (rand(13) % 20),
        (rand(12) % 100) <= 85, 25 + (rand(13) % 25),
        50 + (rand(13) % 50)
    ),
    
    price_each float64 DEFAULT round(exp(rand_normal(4.0, 0.8)), 2),
    order_line_number int32 DEFAULT 1 + (rand(14) % 10),
    
    -- ============================================
    -- SHARED TIMESTAMPS
    -- ============================================
    ts_ns int64 DEFAULT to_int64(now64(9)),
    ts_us int64 DEFAULT to_int64(ts_ns / 1000),
    ts_ms int64 DEFAULT to_int64(ts_ns / 1000000),
    
    -- ============================================
    -- CDC EVENT 1: ORDER
    -- ============================================
    order_cdc_event string DEFAULT concat(
        '{"before":null,"after":{',
        '"orderNumber":', to_string(order_number), ',',
        '"orderDate":', to_string(order_date), ',',
        '"requiredDate":', to_string(required_date), ',',
        '"shippedDate":', multi_if(shipped_date IS NULL, 'null', to_string(shipped_date)), ',',
        '"status":"', status, '",',
        '"comments":"', comments, '",',
        '"customerNumber":', to_string(customer_number),
        '},"source":{},"transaction":null,"op":"c",',
        '"ts_ms":', to_string(ts_ms), ',',
        '"ts_us":', to_string(ts_us), ',',
        '"ts_ns":', to_string(ts_ns),
        '}'
    ),
    
    -- ============================================
    -- CDC EVENT 2: ORDER DETAILS
    -- ============================================
    order_details_cdc_event string DEFAULT concat(
        '{"before":null,"after":{',
        '"orderNumber":', to_string(order_number), ',',
        '"productCode":"', product_code, '",',
        '"quantityOrdered":', to_string(quantity_ordered), ',',
        '"priceEach":', to_string(price_each), ',',
        '"orderLineNumber":', to_string(order_line_number),
        '},"source":{},"transaction":null,"op":"c",',
        '"ts_ms":', to_string(ts_ms), ',',
        '"ts_us":', to_string(ts_us), ',',
        '"ts_ns":', to_string(ts_ns),
        '}'
    )
) SETTINGS eps = 1;

CREATE STREAM IF NOT EXISTS retailer_etl_data.order_with_details_cdc
(
  `order_cdc_event` string,
  `order_details_cdc_event` string
)
TTL to_datetime(_tp_time) + INTERVAL 24 HOUR
SETTINGS logstore_retention_bytes = '107374182', logstore_retention_ms = '300000';


CREATE MATERIALIZED VIEW IF NOT EXISTS retailer_etl_data.mv_order_with_details_cdc INTO retailer_etl_data.order_with_details_cdc AS
SELECT
  order_cdc_event,
  order_details_cdc_event
FROM
  retailer_etl_data.order_with_details_cdc_source;



CREATE STREAM IF NOT EXISTS retailer_etl_data.orders (
    `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'demo.cdc.mysql.retailer.orders', data_format='RawBLOB', one_message_per_row=true;

CREATE MATERIALIZED VIEW IF NOT EXISTS retailer_etl_data.mv_orders INTO retailer_etl_data.orders AS
SELECT
  order_cdc_event as raw
FROM
  retailer_etl_data.order_with_details_cdc

CREATE STREAM IF NOT EXISTS retailer_etl_data.orderdetails (
    `raw` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'demo.cdc.mysql.retailer.orderdetails', data_format='RawBLOB', one_message_per_row=true;


CREATE MATERIALIZED VIEW IF NOT EXISTS retailer_etl_data.mv_ordersdetails INTO retailer_etl_data.orderdetails AS
SELECT
  order_details_cdc_event as raw
FROM
  retailer_etl_data.order_with_details_cdc;


-- test mysql

CREATE EXTERNAL TABLE
    retailer_etl_data.products
SETTINGS
    type='mysql',
    address='35.247.93.97:3306',
    database='retailer',
    table='products',
    user='admin',
    password='Password!'

