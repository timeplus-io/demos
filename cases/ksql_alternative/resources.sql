CREATE DATABASE IF NOT EXISTS ksql_alternative;

CREATE MUTABLE STREAM ksql_alternative.order_events_table
(
  `version` int32,
  `id` string,
  `createdAt` string,
  `lastUpdatedAt` string,
  `deliveredAt` string,
  `completedAt` string,
  `customer` tuple(version int32, id string, firstName string, lastName string, gender string, companyName nullable(string), email string, customerType string, revision int32),
  `orderValue` int32,
  `lineItems.articleId` array(string),
  `lineItems.name` array(string),
  `lineItems.quantity` array(int32),
  `lineItems.quantityUnit` array(string),
  `lineItems.unitPrice` array(int32),
  `lineItems.totalPrice` array(int32),
  `payment` map(string, string),
  `deliveryAddress` tuple(version int32, id string, customer tuple(id string, type string), type string, firstName string, lastName string, state string, street string, houseNumber string, city string, zip string, latitude float64, longitude float64, phone string, additionalAddressInfo string, createdAt string, revision int32),
  `revision` int32,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
PRIMARY KEY id;

-- external streams
CREATE STREAM ksql_alternative.bookings
(
  `action` string,
  `bid` string,
  `booking_time` string,
  `cid` string,
  `expire` string,
  `time` string,
  `uid` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092 ', topic = 'bookings';

CREATE STREAM ksql_alternative.car_live_data
(
  `cid` string,
  `gas_percent` float64,
  `in_use` bool,
  `latitude` float64,
  `longitude` float64,
  `locked` bool,
  `speed_kmh` float64,
  `time` string,
  `total_km` float64
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092 ', topic = 'car_live_data';

CREATE STREAM ksql_alternative.frontend_events
(
  `version` int32,
  `requestedUrl` string,
  `method` string,
  `correlationId` string,
  `ipAddress` string,
  `requestDuration` int32,
  `response` map(string, int32),
  `headers` map(string, string)
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092 2', topic = 'owlshop-frontend-events';

CREATE STREAM ksql_alternative.order_events
(
  `version` int32,
  `id` string,
  `createdAt` string,
  `lastUpdatedAt` string,
  `deliveredAt` string,
  `completedAt` string,
  `customer` tuple(version int32, id string, firstName string, lastName string, gender string, companyName nullable(string), email string, customerType string, revision int32),
  `orderValue` int32,
  `lineItems.articleId` array(string),
  `lineItems.name` array(string),
  `lineItems.quantity` array(int32),
  `lineItems.quantityUnit` array(string),
  `lineItems.unitPrice` array(int32),
  `lineItems.totalPrice` array(int32),
  `payment` map(string, string),
  `deliveryAddress` tuple(version int32, id string, customer tuple(id string, type string), type string, firstName string, lastName string, state string, street string, houseNumber string, city string, zip string, latitude float64, longitude float64, phone string, additionalAddressInfo string, createdAt string, revision int32),
  `revision` int32
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = '10.138.0.23:9092', topic = 'owlshop-orders';

-- MV

CREATE MATERIALIZED VIEW ksql_alternative.mv_method_count_3s
AS
SELECT
  window_start AS ts, window_end AS te, method, count(*) AS count
FROM
  tumble(ksql_alternative.frontend_events, 3s)
GROUP BY
  window_start, window_end, method;

CREATE MATERIALIZED VIEW ksql_alternative.mv_order_events_table INTO ksql_alternative.order_events_table
AS
SELECT
  *
FROM
  ksql_alternative.order_events;