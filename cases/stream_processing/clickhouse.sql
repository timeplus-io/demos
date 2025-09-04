CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE demo.events
(
    `_tp_time` DateTime64(3),
    `url` String,
    `method` String,
    `ip` String
)
ENGINE = MergeTree
PRIMARY KEY (_tp_time, url)
ORDER BY (_tp_time, url)
SETTINGS index_granularity = 8192;

CREATE TABLE demo.http_status_codes
(
    `code` Int32,
    `status` String,
    `rfc` String
)
ENGINE = MergeTree
PRIMARY KEY code
ORDER BY code
SETTINGS index_granularity = 8192;

CREATE TABLE demo.http_code_count_5s
(
    `ts` DateTime64(3),
    `code` UInt8,
    `status` String,
    `views` UInt32
)
ENGINE = MergeTree
PRIMARY KEY (ts, code)
ORDER BY (ts, code)
SETTINGS index_granularity = 8192;
