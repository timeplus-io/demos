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

INSERT INTO demo.http_status_codes (code, status, rfc) VALUES
(100, 'Continue', '[RFC2616]'),
(101, 'Switching Protocols', '[RFC2616]'),
(102, 'Processing', '[RFC2518]'),
(200, 'OK', '[RFC2616]'),
(201, 'Created', '[RFC2616]'),
(202, 'Accepted', '[RFC2616]'),
(203, 'Non-Authoritative Information', '[RFC2616]'),
(204, 'No Content', '[RFC2616]'),
(205, 'Reset Content', '[RFC2616]'),
(206, 'Partial Content', '[RFC2616]'),
(207, 'Multi-Status', '[RFC4918]'),
(208, 'Already Reported', '[RFC5842]'),
(226, 'IM Used', '[RFC3229]'),
(300, 'Multiple Choices', '[RFC2616]'),
(301, 'Moved Permanently', '[RFC2616]'),
(302, 'Found', '[RFC2616]'),
(303, 'See Other', '[RFC2616]'),
(304, 'Not Modified', '[RFC2616]'),
(305, 'Use Proxy', '[RFC2616]'),
(306, 'Reserved', '[RFC2616]'),
(307, 'Temporary Redirect', '[RFC2616]'),
(308, 'Permanent Redirect', '[RFC-reschke-http-status-308-07]'),
(400, 'Bad Request', '[RFC2616]'),
(401, 'Unauthorized', '[RFC2616]'),
(402, 'Payment Required', '[RFC2616]'),
(403, 'Forbidden', '[RFC2616]'),
(404, 'Not Found', '[RFC2616]'),
(405, 'Method Not Allowed', '[RFC2616]'),
(406, 'Not Acceptable', '[RFC2616]'),
(407, 'Proxy Authentication Required', '[RFC2616]'),
(408, 'Request Timeout', '[RFC2616]'),
(409, 'Conflict', '[RFC2616]'),
(410, 'Gone', '[RFC2616]'),
(411, 'Length Required', '[RFC2616]'),
(412, 'Precondition Failed', '[RFC2616]'),
(413, 'Request Entity Too Large', '[RFC2616]'),
(414, 'Request-URI Too Long', '[RFC2616]'),
(415, 'Unsupported Media Type', '[RFC2616]'),
(416, 'Requested Range Not Satisfiable', '[RFC2616]'),
(417, 'Expectation Failed', '[RFC2616]'),
(422, 'Unprocessable Entity', '[RFC4918]'),
(423, 'Locked', '[RFC4918]'),
(424, 'Failed Dependency', '[RFC4918]'),
(425, 'Unassigned', ''),
(426, 'Upgrade Required', '[RFC2817]'),
(427, 'Unassigned', ''),
(428, 'Precondition Required', '[RFC6585]'),
(429, 'Too Many Requests', '[RFC6585]'),
(430, 'Unassigned', ''),
(431, 'Request Header Fields Too Large', '[RFC6585]'),
(500, 'Internal Server Error', '[RFC2616]'),
(501, 'Not Implemented', '[RFC2616]'),
(502, 'Bad Gateway', '[RFC2616]'),
(503, 'Service Unavailable', '[RFC2616]'),
(504, 'Gateway Timeout', '[RFC2616]'),
(505, 'HTTP Version Not Supported', '[RFC2616]'),
(506, 'Variant Also Negotiates (Experimental)', '[RFC2295]'),
(507, 'Insufficient Storage', '[RFC4918]'),
(508, 'Loop Detected', '[RFC5842]'),
(509, 'Unassigned', ''),
(510, 'Not Extended', '[RFC2774]'),
(511, 'Network Authentication Required', '[RFC6585]');
