CREATE DATABASE IF NOT EXISTS geo;

-- note: Run user management SQL with superuser account proton
-- Create the user with a password
CREATE USER IF NOT EXISTS geolookup IDENTIFIED BY 'demo123';

-- Grant privileges on the geo database only
GRANT SELECT ON geo.* TO geolookup;

CREATE MUTABLE STREAM geo.dbip_city_ipv4
(
    `ip_range_start` string,
    `ip_range_end` string,
    `country_code` nullable(string),
    `state1` nullable(string), 
    `state2` nullable(string), 
    `city` nullable(string),
    `postcode` nullable(string),
    `latitude` float64,
    `longitude` float64,
    `timezone` nullable(string)
)
PRIMARY KEY (ip_range_start, ip_range_end);

INSERT INTO geo.dbip_city_ipv4 (ip_range_start, ip_range_end, country_code, state1, state2, city, postcode, latitude, longitude, timezone)
SELECT 
    to_ipv4(ip_range_start), 
    to_ipv4(ip_range_end), 
    country_code, 
    state1, 
    state2, 
    city, 
    postcode, 
    latitude, 
    longitude, 
    timezone 
FROM url('https://tp-solutions.s3.us-west-2.amazonaws.com/ip-location-db/dbip-city-ipv4.csv.gz', 'CSV', 'ip_range_start ipv4, ip_range_end ipv4, country_code nullable(string), state1 nullable(string), state2 nullable(string), city nullable(string), postcode nullable(string), latitude float64, longitude float64, timezone nullable(string)')


CREATE VIEW geo.v_dbip_city_ipv4_with_cidr
AS
WITH 
    ip_range_start, 
    ip_range_end, 
    bit_xor(to_ipv4(ip_range_start), to_ipv4(ip_range_end)) AS xor, 
    bin(xor) AS xor_binary, 
    if(xor != 0, ceil(log2(xor)), 0) AS unmatched, 
    32 - unmatched AS cidr_suffix, 
    cast(bit_and(bit_not(pow(2, unmatched) - 1), to_ipv4(ip_range_start)), 'uint32') AS bitand, 
    to_ipv4(ipv4_num_to_string(bitand)) AS cidr_address
SELECT
  concat(to_string(cidr_address), '/', to_string(cidr_suffix)) AS cidr, 
  to_ipv4(ip_range_start), 
  to_ipv4(ip_range_end), 
  latitude, 
  longitude, 
  country_code, 
  state1, 
  city
FROM
  table(geo.dbip_city_ipv4);

-- stream for geo lookups
CREATE MUTABLE STREAM geo.geoip_lookup
(
  `cidr` string,
  `latitude` float64,
  `longitude` float64,
  `country_code` string,
  `state` string,
  `city` string
)
PRIMARY KEY cidr;

INSERT INTO geo.geoip_lookup (cidr, latitude, longitude, country_code, state, city) 
SELECT
  cidr, latitude, longitude, coalesce(country_code, '') AS country_code, coalesce(state1, '') AS state, coalesce(city, '') AS  city
FROM
  geo.v_dbip_city_ipv4_with_cidr;

CREATE DICTIONARY geo.ip_trie
(
  `cidr` string,
  `latitude` float64,
  `longitude` float64,
  `country_code` string,
  `state` string,
  `city` string
)
PRIMARY KEY cidr
SOURCE(TIMEPLUS(STREAM 'geoip_lookup' USER 'geolookup' PASSWORD 'demo123' ))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(IP_TRIE);

-- lookup github.com IP
-- it will take sometime to load the dictionary for cold start
SELECT dict_get('geo.ip_trie', ('country_code', 'latitude', 'longitude', 'city'), to_ipv4('140.82.112.4')); 
