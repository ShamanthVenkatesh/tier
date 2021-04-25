DROP TABLE IF EXISTS shamanth.weather_data;
CREATE TABLE shamanth.weather_data
(
currently_apparenttemperature DOUBLE,
currently_humidity DOUBLE,
currently_precipintensity DOUBLE,
currently_precipprobability DOUBLE,
currently_preciptype STRING,
currently_temperature DOUBLE,
currently_visibility STRING,
currently_windspeed DOUBLE,
date_time STRING
)
PARTITIONED BY (processed_date DATE, city STRING)
STORED AS PARQUET;

--hdfs://nameservice1/user/hive/warehouse/shamanth.db/weather_data


DROP TABLE IF EXISTS shamanth.track_events;
CREATE TABLE shamanth.track_events
(
anonymous_id STRING,
context_app_build STRING,
context_app_name STRING,
context_app_version STRING,
context_device_id STRING,
context_device_manufacturer STRING,
context_device_model STRING,
context_device_type STRING,
context_os_name STRING,
context_os_version STRING,
context_timezone STRING,
event_name STRING,
original_timestamp_time STRING,
properties_rating STRING,
received_time STRING,
sent_time STRING)
PARTITIONED BY (processed_date DATE, region STRING, market_area STRING)
STORED AS PARQUET;

--hdfs://nameservice1/user/hive/warehouse/shamanth.db/track_events