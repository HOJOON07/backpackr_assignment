CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
    event_time_utc      TIMESTAMP,
    event_time_kst      TIMESTAMP,
    event_type          STRING,
    product_id          BIGINT,
    category_id         BIGINT,
    category_code       STRING,
    brand               STRING,
    price               DOUBLE,
    user_id             BIGINT,
    source_user_session STRING,
    session_id          STRING,
    session_start_utc   TIMESTAMP
)
PARTITIONED BY (event_date_kst DATE)
STORED AS PARQUET
LOCATION '${output_path}'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
