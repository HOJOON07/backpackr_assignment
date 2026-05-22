CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${bronze_table} (
    event_time     STRING,
    event_type     STRING,
    product_id     STRING,
    category_id    STRING,
    category_code  STRING,
    brand          STRING,
    price          STRING,
    user_id        STRING,
    user_session   STRING
)
PARTITIONED BY (event_date_utc DATE)
STORED AS PARQUET
LOCATION '${bronze_output_path}'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
