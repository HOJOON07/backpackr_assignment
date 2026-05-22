CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${wau_table} (
    week_start_date  DATE,
    wau_by_user      BIGINT,
    wau_by_session   BIGINT
)
STORED AS PARQUET
LOCATION '${wau_output_path}'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY')
