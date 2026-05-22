WITH parsed AS (
    SELECT
        try_to_timestamp(regexp_replace(event_time, ' UTC$', '')) AS event_time_utc,
        event_type,
        try_cast(product_id   AS BIGINT) AS product_id,
        try_cast(category_id  AS BIGINT) AS category_id,
        category_code,
        brand,
        try_cast(price        AS DOUBLE) AS price,
        try_cast(user_id      AS BIGINT) AS user_id,
        user_session                     AS source_user_session
    FROM raw_events
),

clean AS (
    SELECT
        *,
        from_utc_timestamp(event_time_utc, 'Asia/Seoul') AS event_time_kst
    FROM parsed
    WHERE event_time_utc IS NOT NULL
      AND user_id IS NOT NULL
),

with_prev AS (
    SELECT
        *,
        lag(event_time_utc) OVER (
            PARTITION BY user_id
            ORDER BY event_time_utc, event_type, product_id
        ) AS prev_event_time_utc
    FROM clean
),

marked AS (
    SELECT
        *,
        CASE
            WHEN prev_event_time_utc IS NULL THEN 1
            WHEN unix_timestamp(event_time_utc) - unix_timestamp(prev_event_time_utc) >= ${gap_seconds} THEN 1
            ELSE 0
        END AS is_new_session
    FROM with_prev
),

sessionized AS (
    SELECT
        *,
        sum(is_new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_time_utc, event_type, product_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_seq
    FROM marked
),

with_session_start AS (
    SELECT
        *,
        min(event_time_utc) OVER (
            PARTITION BY user_id, session_seq
        ) AS session_start_utc
    FROM sessionized
)

SELECT
    event_time_utc,
    event_time_kst,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    source_user_session,
    concat_ws('_', cast(user_id AS string), cast(unix_timestamp(session_start_utc) AS string)) AS session_id,
    session_start_utc,
    to_date(event_time_kst) AS event_date_kst
FROM with_session_start
