INSERT OVERWRITE TABLE ${database}.${wau_table}
SELECT
    cast(date_trunc('week', event_date_kst) AS DATE) AS week_start_date,
    count(DISTINCT user_id)                          AS wau_by_user,
    count(DISTINCT session_id)                       AS wau_by_session
FROM ${database}.${table}
GROUP BY date_trunc('week', event_date_kst)
