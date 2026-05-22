SELECT
    date_trunc('week', event_date_kst) AS week_start_date,
    count(DISTINCT user_id)            AS wau_by_user
FROM ${database}.${table}
GROUP BY date_trunc('week', event_date_kst)
ORDER BY week_start_date
