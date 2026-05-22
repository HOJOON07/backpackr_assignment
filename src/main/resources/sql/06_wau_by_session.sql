SELECT
    date_trunc('week', event_date_kst) AS week_start_date,
    count(DISTINCT session_id)         AS wau_by_session
FROM ${database}.${table}
GROUP BY date_trunc('week', event_date_kst)
ORDER BY week_start_date
