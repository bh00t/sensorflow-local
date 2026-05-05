SELECT
    machine_id,
    COUNT(*) AS actual_readings,
    ROUND(COUNT(*) * 100.0 / (
        strftime('%d', date_trunc('month', current_date) +
        interval '1 month' - interval '1 day')::INT * 86400 / 5 * 4
    ), 1) AS uptime_pct
FROM fact_sensor_readings
WHERE date_trunc('month', reading_ts) = date_trunc('month', current_timestamp)
GROUP BY machine_id
ORDER BY uptime_pct ASC;