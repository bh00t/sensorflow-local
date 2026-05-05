SELECT
    shift,
    COUNT(*) AS total_readings,
    COUNT(*) FILTER (WHERE status != 'NORMAL')   AS anomalies,
    ROUND(
        COUNT(*) FILTER (WHERE status != 'NORMAL') * 100.0 / COUNT(*), 2
    ) AS anomaly_pct
FROM fact_sensor_readings
GROUP BY shift
ORDER BY anomaly_pct DESC;