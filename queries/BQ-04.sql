SELECT
    machine_id,
    CAST(reading_ts AS DATE) AS reading_date,
    AVG(reading_value) OVER (
        PARTITION BY machine_id
        ORDER BY CAST(reading_ts AS DATE)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7d
FROM fact_sensor_readings f
JOIN dim_sensor s ON f.sensor_sk = s.sensor_sk
WHERE s.sensor_type = 'temperature'
ORDER BY machine_id, reading_date;