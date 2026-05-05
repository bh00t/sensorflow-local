SELECT
    machine_id,
    MAX(reading_value)                           AS max_temp,
    COUNT(*) FILTER (WHERE status = 'HIGH')      AS high_count,
    COUNT(*)                                     AS total_readings
FROM fact_sensor_readings f
JOIN dim_sensor s ON f.sensor_sk = s.sensor_sk
WHERE s.sensor_type = 'temperature'
GROUP BY machine_id
ORDER BY high_count DESC;