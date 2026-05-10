import duckdb, plotly.express as px

# LOCAL: duckdb.connect(read_only=True) — only reads, avoids lock conflict with cdc_handler.py.
# AWS: replace with boto3 Athena client (8 lines change)
con = duckdb.connect("data\\sensorflow.db", read_only=True)

df = con.execute("""
    SELECT machine_id,
           CAST(reading_ts AS DATE) AS reading_date,
           AVG(reading_value) OVER (
               PARTITION BY machine_id
               ORDER BY CAST(reading_ts AS DATE)
               ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
           ) AS rolling_avg_7d
    FROM fact_sensor_readings f
    JOIN dim_sensor s ON f.sensor_sk = s.sensor_sk
    WHERE s.sensor_type = 'temperature'
    ORDER BY machine_id, reading_date
""").df()  # .df() returns pandas DataFrame — unique to DuckDB

fig = px.line(df, x='reading_date', y='rolling_avg_7d', color='machine_id',
              title='7-Day Rolling Average Temperature — SensorFlow',
              labels={'rolling_avg_7d':'Avg Temp (°C)','reading_date':'Date'})
fig.write_html('data\\bq04_rolling_avg.html')
print("Chart saved: data\\bq04_rolling_avg.html — open in browser")
con.close()