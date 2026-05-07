import duckdb
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

DB_PATH = "data\\sensorflow.db"

def run_nightly(test_mode=False):
    # test_mode=True: aggregate today's data instead of yesterday's.
    # Use this when running the nightly job manually for the first time
    # (the simulator produces data for today, not yesterday).
    target_date = datetime.now().date() if test_mode else (datetime.now() - timedelta(days=1)).date()
    con = duckdb.connect(DB_PATH)
    count = con.execute(
        f"SELECT COUNT(*) FROM fact_sensor_readings WHERE CAST(reading_ts AS DATE) = '{target_date}'"
    ).fetchone()[0]
    if count == 0:
        print(f"No data for {target_date} — skipping. Tip: if simulator ran today, call run_nightly(test_mode=True)")
        con.close(); return

    con.execute("""
        CREATE TABLE IF NOT EXISTS hourly_machine_summary (
            summary_id    VARCHAR PRIMARY KEY,
            machine_id    VARCHAR(20),
            sensor_type   VARCHAR(30),
            hour_ts       TIMESTAMP,
            avg_value     DOUBLE,
            min_value     DOUBLE,
            max_value     DOUBLE,
            anomaly_count INTEGER,
            reading_count INTEGER
        )
    """)

    con.execute(f"""
        INSERT OR REPLACE INTO hourly_machine_summary
        SELECT machine_id || '_' || sensor_type || '_' || strftime(DATE_TRUNC('hour', reading_ts), '%Y%m%d%H'),
               machine_id, sensor_type, DATE_TRUNC('hour', reading_ts),
               AVG(reading_value), MIN(reading_value), MAX(reading_value),
               SUM(CASE WHEN status!='NORMAL' THEN 1 ELSE 0 END), COUNT(*)
        FROM fact_sensor_readings f
        JOIN dim_sensor s ON f.sensor_sk = s.sensor_sk
        WHERE CAST(reading_ts AS DATE) = '{target_date}'
        GROUP BY machine_id, sensor_type, DATE_TRUNC('hour', reading_ts)
    """)
    print(f"Nightly job done for {target_date}: {count} rows aggregated")
    con.close()

# To run immediately for testing, uncomment the line below:
# run_nightly()

scheduler = BlockingScheduler()
scheduler.add_job(run_nightly, 'cron', hour=1, minute=0)
print("Scheduler running — nightly job fires at 01:00")
scheduler.start()