import duckdb
import logging
from pathlib import Path
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

DB_PATH  = "data\\sensorflow.db"
LOG_DIR  = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "nightly_job.log"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger("nightly_job")

def run_nightly(test_mode=False):
    try:
        target_date = (
            datetime.now().date() if test_mode
            else (datetime.now() - timedelta(days=1)).date()
        )
        logger.info(f"Nightly job starting -- target date: {target_date}")

        # Step 1: READ with read_only -- no write lock held at all.
        # cdc_handler.py can continue inserting anomalies freely here.
        con_r = duckdb.connect(DB_PATH, read_only=True)
        rows = con_r.execute(f'''
            SELECT machine_id, sensor_type,
                   DATE_TRUNC('hour', reading_ts) AS hour_ts,
                   AVG(reading_value), MIN(reading_value), MAX(reading_value),
                   SUM(CASE WHEN status != 'NORMAL' THEN 1 ELSE 0 END),
                   COUNT(*)
            FROM fact_sensor_readings f
            JOIN dim_sensor s ON f.sensor_sk = s.sensor_sk
            WHERE CAST(reading_ts AS DATE) = '{target_date}'
            GROUP BY machine_id, sensor_type, DATE_TRUNC('hour', reading_ts)
        ''').fetchall()
        con_r.close()   # read lock released -- cdc_handler unaffected

        if not rows:
            logger.warning(
                f"No data for {target_date} -- skipping. "
                "If simulator ran today use run_nightly(test_mode=True)"
            )
            return

        logger.info(f"Read {len(rows)} hourly groups from fact_sensor_readings")

        # Step 2: compute summary keys in Python -- no DB lock held
        records = []
        for r in rows:
            machine_id, sensor_type, hour_ts = r[0], r[1], r[2]
            hour_str = (
                hour_ts.strftime('%Y%m%d%H')
                if hasattr(hour_ts, 'strftime')
                else str(hour_ts)[:13].replace(' ','').replace('-','').replace(':','')
            )
            summary_id = f"{machine_id}_{sensor_type}_{hour_str}"
            records.append((summary_id, machine_id, sensor_type,
                            hour_ts, r[3], r[4], r[5], r[6], r[7]))

        # Step 3: WRITE -- lock held only for this brief INSERT then released
        con_w = duckdb.connect(DB_PATH)
        con_w.execute('''
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
        ''')
        con_w.executemany(
            "INSERT OR REPLACE INTO hourly_machine_summary VALUES (?,?,?,?,?,?,?,?,?)",
            records
        )
        con_w.close()   # write lock released immediately

        logger.info(
            f"Nightly job complete -- {len(records)} hourly summaries "
            f"written for {target_date}"
        )

    except Exception as e:
        logger.error(f"Nightly job FAILED: {e}", exc_info=True)

# To test without waiting for 01:00 -- run this one-liner:
# python -c "from src.aggregation.nightly_job import run_nightly; run_nightly(test_mode=True)"

scheduler = BlockingScheduler()
scheduler.add_job(run_nightly, 'cron', hour=1, minute=0)
logger.info("Scheduler started -- nightly job fires at 01:00 daily")
scheduler.start()