"""
export_for_grafana.py
=====================
Exports DuckDB Gold layer data to JSON files served via HTTP server
to Grafana Infinity plugin dashboards.

Run this after every pipeline execution to refresh dashboard data:
    python src/analytics/export_for_grafana.py

HTTP server must be running in a separate terminal:
    cd data/grafana
    python -m http.server 8765

Grafana panels use: http://192.x.x.x:8765/<filename>.json
"""

import duckdb
import json
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────
DB_PATH  = "data/sensorflow.db"
OUT_DIR  = Path("data/grafana")
LIMIT    = 2000   # max rows per export — increase if you need more history

# Anomaly thresholds — must match ETL enrichment logic in glue_etl_local.py
THRESHOLDS = {
    "temperature": 90,
    "vibration":   15,
    "pressure":    160,
    "power":       60,
}

# ── Setup ─────────────────────────────────────────────────────────────────
OUT_DIR.mkdir(parents=True, exist_ok=True)

# read_only=True avoids write-lock conflict if cdc_handler.py is running
con = duckdb.connect(DB_PATH, read_only=True)
print(f"Connected to {DB_PATH}")
print(f"Exporting to {OUT_DIR.resolve()}")
print(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 50)


# ── Export 1: per-sensor-type files (one per sensor) ─────────────────────
# Each file feeds one dedicated Grafana panel.
# Pre-filtering here means Grafana gets clean focused data —
# no mixed Y axis, no transformation needed inside Grafana.
for sensor, threshold in THRESHOLDS.items():
    rows = con.execute(f"""
        SELECT
            machine_id                          AS machine,
            reading_value                       AS value,
            reading_ts::VARCHAR                 AS ts,
            status,
            is_anomaly,
            shift,
            CASE
                WHEN reading_value > {threshold} THEN 'ABOVE_THRESHOLD'
                ELSE 'NORMAL'
            END                                 AS threshold_status
        FROM fact_sensor_readings
        WHERE sensor_type = '{sensor}'
        ORDER BY reading_ts DESC
        LIMIT {LIMIT}
    """).fetchall()

    cols = ["machine", "value", "ts", "status", "is_anomaly",
            "shift", "threshold_status"]
    data = [dict(zip(cols, r)) for r in rows]

    # Convert booleans — JSON serialisation safety
    for row in data:
        row["value"]     = float(row["value"]) if row["value"] is not None else 0.0
        row["is_anomaly"] = bool(row["is_anomaly"])

    filename = f"{sensor}.json"
    (OUT_DIR / filename).write_text(json.dumps(data, default=str))
    anomaly_count = sum(1 for r in data if r["is_anomaly"])
    print(f"  {filename:<25} {len(rows):>4} rows  |  {anomaly_count} anomalies")


# ── Export 2: live_readings.json — all sensors combined ──────────────────
# Used for the overview panel showing all sensor types together.
rows = con.execute(f"""
    SELECT
        machine_id      AS machine,
        sensor_type     AS sensor,
        reading_value   AS value,
        reading_ts::VARCHAR AS ts,
        status,
        shift,
        is_anomaly
    FROM fact_sensor_readings
    ORDER BY reading_ts DESC
    LIMIT {LIMIT}
""").fetchall()
cols = ["machine", "sensor", "value", "ts", "status", "shift", "is_anomaly"]
data = [dict(zip(cols, r)) for r in rows]
for row in data:
    row["value"]      = float(row["value"]) if row["value"] is not None else 0.0
    row["is_anomaly"] = bool(row["is_anomaly"])
(OUT_DIR / "live_readings.json").write_text(json.dumps(data, default=str))
print(f"  {'live_readings.json':<25} {len(rows):>4} rows  |  all sensors combined")


# ── Export 3: anomalies.json — anomalous readings only ───────────────────
# Used for the anomaly log table panel and anomaly count stat panel.
rows2 = con.execute(f"""
    SELECT
        machine_id      AS machine,
        sensor_type     AS sensor,
        reading_value   AS value,
        status,
        reading_ts::VARCHAR AS ts,
        shift
    FROM fact_sensor_readings
    WHERE is_anomaly = true
    ORDER BY reading_ts DESC
    LIMIT 200
""").fetchall()
cols2 = ["machine", "sensor", "value", "status", "ts", "shift"]
data2 = [dict(zip(cols2, r)) for r in rows2]
for row in data2:
    row["value"] = float(row["value"]) if row["value"] is not None else 0.0
(OUT_DIR / "anomalies.json").write_text(json.dumps(data2, default=str))
print(f"  {'anomalies.json':<25} {len(rows2):>4} rows  |  anomalies only")


# ── Export 4: machine_summary.json — averages per machine per sensor ──────
# Used for the bar chart panel showing machine health at a glance.
rows3 = con.execute("""
    SELECT
        machine_id                          AS machine,
        sensor_type                         AS sensor,
        ROUND(AVG(reading_value), 2)        AS avg_value,
        ROUND(MAX(reading_value), 2)        AS max_value,
        ROUND(MIN(reading_value), 2)        AS min_value,
        COUNT(*)                            AS total_readings,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
        ROUND(
            100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*),
            1
        )                                   AS anomaly_pct
    FROM fact_sensor_readings
    GROUP BY machine_id, sensor_type
    ORDER BY machine_id, sensor_type
""").fetchall()
cols3 = ["machine", "sensor", "avg_value", "max_value", "min_value",
         "total_readings", "anomaly_count", "anomaly_pct"]
data3 = [dict(zip(cols3, r)) for r in rows3]
(OUT_DIR / "machine_summary.json").write_text(json.dumps(data3, default=str))
print(f"  {'machine_summary.json':<25} {len(rows3):>4} rows  |  per machine per sensor")


# ── Export 5: anomaly_rate.json — anomaly % per machine ──────────────────
# Used for the gauge/stat panel showing which machine is most problematic.
rows4 = con.execute("""
    SELECT
        machine_id                          AS machine,
        COUNT(*)                            AS total,
        SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies,
        ROUND(
            100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*),
            1
        )                                   AS anomaly_pct
    FROM fact_sensor_readings
    GROUP BY machine_id
    ORDER BY anomaly_pct DESC
""").fetchall()
cols4 = ["machine", "total", "anomalies", "anomaly_pct"]
data4 = [dict(zip(cols4, r)) for r in rows4]
(OUT_DIR / "anomaly_rate.json").write_text(json.dumps(data4, default=str))
print(f"  {'anomaly_rate.json':<25} {len(rows4):>4} rows  |  anomaly % per machine")


# ── Done ──────────────────────────────────────────────────────────────────
con.close()
print("-" * 50)
print(f"All files written to: {OUT_DIR.resolve()}")
print(f"Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()
print("Grafana panel URLs (replace IP with your LAN IP):")
files = [
    "temperature.json", "vibration.json", "pressure.json", "power.json",
    "live_readings.json", "anomalies.json", "machine_summary.json",
    "anomaly_rate.json"
]
for f in files:
    print(f"  http://192.x.x.x:8765/{f}")
