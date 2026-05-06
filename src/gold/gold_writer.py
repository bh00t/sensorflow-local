import duckdb

DB_PATH = "data\\sensorflow.db"
con = duckdb.connect(DB_PATH)

# Configure DuckDB to read Parquet files from local MinIO ──────
con.execute("""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='localhost:9000';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='sensorflow';
    SET s3_secret_access_key='sensorflow123';
""")

# ── CREATE SCHEMA ────────────────────────────────────────────
con.execute("""
    CREATE TABLE IF NOT EXISTS dim_machine (
        machine_sk   INTEGER PRIMARY KEY,
        machine_id   VARCHAR(20) UNIQUE NOT NULL,
        machine_type VARCHAR(50),
        plant        VARCHAR(20),
        status       VARCHAR(10) DEFAULT 'active'
    )
""" )

con.execute("""
    CREATE TABLE IF NOT EXISTS dim_sensor (
        sensor_sk         INTEGER PRIMARY KEY,
        sensor_type       VARCHAR(30) UNIQUE NOT NULL,
        unit              VARCHAR(20),
        anomaly_threshold DOUBLE
    )
""" )

con.execute("""
    CREATE TABLE IF NOT EXISTS dim_location (
        location_sk  INTEGER PRIMARY KEY,
        location_id  VARCHAR(20) UNIQUE NOT NULL,
        plant_name   VARCHAR(50),
        city         VARCHAR(50),
        state        VARCHAR(50)
    )
""" )

con.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_sk     INTEGER PRIMARY KEY,
        full_date   DATE UNIQUE NOT NULL,
        year        INTEGER, month INTEGER, day INTEGER,
        day_of_week INTEGER, is_weekend BOOLEAN
    )
""" )

con.execute("""
    CREATE TABLE IF NOT EXISTS fact_sensor_readings (
        reading_id    VARCHAR(36) PRIMARY KEY,
        machine_sk    INTEGER REFERENCES dim_machine(machine_sk),
        sensor_sk     INTEGER REFERENCES dim_sensor(sensor_sk),
        reading_value DOUBLE,
        reading_ts    TIMESTAMP,
        machine_id    VARCHAR(20),
        sensor_type   VARCHAR(30),
        status        VARCHAR(10),
        shift         VARCHAR(15),
        is_anomaly    BOOLEAN,
        _ingested_at  TIMESTAMP,
        _loaded_at    TIMESTAMP DEFAULT current_timestamp
    )
""" )

# ── SEED DIMENSIONS (INSERT OR IGNORE = safe to re-run) ──────
con.execute("""
    INSERT OR IGNORE INTO dim_machine VALUES
        (1,'MACHINE_01','CNC Mill',       'Plant-A','active'),
        (2,'MACHINE_02','Conveyor Belt',   'Plant-A','active'),
        (3,'MACHINE_03','Air Compressor',  'Plant-B','active'),
        (4,'MACHINE_04','Industrial Oven', 'Plant-B','active'),
        (5,'MACHINE_05','Hydraulic Press', 'Plant-C','active')
""" )

con.execute("""
    INSERT OR IGNORE INTO dim_sensor VALUES
        (1,'temperature','celsius',90),
        (2,'vibration',  'mm/s',   15),
        (3,'pressure',   'PSI',   160),
        (4,'power',      'kW',     60)
""" )

con.execute("""
    INSERT OR IGNORE INTO dim_location VALUES
        (1,'Plant-A','Plant Alpha','Mumbai',   'Maharashtra'),
        (2,'Plant-B','Plant Beta', 'Pune',     'Maharashtra'),
        (3,'Plant-C','Plant Gamma','Bangalore','Karnataka')
""" )

# Generate 1096 days (2024–2026) using DuckDB range function
# Start from 2024 to match the AWS Athena dim_date and support any migrated data
con.execute("""
    INSERT OR IGNORE INTO dim_date
    SELECT CAST(strftime(dt,'%Y%m%d') AS INTEGER) AS date_sk,
           dt AS full_date, year(dt), month(dt), day(dt),
           dayofweek(dt), dayofweek(dt) IN (0,6) AS is_weekend
    FROM (SELECT range AS dt FROM range(DATE '2024-01-01', DATE '2027-01-01', INTERVAL 1 DAY))
""" )

# ── LOAD FACT from Silver Parquet ─────────────────────────────────────────────
# IDEMPOTENCY LAYER 2: INSERT OR IGNORE on reading_id (PRIMARY KEY).
# If reading_id already exists, DuckDB silently skips it — re-runs are safe.
#
# IMPORTANT: this is a safety net, not the primary guard.
# The real fix is Phase 02 dynamic partition overwrite keeping Silver clean.
# If Silver has append-mode duplicates with different reading_ids, INSERT OR IGNORE
# will NOT catch them — duplicates load and aggregations become wrong.
# Always verify Silver idempotency (Phase 02 verify step) before running Gold.
con.execute("""
    INSERT OR IGNORE INTO fact_sensor_readings
    SELECT p.message_id, dm.machine_sk, ds.sensor_sk,
           p.reading_value, p.reading_ts, p.machine_id,
           p.sensor_type, p.status, p.shift, p.is_anomaly,
           p._ingested_at, current_timestamp
    FROM read_parquet('s3://sensorflow-local/processed/**/*.parquet') p
    JOIN dim_machine dm ON dm.machine_id  = p.machine_id
    JOIN dim_sensor  ds ON ds.sensor_type = p.sensor_type
""" )

n = con.execute("SELECT COUNT(*) FROM fact_sensor_readings").fetchone()[0]
print(f"Gold FACT loaded: {n} rows")
con.close()