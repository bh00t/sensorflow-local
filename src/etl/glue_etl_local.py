# ── WINDOWS FIX: must run before importing PySpark ─────────────────────────────────────
# Root cause: spark.jars.packages copies JARs into the system temp folder and tries to
# delete them on exit. Windows file-locking prevents the delete and raises PermissionError.
# Fix: load JARs via extraClassPath — no copy, no delete, no error.
import os

# Absolute paths to the local JAR files in src/etl/jars/
jar_path       = os.path.abspath('./src/etl/jars')
hadoop_aws_jar = os.path.join(jar_path, 'hadoop-aws-3.3.4.jar')
aws_sdk_jar    = os.path.join(jar_path, 'aws-java-sdk-bundle-1.12.367.jar')
windows_classpath = f'{hadoop_aws_jar};{aws_sdk_jar}'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, hour, current_timestamp
from pyspark.sql.types import DoubleType

# ── CONFIG — only these 3 constants change for AWS Glue ────────────────────────────────
RAW_PATH        = 's3a://sensorflow-local/raw/'
PROCESSED_PATH  = 's3a://sensorflow-local/processed/'
QUARANTINE_PATH = 's3a://sensorflow-local/quarantine/'

# ── Spark session ──────────────────────────────────────────────────────────────────────
# IDEMPOTENCY KEY: spark.sql.sources.partitionOverwriteMode = dynamic
# This single config is what makes re-running the ETL safe.
# Without it, mode('overwrite') wipes ALL partitions every run — months of data gone.
# With 'dynamic': only the partitions in THIS batch are replaced. All others untouched.
# AWS Glue migration: remove both extraClassPath lines + all 4 S3A lines. Keep partitionOverwriteMode.
spark = (SparkSession.builder
    .appName('SensorFlow-ETL-Local')
    .config('spark.driver.extraClassPath',               windows_classpath)  # REMOVE for Glue
    .config('spark.executor.extraClassPath',             windows_classpath)  # REMOVE for Glue
    .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')          # KEEP for Glue
    .config('spark.hadoop.fs.s3a.endpoint',             'http://localhost:9000')  # REMOVE for Glue
    .config('spark.hadoop.fs.s3a.access.key',           'sensorflow')            # REMOVE for Glue
    .config('spark.hadoop.fs.s3a.secret.key',           'sensorflow123')         # REMOVE for Glue
    .config('spark.hadoop.fs.s3a.path.style.access',    'true')                  # REMOVE for Glue
    .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

# ── 1. READ raw JSON ───────────────────────────────────────────────────────────────────
raw_df = spark.read.option('multiline', 'false').json(RAW_PATH)
total_input = raw_df.count()
print(f'Input rows: {total_input}')

# ── 2. CAST types ──────────────────────────────────────────────────────────────────────
typed_df = (raw_df
    .withColumn('reading_value', col('value').cast(DoubleType()))
    .withColumn('reading_ts',    to_timestamp(col('ts')))
    .withColumn('_ingested_at',  current_timestamp())
)

# ── 3. QUARANTINE bad casts ────────────────────────────────────────────────────────────
bad_df  = typed_df.filter(col('reading_value').isNull())
good_df = typed_df.filter(col('reading_value').isNotNull())
bad_count = bad_df.count()
if bad_count > 0:
    bad_df.write.mode('append').json(QUARANTINE_PATH)
    print(f'Quarantined {bad_count} bad records')

# ── 4. DEDUPLICATE on message_id ───────────────────────────────────────────────────────
# WHY THIS EXISTS: running the ETL multiple times on the same Bronze data (normal during
# local dev) would write duplicate rows into Silver if we didn't guard here.
# message_id is the natural idempotency key — one unique ID per sensor reading.
# This dedup runs BEFORE the write so Silver never accumulates duplicates regardless
# of how many times you run the ETL. The Gold writer's INSERT OR IGNORE is a second
# safety net — clean Silver is always better than fixing duplicates downstream in Gold.
dedup_df    = good_df.dropDuplicates(['message_id'])
dedup_count = dedup_df.count()
print(f'After dedup: {dedup_count} rows ({total_input - dedup_count} dupes removed)')

# ── 5 & 6. ANOMALY FLAG + SHIFT ──────────────────────────────────────────────────────
enriched_df = (dedup_df
    .withColumn('is_anomaly', when(
        ((col('sensor_type')=='temperature') & (col('reading_value') > 90))  |
        ((col('sensor_type')=='vibration')   & (col('reading_value') > 15))  |
        ((col('sensor_type')=='pressure')    & (col('reading_value') > 160)) |
        ((col('sensor_type')=='power')       & (col('reading_value') > 60)),
        True).otherwise(False))
    .withColumn('shift', when(
        (hour('reading_ts') >= 6)  & (hour('reading_ts') < 14), 'morning')
        .when((hour('reading_ts') >= 14) & (hour('reading_ts') < 22), 'afternoon')
        .otherwise('night'))
)

# ── 7. WRITE Silver Parquet — idempotent dynamic partition overwrite ───────────────────
#
# mode('overwrite') + partitionOverwriteMode='dynamic' (set in SparkSession config above):
#   Only partitions present in THIS dataframe are replaced on disk.
#   All OTHER existing partitions are left completely untouched.
#   Re-running the ETL on the same data = same Parquet files, no accumulation.
#
# WHY NOT mode('append')?  ← the data swamp trap
#   Append adds new Parquet files alongside existing ones on every run.
#   3 runs = 3 copies of every row. DuckDB and Athena read ALL files in the
#   partition folder, so row counts triple silently. Aggregations are wrong,
#   Gold writer gets duplicate PRIMARY KEY errors, dashboards show inflated numbers.
#   This is data swampification — Silver becomes unreliable garbage.
#
# WHY NOT plain mode('overwrite') without dynamic?
#   Static overwrite wipes the ENTIRE PROCESSED_PATH on every run.
#   Running the ETL for today's data deletes all previous months' partitions.
#
# AWS Glue: this exact write call works unchanged. Glue job bookmarks handle
#   'only process new Bronze files' on the read side — the write stays the same.
(enriched_df
    .write
    .mode('overwrite')
    .partitionBy('year', 'month', 'day')
    .parquet(PROCESSED_PATH))

partition_count = enriched_df.select('year','month','day').distinct().count()
print(f'Silver written — {dedup_count} rows across {partition_count} partitions')
print('Re-run this script — row count must be identical (idempotency check).')