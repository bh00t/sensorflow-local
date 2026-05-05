import os

# Point to your local JARs
jar_path = os.path.abspath("./src/etl/jars")
hadoop_aws_jar = os.path.join(jar_path, "hadoop-aws-3.3.4.jar")
aws_sdk_jar = os.path.join(jar_path, "aws-java-sdk-bundle-1.12.367.jar")

# Combine them with a semicolon for Windows
windows_classpath = f"{hadoop_aws_jar};{aws_sdk_jar}"


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, hour, current_timestamp
from pyspark.sql.types import DoubleType

# ── CONFIG — only these 3 constants change for AWS deployment ──
RAW_PATH        = "s3a://sensorflow-local/raw/"
PROCESSED_PATH  = "s3a://sensorflow-local/processed/"
QUARANTINE_PATH = "s3a://sensorflow-local/quarantine/"

# ── Spark session ─────────────────────────────────────────────
# Remove the 5 config lines below + spark.jars.packages for AWS Glue
spark = (SparkSession.builder
         .appName("SensorFlow-ETL-Local")
         # Replace spark.jars with extraClassPath to stop the temp folder copying
         .config("spark.driver.extraClassPath", windows_classpath)
         .config("spark.executor.extraClassPath", windows_classpath)
         .config("spark.hadoop.fs.s3a.endpoint",          "http://localhost:9000")
         .config("spark.hadoop.fs.s3a.access.key",        "sensorflow")
         .config("spark.hadoop.fs.s3a.secret.key",        "sensorflow123")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ── 1. READ raw JSON ─────────────────────────────────────────
raw_df = spark.read.option("multiline", "false").json(RAW_PATH)
total_input = raw_df.count()
print(f"Input rows: {total_input}")

# ── 2. CAST types ────────────────────────────────────────────
typed_df = (raw_df
    .withColumn("reading_value", col("value").cast(DoubleType()))
    .withColumn("reading_ts",    to_timestamp(col("ts")))
    .withColumn("_ingested_at",  current_timestamp())
)

# ── 3. QUARANTINE bad casts ──────────────────────────────────
bad_df  = typed_df.filter(col("reading_value").isNull())
good_df = typed_df.filter(col("reading_value").isNotNull())
bad_count = bad_df.count()
if bad_count > 0:
    bad_df.write.mode("append").json(QUARANTINE_PATH)
    print(f"Quarantined {bad_count} bad records")

# ── 4. DEDUPLICATE on message_id ─────────────────────────────
dedup_df    = good_df.dropDuplicates(["message_id"])
dedup_count = dedup_df.count()
print(f"After dedup: {dedup_count} rows ({total_input - dedup_count} dupes removed)")

# ── 5 & 6. ANOMALY FLAG + SHIFT ────────────────────────────
enriched_df = (dedup_df
    .withColumn("is_anomaly", when(
        ((col("sensor_type")=="temperature") & (col("reading_value") > 90))  |
        ((col("sensor_type")=="vibration")   & (col("reading_value") > 15))  |
        ((col("sensor_type")=="pressure")    & (col("reading_value") > 160)) |
        ((col("sensor_type")=="power")       & (col("reading_value") > 60)),
        True).otherwise(False))
    .withColumn("shift", when(
        (hour("reading_ts") >= 6)  & (hour("reading_ts") < 14), "morning")
        .when((hour("reading_ts") >= 14) & (hour("reading_ts") < 22), "afternoon")
        .otherwise("night"))
)

# ── 7. WRITE Silver Parquet ──────────────────────────────────
(enriched_df
    .write
    .mode("append")
    .partitionBy("year", "month", "day")
    .parquet(PROCESSED_PATH))
print(f"Silver Parquet written. Total good rows: {dedup_count - bad_count}")