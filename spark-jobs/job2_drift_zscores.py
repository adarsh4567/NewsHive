import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


spark = (
    SparkSession.builder
    .appName("FinancialAgent_Job2_Z-ScoreCalculator")
    .config("spark.hadoop.fs.s3a.impl",         "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET  = spark.conf.get("spark.agent.bucket",     os.getenv("S3_BUCKET_NAME",      "agent-eval-bucket"))
AWS_KEY    = spark.conf.get("spark.agent.aws.key",    os.getenv("AWS_ACCESS_KEY_ID",   ""))
AWS_SECRET = spark.conf.get("spark.agent.aws.secret", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
AWS_REGION = spark.conf.get("spark.agent.aws.region", os.getenv("AWS_REGION",          "ap-south-1"))

hc = spark.sparkContext._jsc.hadoopConfiguration()
hc.set("fs.s3a.access.key",   AWS_KEY)
hc.set("fs.s3a.secret.key",   AWS_SECRET)
hc.set("fs.s3a.endpoint",     f"s3.{AWS_REGION}.amazonaws.com")
hc.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

baseline_stats = {
    # Derived from the user-provided baseline record:
    # seq_length=8, total_tool_calls=3, tavily=2, stock=1, quality=1.0
    "seq_length_mean": 8.0,
    "seq_length_std": 1.5,
    "total_tool_calls_mean": 3.0,
    "total_tool_calls_std": 1.0,
    "tavily_tool_count_mean": 2.0,
    "tavily_tool_count_std": 0.8,
    "stock_tool_count_mean": 1.0,
    "stock_tool_count_std": 0.5,
    "stock_report_quality_mean": 1.0,
    # FIX: Job 1 now correctly scores null fields as 0.0 (not 1.0).
    # A std of 0.1 was too tight and produced Z = -10 for every null-data run.
    # 0.35 gives a realistic spread across the full 0.0-1.0 quality range,
    # so the Z-score alert threshold of ±2.0 is meaningful and calibrated.
    "stock_report_quality_std": 0.35
}

PROCESSED_PATH = f"s3a://{S3_BUCKET}/processed/agent_fingerprints/"
df = spark.read.parquet(PROCESSED_PATH)

processed_count = df.count()
print(f"[Step 1] Processed records loaded: {processed_count}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Broadcast Mock Baseline Stats
# ─────────────────────────────────────────────────────────────────────────────
for col_name, value in baseline_stats.items():
    df = df.withColumn(col_name, F.lit(value))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Calculate Z-Scores
# Formula: (Value - Mean) / StdDev
# ─────────────────────────────────────────────────────────────────────────────
df = df.withColumn("z_seq_length",           (F.col("seq_length") - F.col("seq_length_mean")) / F.col("seq_length_std"))
df = df.withColumn("z_total_tool_calls",     (F.col("total_tool_calls") - F.col("total_tool_calls_mean")) / F.col("total_tool_calls_std"))
df = df.withColumn("z_tavily_tool_count",    (F.col("tavily_tool_count") - F.col("tavily_tool_count_mean")) / F.col("tavily_tool_count_std"))
df = df.withColumn("z_stock_tool_count",     (F.col("stock_tool_count") - F.col("stock_tool_count_mean")) / F.col("stock_tool_count_std"))
df = df.withColumn("z_stock_report_quality", (F.col("stock_report_quality") - F.col("stock_report_quality_mean")) / F.col("stock_report_quality_std"))

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Flag Anomalies (Drift Alerts)
# Z-Scores > 2.0 or < -2.0 are flagged. Categorical failures are explicitly flagged.
# ─────────────────────────────────────────────────────────────────────────────
df = df.withColumn("alert_seq_length",           F.abs(F.col("z_seq_length")) > 2.0)
df = df.withColumn("alert_total_tool_calls",     F.abs(F.col("z_total_tool_calls")) > 2.0)
df = df.withColumn("alert_tavily_tool_count",    F.abs(F.col("z_tavily_tool_count")) > 2.0)
df = df.withColumn("alert_stock_tool_count",     F.abs(F.col("z_stock_tool_count")) > 2.0)
df = df.withColumn("alert_stock_report_quality", F.abs(F.col("z_stock_report_quality")) > 2.0)

# Categorical Alerts
df = df.withColumn("alert_looping",           F.col("is_looping") == True)
df = df.withColumn("alert_api_starvation",    F.col("api_starvation") == True)
df = df.withColumn("alert_extraction_failed", F.col("extraction_balance") == False)
df = df.withColumn("alert_run_failed",        F.col("is_success") == False)

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Select & Write Output
# ─────────────────────────────────────────────────────────────────────────────
df_final = df.select(
    "run_id", "timestamp", "run_date",
    "z_seq_length", "z_total_tool_calls", "z_tavily_tool_count", 
    "z_stock_tool_count", "z_stock_report_quality",
    "alert_seq_length", "alert_total_tool_calls", "alert_tavily_tool_count",
    "alert_stock_tool_count", "alert_stock_report_quality",
    "alert_looping", "alert_api_starvation", "alert_extraction_failed", "alert_run_failed"
)

OUTPUT_PATH = f"s3a://{S3_BUCKET}/metrics/drift_scores/"

print("\n[Step 2] Final Output Schema:")
df_final.printSchema()

print(f"\n[Step 3] Writing Drift Scores Parquet to: {OUTPUT_PATH}")
df_final.write.mode("overwrite").partitionBy("run_date").parquet(OUTPUT_PATH)

print(f"\n{'='*60}")
print(f"  JOB 2 COMPLETE")
print(f"{'='*60}\n")
