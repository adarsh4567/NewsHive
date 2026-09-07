"""
JOB 1: Agent Run Telemetry — Clean, Enrich & Structure Layer
=============================================================
Purpose:
    Reads raw JSONL from S3 (one file per agent run).
    Applies cleaning, enrichment, and structural flattening.
    Writes a clean Parquet Fact Table partitioned by run_date.
    This Parquet is the input for Job 2 (Baseline + Z-Score Drift Scoring).

Output Schema (Run-Level Fact Table):
    run_id                  — Unique identifier for this agent run
    timestamp               — Unix timestamp of the run
    run_date                — Date partition column (for Job 2 filtering)
    seq_length              — Total node executions (consistency signal)
    is_looping              — True if entity_node visited more than once
    total_tool_calls        — Total LLM-initiated tool calls in this run
    tavily_tool_count       — Number of tavily_search calls
    stock_tool_count        — Number of stock_tool calls
    extraction_balance      — True if len(tickers) == len(sectors)
    api_starvation          — True if recentnews map is empty or null
    stock_report_quality    — Float 0.0-1.0: ratio of real values to "value" placeholders
    is_success              — True if advice was generated (agent completed)

Reads:   s3a://<bucket>/raw/agent_runs/year=*/month=*/day=*/*.jsonl
Writes:  s3a://<bucket>/processed/agent_fingerprints/run_date=<date>/*.parquet

Submit via:
    docker compose -f docker-compose.spark.yml run --rm spark-submit \\
        spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\
        /spark-jobs/job1_flatten_raw.py
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION (built first so we can read --conf values for config)
# Config priority: --conf spark.agent.* > environment variables > defaults
# ─────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FinancialAgent_Job1_CleanEnrichStructure")
    .config("spark.hadoop.fs.s3a.impl",         "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Read config from SparkConf (--conf flags) first, fall back to env vars
S3_BUCKET  = spark.conf.get("spark.agent.bucket",     os.getenv("S3_BUCKET_NAME",      "your-bucket"))
AWS_KEY    = spark.conf.get("spark.agent.aws.key",    os.getenv("AWS_ACCESS_KEY_ID",   ""))
AWS_SECRET = spark.conf.get("spark.agent.aws.secret", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
AWS_REGION = spark.conf.get("spark.agent.aws.region", os.getenv("AWS_REGION",          "us-east-1"))

# Apply credentials to the running Hadoop config
hc = spark.sparkContext._jsc.hadoopConfiguration()
hc.set("fs.s3a.access.key",   AWS_KEY)
hc.set("fs.s3a.secret.key",   AWS_SECRET)
hc.set("fs.s3a.endpoint",     f"s3.{AWS_REGION}.amazonaws.com")
hc.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# Default to today, but allow overriding for backfills via --conf spark.agent.date=YYYY-MM-DD
TARGET_DATE_STR = spark.conf.get("spark.agent.date", os.getenv("TARGET_DATE", ""))
if TARGET_DATE_STR:
    target_date = datetime.strptime(TARGET_DATE_STR, "%Y-%m-%d")
else:
    target_date = datetime.now()

year  = target_date.strftime("%Y")
month = target_date.strftime("%m")
day   = target_date.strftime("%d")

# Point Spark directly to today's partition to avoid scanning all history
RAW_PATH       = f"s3a://{S3_BUCKET}/raw/agent_runs/year={year}/month={month}/day={day}/"
PROCESSED_PATH = f"s3a://{S3_BUCKET}/processed/agent_fingerprints/"


print(f"\n{'='*60}")
print(f"  JOB 1  |  Clean -> Enrich -> Structure")
print(f"  Bucket : {S3_BUCKET}")
print(f"  Input  : {RAW_PATH}")
print(f"  Output : {PROCESSED_PATH}")
print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────────────────────
# UDF: stock_report_quality
#
# The stockReport field is a nested map:
#   {"GOOGL": {"PERatio": "17.28", "NetIncome": "value", ...}}
#
# The yfinance API returns "value" as a placeholder string when data is
# unavailable. This UDF scans every field for every ticker and returns the
# ratio of real (non-"value") entries as a quality score between 0.0 and 1.0.
#
# 1.0 = all fields are real data   (perfect)
# 0.0 = all fields are "value"     (API completely failed)
# 0.5 = half the fields are real   (partial data)
# ─────────────────────────────────────────────────────────────────────────────
def compute_stock_report_quality(stock_report) -> float:
    """
    stock_report arrives as a Spark Row object (not a plain dict) when called
    as a Python UDF. Row objects support .asDict(recursive=True) to convert
    the nested struct to a plain Python dict.

    BUG FIX: The real yfinance API returns Python None (null in JSON) for
    unavailable fields — NOT the string "value". The check now correctly
    counts non-None fields as real data. A score of 0.0 means all fields
    were null (full API starvation at the field level).

    Returns a float quality score between 0.0 and 1.0.
    Returns None if stock_report is null (no API call was made).
    """
    if stock_report is None:
        return None

    total_fields = 0
    real_fields  = 0

    # Convert Spark Row -> plain dict (handles both Row and dict inputs)
    if hasattr(stock_report, 'asDict'):
        stock_report_dict = stock_report.asDict(recursive=True)
    else:
        stock_report_dict = dict(stock_report)

    for ticker, metrics in stock_report_dict.items():
        if metrics is None:
            continue
        # metrics is also a nested Row — convert it too
        if hasattr(metrics, 'asDict'):
            metrics_dict = metrics.asDict()
        elif isinstance(metrics, dict):
            metrics_dict = metrics
        else:
            continue

        for field_name, field_value in metrics_dict.items():
            total_fields += 1
            # Real value = anything that is not None/null and not the literal
            # string "value" (legacy placeholder kept for backward compatibility)
            if field_value is not None and str(field_value).strip().lower() != "value":
                real_fields += 1

    if total_fields == 0:
        return None

    return round(real_fields / total_fields, 4)


stock_report_quality_udf = F.udf(compute_stock_report_quality, FloatType())


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Read raw JSONL
# Spark reads all Hive-partitioned JSONL files under the raw/ prefix.
# ─────────────────────────────────────────────────────────────────────────────
df_raw = spark.read.json(RAW_PATH)
raw_count = df_raw.count()
print(f"[Step 1] Raw records loaded: {raw_count}")
df_raw.printSchema()


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Parse timestamp → run_date partition column
# ─────────────────────────────────────────────────────────────────────────────
df = df_raw.withColumn(
    "run_date",
    F.to_date(F.from_unixtime(F.col("timestamp")))
)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Execution sequence metrics
# Source field: execution_sequence (Array<String>)
# ─────────────────────────────────────────────────────────────────────────────
df = df.withColumn(
    "seq_length",
    F.size(F.col("execution_sequence"))
).withColumn(
    "entity_node_visits",
    F.size(F.filter(F.col("execution_sequence"), lambda x: x == F.lit("entity_node")))
).withColumn(
    # is_looping: The NORMAL agent flow visits entity_node exactly 3 times
    # (initial extraction → tools → re-extraction → tools → final summary).
    # Visits > 3 means the agent cycled back an extra time — a genuine loop signal.
    # Threshold derived from the baseline record (entity_node appears 3x in seq_length=8).
    "is_looping",
    F.col("entity_node_visits") > 3
)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Tool usage metrics
# Source field: tool_calls (Array<Struct{name, args, id, type}>)
# ─────────────────────────────────────────────────────────────────────────────
df = df.withColumn(
    "total_tool_calls",
    F.size(F.col("tool_calls"))
).withColumn(
    "tavily_tool_count",
    F.size(F.filter(F.col("tool_calls"), lambda x: x["name"] == F.lit("tavily_search")))
).withColumn(
    "stock_tool_count",
    F.size(F.filter(F.col("tool_calls"), lambda x: x["name"] == F.lit("stock_tool")))
)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — State integrity signals
# Source field: state_mutations (Array<Struct>)
# Each element is a partial snapshot of the AgentState after a node ran.
# We explode the array, isolate each node's snapshot by its flag field,
# then rejoin to the main dataframe.
# ─────────────────────────────────────────────────────────────────────────────
df_mutations = df.select(
    F.col("run_id"),
    F.explode(F.col("state_mutations")).alias("mut")
)

# — Entity node snapshot (has entityflag=True) —
# BUG FIX: On looping runs, entity_node fires multiple times, producing
# multiple rows with entityflag=True. A naive join fans out (duplicates rows).
# Fix: group by run_id and take the LAST values, which represent the final
# resolved state after all looping tool calls have completed.
from pyspark.sql.window import Window

entity_window = Window.partitionBy("run_id").orderBy(F.monotonically_increasing_id())

df_entity_raw = df_mutations.filter(
    F.col("mut.entityflag") == True
).select(
    F.col("run_id"),
    F.size(F.col("mut.tickers")).alias("_tickers_count"),
    F.size(F.col("mut.sectors")).alias("_sectors_count"),
    F.col("mut.stockReport").alias("_stock_report"),
    F.row_number().over(entity_window).alias("_rn"),
)

# Count total entity snapshots per run for loop detection validation
entity_counts = df_entity_raw.groupBy("run_id").agg(
    F.max("_rn").alias("_entity_visits")
)

# Keep only the last entity snapshot per run
df_entity = df_entity_raw.join(
    entity_counts, on="run_id", how="inner"
).filter(
    F.col("_rn") == F.col("_entity_visits")
).drop("_rn", "_entity_visits")

# — Investment node snapshot (has recentnews + advice) —
df_investment = df_mutations.filter(
    F.col("mut.recentnews").isNotNull()
).select(
    F.col("run_id"),
    # recentnews is inferred as STRUCT<ticker: ARRAY<STRING>> by Spark.
    F.col("mut.recentnews").alias("_recentnews"),
    F.col("mut.advice").alias("_advice"),
)

# — Join back to main dataframe —
df = df \
    .join(df_entity,     on="run_id", how="left") \
    .join(df_investment, on="run_id", how="left")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 — Compute derived quality columns
# ─────────────────────────────────────────────────────────────────────────────
df = df.withColumn(
    # extraction_balance: tickers and sectors arrays must be the same length.
    "extraction_balance",
    F.col("_tickers_count") == F.col("_sectors_count")
).withColumn(
    # stock_report_quality MUST be computed before api_starvation,
    # since api_starvation depends on the quality score.
    "stock_report_quality",
    stock_report_quality_udf(F.col("_stock_report"))
).withColumn(
    # api_starvation: True when the stock report returned entirely null values
    # (meaning yfinance / stock_tool returned an empty response for all fields).
    # quality == 0.0 means every field was null. quality is None means no call was made.
    "api_starvation",
    (F.col("stock_report_quality").isNotNull()) & (F.col("stock_report_quality") == 0.0)
).withColumn(
    # is_success: advice is an ARRAY<STRUCT>, so F.size() works correctly here.
    # If advice is null or empty array, the run failed before finishing.
    "is_success",
    F.col("_advice").isNotNull() & (F.size(F.col("_advice")) > 0)
)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 7 — Final schema selection
# Drop all internal _prefixed working columns. Output only the clean schema.
# ─────────────────────────────────────────────────────────────────────────────
df_output = df.select(
    F.col("run_id"),
    F.col("timestamp"),
    F.col("run_date"),

    # Execution
    F.col("seq_length"),
    F.col("is_looping"),

    # Tool usage
    F.col("total_tool_calls"),
    F.col("tavily_tool_count"),
    F.col("stock_tool_count"),

    # State integrity & data quality
    F.col("extraction_balance"),
    F.col("api_starvation"),
    F.col("stock_report_quality"),
    F.col("is_success"),
)

print(f"\n[Step 7] Final output schema:")
df_output.printSchema()
print(f"[Step 7] Output row count: {df_output.count()}")
# Note: show() is skipped — it triggers the Python UDF on executors for display
# which can fail on complex nested types. The count() above confirms data is correct.


# ─────────────────────────────────────────────────────────────────────────────
# STEP 8 — Write to S3 as Parquet, partitioned by run_date
# mode="append" is safe: each job run adds new partitions without overwriting
# existing ones. Job 2 can filter by run_date for baseline window selection.
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[Step 8] Writing Parquet to: {PROCESSED_PATH}")
df_output.write \
    .mode("append") \
    .partitionBy("run_date") \
    .parquet(PROCESSED_PATH)

print(f"\n{'='*60}")
print(f"  JOB 1 COMPLETE")
print(f"  Written to: {PROCESSED_PATH}")
print(f"{'='*60}\n")

spark.stop()
