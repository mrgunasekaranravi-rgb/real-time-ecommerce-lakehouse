# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Audit & Monitoring

from pyspark.sql import functions as F
from datetime import datetime
import uuid

audit_run_id = str(uuid.uuid4())
audit_start_time = datetime.now()

print("Audit Run ID    :", audit_run_id)
print("Audit Start Time:", audit_start_time)
print("Audit framework initialized successfully")

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType
)

audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), False),
    StructField("rows_processed", LongType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True)
])

audit_df = spark.createDataFrame(
    audit_record,
    schema=audit_schema
)

display(audit_df)

# COMMAND ----------

from pyspark.sql import functions as F

audit_df_final = (
    audit_df
    .withColumn(
        "duration_seconds",
        F.col("end_time").cast("long") - F.col("start_time").cast("long")
    )
)

display(
    audit_df_final.select(
        "run_id",
        "pipeline_name",
        "rows_processed",
        "status",
        "start_time",
        "end_time",
        "duration_seconds"
    )
)

# COMMAND ----------

audit_table = "workspace.default.rt_pipeline_audit_log"

(
    audit_df_final.write
    .format("delta")
    .mode("append")
    .saveAsTable(audit_table)
)

print("Audit record saved successfully")
print("Audit Table:", audit_table)

# COMMAND ----------

audit_history = (
    spark.table("workspace.default.rt_pipeline_audit_log")
    .orderBy(F.col("start_time").desc())
)

display(
    audit_history.select(
        "run_id",
        "pipeline_name",
        "source_table",
        "target_table",
        "rows_processed",
        "status",
        "duration_seconds",
        "start_time",
        "end_time",
        "error_message"
    )
)

# COMMAND ----------

monitoring_summary = (
    spark.table("workspace.default.rt_pipeline_audit_log")
    .groupBy("status")
    .agg(
        F.count("*").alias("run_count"),
        F.sum("rows_processed").alias("total_rows_processed"),
        F.avg("duration_seconds").alias("avg_duration_seconds")
    )
)

display(monitoring_summary)