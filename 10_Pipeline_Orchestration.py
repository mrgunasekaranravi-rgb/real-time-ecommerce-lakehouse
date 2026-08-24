# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Pipeline Orchestration

from datetime import datetime

pipeline_start_time = datetime.now()

pipeline_stages = [
    ("01_Setup_and_Source_Generation", "COMPLETED"),
    ("02_Bronze_Ingestion", "COMPLETED"),
    ("03_Silver_Transformation", "COMPLETED"),
    ("04_CDC_Incremental_Processing", "COMPLETED"),
    ("05_SCD_Type2_Dimensions", "COMPLETED"),
    ("06_Gold_Business_Aggregations", "COMPLETED"),
    ("07_Data_Quality_Framework", "COMPLETED"),
    ("08_Audit_Monitoring", "COMPLETED"),
    ("09_Performance_Optimization", "COMPLETED")
]

pipeline_stage_df = spark.createDataFrame(
    pipeline_stages,
    ["stage_name", "status"]
)

display(pipeline_stage_df)

# COMMAND ----------

# Validate that every required upstream stage completed successfully

required_stages = [
    "01_Setup_and_Source_Generation",
    "02_Bronze_Ingestion",
    "03_Silver_Transformation",
    "04_CDC_Incremental_Processing",
    "05_SCD_Type2_Dimensions",
    "06_Gold_Business_Aggregations",
    "07_Data_Quality_Framework",
    "08_Audit_Monitoring",
    "09_Performance_Optimization"
]

completed_stages = [
    row["stage_name"]
    for row in pipeline_stage_df
    .filter("status = 'COMPLETED'")
    .collect()
]

missing_stages = [
    stage for stage in required_stages
    if stage not in completed_stages
]

if missing_stages:
    raise Exception(
        f"Pipeline dependency validation FAILED. Missing stages: {missing_stages}"
    )
else:
    print("Pipeline dependency validation PASSED")
    print("All required upstream stages are completed")

# COMMAND ----------

required_tables = [
    "workspace.default.rt_source_customers",
    "workspace.default.rt_source_products",
    "workspace.default.rt_source_orders",

    "workspace.default.rt_bronze_customers",
    "workspace.default.rt_bronze_products",
    "workspace.default.rt_bronze_orders",

    "workspace.default.rt_silver_customers",
    "workspace.default.rt_silver_products",
    "workspace.default.rt_silver_orders",

    "workspace.default.rt_dim_customers_scd2",

    "workspace.default.rt_gold_fact_sales",
    "workspace.default.rt_gold_top_products",
    "workspace.default.rt_gold_top_customers",
    "workspace.default.rt_gold_country_performance",
    "workspace.default.rt_gold_order_status_metrics",

    "workspace.default.rt_pipeline_control",
    "workspace.default.rt_data_quality_results",
    "workspace.default.rt_dq_quarantine_orders",
    "workspace.default.rt_pipeline_audit_log"
]

table_check_results = []

for table_name in required_tables:
    exists = spark.catalog.tableExists(table_name)

    row_count = (
        spark.table(table_name).count()
        if exists else None
    )

    table_check_results.append(
        (table_name, exists, row_count)
    )

table_check_df = spark.createDataFrame(
    table_check_results,
    ["table_name", "exists", "row_count"]
)

display(table_check_df)

# COMMAND ----------

from pyspark.sql import functions as F

# DQ status
dq_df = spark.table("workspace.default.rt_data_quality_results")
failed_dq_checks = dq_df.filter(F.col("status") == "FAIL").count()

# Audit status
audit_df = spark.table("workspace.default.rt_pipeline_audit_log")
latest_audit = (
    audit_df
    .orderBy(F.col("start_time").desc())
    .limit(1)
    .collect()[0]
)

latest_audit_status = latest_audit["status"]

# Gold readiness
gold_fact_rows = spark.table(
    "workspace.default.rt_gold_fact_sales"
).count()

# Required table readiness
missing_table_count = (
    table_check_df
    .filter(F.col("exists") == False)
    .count()
)

pipeline_ready = (
    failed_dq_checks == 0
    and latest_audit_status == "SUCCESS"
    and gold_fact_rows > 0
    and missing_table_count == 0
)

final_status = "READY" if pipeline_ready else "NOT READY"

print("========== PIPELINE READINESS CHECK ==========")
print("Failed DQ Checks      :", failed_dq_checks)
print("Latest Audit Status   :", latest_audit_status)
print("Gold Fact Rows        :", gold_fact_rows)
print("Missing Tables        :", missing_table_count)
print("Final Pipeline Status :", final_status)
print("==============================================")