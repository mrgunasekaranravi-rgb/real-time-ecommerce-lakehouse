# Databricks notebook source
from pyspark.sql import functions as F

validation_checks = []

# Bronze / Silver reconciliation
bronze_customers = spark.table("workspace.default.rt_bronze_customers")
silver_customers = spark.table("workspace.default.rt_silver_customers")

bronze_products = spark.table("workspace.default.rt_bronze_products")
silver_products = spark.table("workspace.default.rt_silver_products")

bronze_orders = spark.table("workspace.default.rt_bronze_orders")
silver_orders = spark.table("workspace.default.rt_silver_orders")

validation_checks.append(
    ("Customers Bronze→Silver", bronze_customers.count() == silver_customers.count())
)

validation_checks.append(
    ("Products Bronze→Silver", bronze_products.count() == silver_products.count())
)

# Orders count can increase because CDC inserted new orders
validation_checks.append(
    ("Orders CDC Applied", silver_orders.count() >= bronze_orders.count())
)

# Gold validation
gold_fact = spark.table("workspace.default.rt_gold_fact_sales")

validation_checks.append(
    ("Gold Fact Has Data", gold_fact.count() > 0)
)

validation_df = spark.createDataFrame(
    [
        (name, "PASS" if result else "FAIL")
        for name, result in validation_checks
    ],
    ["validation_check", "status"]
)

display(validation_df)

# COMMAND ----------

final_checks = []

# SCD Type 2 validation
scd2_df = spark.table("workspace.default.rt_dim_customers_scd2")

customer_1003_history = (
    scd2_df
    .filter(F.col("customer_id") == 1003)
)

scd2_history_count = customer_1003_history.count()

scd2_current_count = (
    customer_1003_history
    .filter(F.col("is_current") == True)
    .count()
)

final_checks.append(
    ("SCD2 History Created", scd2_history_count == 2)
)

final_checks.append(
    ("SCD2 Single Current Version", scd2_current_count == 1)
)

# Data Quality validation
dq_results = spark.table("workspace.default.rt_data_quality_results")
failed_dq_checks = dq_results.filter(F.col("status") == "FAIL").count()

final_checks.append(
    ("Data Quality Checks Passed", failed_dq_checks == 0)
)

# Quarantine validation
quarantine_count = spark.table(
    "workspace.default.rt_dq_quarantine_orders"
).count()

final_checks.append(
    ("Invalid Records Quarantined", quarantine_count == 4)
)

# Audit validation
audit_df = spark.table("workspace.default.rt_pipeline_audit_log")

successful_audit_runs = (
    audit_df
    .filter(F.col("status") == "SUCCESS")
    .count()
)

final_checks.append(
    ("Audit Logging Working", successful_audit_runs > 0)
)

final_validation_df = spark.createDataFrame(
    [
        (name, "PASS" if result else "FAIL")
        for name, result in final_checks
    ],
    ["validation_check", "status"]
)

display(final_validation_df)

# COMMAND ----------



# COMMAND ----------

all_checks_df = validation_df.unionByName(final_validation_df)

failed_checks = (
    all_checks_df
    .filter(F.col("status") == "FAIL")
    .count()
)

total_checks = all_checks_df.count()

project_status = (
    "COMPLETED / READY"
    if failed_checks == 0
    else "VALIDATION FAILED"
)

print("========== PROJECT VALIDATION SUMMARY ==========")
print("Total Validation Checks :", total_checks)
print("Failed Validation Checks:", failed_checks)
print("Project Status          :", project_status)
print("================================================")

display(all_checks_df)