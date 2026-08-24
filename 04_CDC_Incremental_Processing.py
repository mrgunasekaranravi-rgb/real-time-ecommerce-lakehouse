# Databricks notebook source
# Real-Time E-Commerce Lakehouse
# CDC / Incremental Processing

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import datetime

target_table_name = "workspace.default.rt_silver_orders"

target_orders_df = spark.table(target_table_name)

print("Current target rows:", target_orders_df.count())
display(target_orders_df.orderBy("order_id"))

# COMMAND ----------

cdc_batch_data = [
    # Existing order -> UPDATE scenario
    (5007, 1002, 2005, 1, datetime(2026, 8, 24, 10, 5, 0), "Bengaluru", "COMPLETED"),

    # New orders -> INSERT scenarios
    (5011, 1006, 2007, 1, datetime(2026, 8, 24, 10, 10, 0), "Toronto", "COMPLETED"),
    (5012, 1008, 2008, 2, datetime(2026, 8, 24, 10, 15, 0), "Dubai", "PENDING")
]

cdc_batch_df = spark.createDataFrame(
    cdc_batch_data,
    target_orders_df.schema
)

cdc_batch_df = cdc_batch_df.withColumn(
    "processing_timestamp",
    F.current_timestamp()
)

display(cdc_batch_df)

# COMMAND ----------

from delta.tables import DeltaTable

target_delta = DeltaTable.forName(
    spark,
    target_table_name
)

merge_source_df = cdc_batch_df.drop("processing_timestamp")

(
    target_delta.alias("target")
    .merge(
        merge_source_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("CDC Delta MERGE completed successfully")

# COMMAND ----------

updated_orders_df = spark.table(target_table_name)

print("Rows after CDC MERGE:", updated_orders_df.count())

display(
    updated_orders_df
    .filter(F.col("order_id").isin(5007, 5011, 5012))
    .orderBy("order_id")
)

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

control_data = [
    Row(
        pipeline_name="rt_ecommerce_orders_cdc",
        target_table="workspace.default.rt_silver_orders",
        last_processed_timestamp=datetime(2026, 8, 24, 10, 15, 0),
        batch_status="SUCCESS",
        records_processed=3
    )
]

control_df = spark.createDataFrame(control_data)

control_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_pipeline_control")

display(
    spark.table("workspace.default.rt_pipeline_control")
)

# COMMAND ----------

before_count = spark.table(target_table_name).count()

(
    target_delta.alias("target")
    .merge(
        merge_source_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

after_count = spark.table(target_table_name).count()

print("Rows before re-run:", before_count)
print("Rows after re-run :", after_count)

if before_count == after_count:
    print("IDEMPOTENCY CHECK: PASS")
else:
    print("IDEMPOTENCY CHECK: FAIL")