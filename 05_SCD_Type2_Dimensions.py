# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Read current Silver customer data
customers_df = spark.table("rt_silver_customers")

print("Silver customer rows:", customers_df.count())
display(customers_df)

# COMMAND ----------

from pyspark.sql import functions as F

scd2_initial_df = (
    customers_df
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

scd2_initial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_dim_customers_scd2")

print("SCD Type 2 customer dimension initialized successfully")

display(
    spark.table("workspace.default.rt_dim_customers_scd2")
    .orderBy("customer_id")
)

# COMMAND ----------

from datetime import date

customer_change_data = [
    (
        1003,
        "James Wilson",
        "james.wilson@email.com",
        "Birmingham",   # London -> Birmingham change
        "UK",
        date(2025, 2, 2)
    )
]

customer_change_df = spark.createDataFrame(
    customer_change_data,
    customers_df.schema
)

display(customer_change_df)

# COMMAND ----------

from delta.tables import DeltaTable

scd2_table = DeltaTable.forName(
    spark,
    "workspace.default.rt_dim_customers_scd2"
)

scd2_table.update(
    condition="""
        customer_id = 1003
        AND is_current = true
    """,
    set={
        "effective_to": "current_timestamp()",
        "is_current": "false"
    }
)

print("Old customer version closed successfully")

# COMMAND ----------

new_customer_version_df = (
    customer_change_df
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

new_customer_version_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("workspace.default.rt_dim_customers_scd2")

print("New customer version inserted successfully")

# COMMAND ----------

customer_history_df = (
    spark.table("workspace.default.rt_dim_customers_scd2")
    .filter(F.col("customer_id") == 1003)
    .select(
        "customer_id",
        "customer_name",
        "city",
        "effective_from",
        "effective_to",
        "is_current"
    )
    .orderBy("effective_from")
)

display(customer_history_df)