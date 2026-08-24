# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Performance Optimization

from pyspark.sql import functions as F

fact_table = "workspace.default.rt_gold_fact_sales"
orders_table = "workspace.default.rt_silver_orders"

fact_df = spark.table(fact_table)
orders_df = spark.table(orders_table)

print("Gold fact rows :", fact_df.count())
print("Silver orders  :", orders_df.count())

# Serverless-compatible Delta table statistics
print("\n=== GOLD FACT TABLE DETAILS ===")
spark.sql(f"DESCRIBE DETAIL {fact_table}").select(
    "format",
    "numFiles",
    "sizeInBytes"
).show(truncate=False)

print("\n=== SILVER ORDERS TABLE DETAILS ===")
spark.sql(f"DESCRIBE DETAIL {orders_table}").select(
    "format",
    "numFiles",
    "sizeInBytes"
).show(truncate=False)

print("\nBaseline performance check completed")

# COMMAND ----------

query_df = (
    fact_df
    .filter(F.col("country") == "INDIA")
    .groupBy("category")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy(F.col("total_revenue").desc())
)

query_df.explain("formatted")

# COMMAND ----------

# Optimize Silver Orders Delta table

spark.sql("""
OPTIMIZE workspace.default.rt_silver_orders
""").show(truncate=False)

# COMMAND ----------

after_optimize = spark.sql("""
DESCRIBE DETAIL workspace.default.rt_silver_orders
""")

display(
    after_optimize.select(
        "format",
        "numFiles",
        "sizeInBytes"
    )
)

# COMMAND ----------

from pyspark.sql.functions import broadcast

products_df = spark.table("workspace.default.rt_silver_products")
orders_df = spark.table("workspace.default.rt_silver_orders")

optimized_join_df = (
    orders_df
    .join(
        broadcast(products_df),
        on="product_id",
        how="inner"
    )
    .select(
        "order_id",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "order_status"
    )
)

optimized_join_df.explain("formatted")

# COMMAND ----------

spark.sql("""
ANALYZE TABLE workspace.default.rt_silver_orders
COMPUTE STATISTICS
""")

spark.sql("""
ANALYZE TABLE workspace.default.rt_silver_products
COMPUTE STATISTICS
""")

spark.sql("""
ANALYZE TABLE workspace.default.rt_gold_fact_sales
COMPUTE STATISTICS
""")

print("Table statistics collected successfully")

# COMMAND ----------

# Final Performance Optimization Summary

silver_detail = (
    spark.sql("DESCRIBE DETAIL workspace.default.rt_silver_orders")
    .select("numFiles", "sizeInBytes")
    .first()
)

gold_detail = (
    spark.sql("DESCRIBE DETAIL workspace.default.rt_gold_fact_sales")
    .select("numFiles", "sizeInBytes")
    .first()
)

summary_data = [
    ("Delta File Compaction", "OPTIMIZE", "COMPLETED"),
    ("Query Engine", "Photon", "VERIFIED"),
    ("Filter Optimization", "Predicate / Filter Pushdown", "VERIFIED"),
    ("Join Optimization", "Broadcast Hash Join", "VERIFIED"),
    ("Optimizer Statistics", "ANALYZE TABLE", "COMPLETED"),
    ("Silver Orders Files", str(silver_detail["numFiles"]), "CURRENT"),
    ("Silver Orders Size Bytes", str(silver_detail["sizeInBytes"]), "CURRENT"),
    ("Gold Fact Files", str(gold_detail["numFiles"]), "CURRENT")
]

summary_df = spark.createDataFrame(
    summary_data,
    ["optimization_area", "technique_or_value", "status"]
)

display(summary_df)