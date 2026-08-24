# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Gold Layer

from pyspark.sql import functions as F

silver_customers = spark.table("workspace.default.rt_silver_customers")
silver_products = spark.table("workspace.default.rt_silver_products")
silver_orders = spark.table("workspace.default.rt_silver_orders")

print("Silver tables loaded successfully")
print("Customers:", silver_customers.count())
print("Products :", silver_products.count())
print("Orders   :", silver_orders.count())

# COMMAND ----------

gold_fact_sales = (
    silver_orders
    .join(
        silver_products,
        on="product_id",
        how="inner"
    )
    .join(
        silver_customers,
        on="customer_id",
        how="inner"
    )
    .withColumn(
        "revenue",
        F.col("quantity") * F.col("unit_price")
    )
    .select(
        "order_id",
        "order_timestamp",
        "customer_id",
        "customer_name",
        "country",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "revenue",
        "order_status"
    )
)

display(gold_fact_sales.orderBy("order_id"))

# COMMAND ----------

completed_fact_sales = (
    gold_fact_sales
    .filter(F.col("order_status") == "COMPLETED")
)

completed_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_gold_fact_sales")

print("Gold fact sales table created successfully")
print("Completed sales rows:", completed_fact_sales.count())

# COMMAND ----------

top_products = (
    completed_fact_sales
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("revenue").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(top_products)

# COMMAND ----------

top_customers = (
    completed_fact_sales
    .groupBy(
        "customer_id",
        "customer_name",
        "country"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(top_customers)

# COMMAND ----------

country_performance = (
    completed_fact_sales
    .groupBy("country")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("revenue").alias("total_revenue")
    )
    .orderBy(F.col("total_revenue").desc())
)

display(country_performance)

# COMMAND ----------

order_status_metrics = (
    gold_fact_sales
    .groupBy("order_status")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("revenue").alias("gross_order_value")
    )
    .orderBy(F.col("order_count").desc())
)

display(order_status_metrics)

# COMMAND ----------

top_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_gold_top_products")

top_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_gold_top_customers")

country_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_gold_country_performance")

order_status_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_gold_order_status_metrics")

print("Gold KPI tables created successfully")

# COMMAND ----------

gold_tables = [
    "workspace.default.rt_gold_fact_sales",
    "workspace.default.rt_gold_top_products",
    "workspace.default.rt_gold_top_customers",
    "workspace.default.rt_gold_country_performance",
    "workspace.default.rt_gold_order_status_metrics"
]

for table_name in gold_tables:
    df = spark.table(table_name)
    print(f"{table_name} -> {df.count()} rows")