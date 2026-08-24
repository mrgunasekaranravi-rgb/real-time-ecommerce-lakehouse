# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Silver Layer

from pyspark.sql import functions as F

bronze_customers = spark.table("workspace.default.rt_bronze_customers")
bronze_products = spark.table("workspace.default.rt_bronze_products")
bronze_orders = spark.table("workspace.default.rt_bronze_orders")

print("Bronze tables loaded successfully")
print("Customers:", bronze_customers.count())
print("Products :", bronze_products.count())
print("Orders   :", bronze_orders.count())

# COMMAND ----------

silver_customers = (
    bronze_customers
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
    .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
)

display(silver_customers)

# COMMAND ----------

silver_products = (
    bronze_products
    .dropDuplicates(["product_id"])
    .filter(
        F.col("product_id").isNotNull() &
        F.col("product_name").isNotNull() &
        F.col("unit_price").isNotNull() &
        (F.col("unit_price") > 0)
    )
    .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
)

display(silver_products)

# COMMAND ----------

valid_statuses = ["COMPLETED", "PENDING", "CANCELLED"]

silver_orders = (
    bronze_orders
    .dropDuplicates(["order_id"])
    .filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("quantity").isNotNull() &
        (F.col("quantity") > 0) &
        F.col("order_status").isin(valid_statuses)
    )
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
)

display(silver_orders)

# COMMAND ----------

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_silver_customers")

silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_silver_products")

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_silver_orders")

print("Silver Delta tables created successfully")


# COMMAND ----------

# Silver Layer Validation & Reconciliation

validation_results = []

tables = [
    ("Customers", bronze_customers, silver_customers, "customer_id"),
    ("Products", bronze_products, silver_products, "product_id"),
    ("Orders", bronze_orders, silver_orders, "order_id")
]

for name, bronze_df, silver_df, key_col in tables:
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    duplicate_count = (
        silver_df.groupBy(key_col)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    null_key_count = silver_df.filter(F.col(key_col).isNull()).count()

    status = (
        "PASS"
        if silver_count <= bronze_count
        and duplicate_count == 0
        and null_key_count == 0
        else "FAIL"
    )

    validation_results.append(
        (name, bronze_count, silver_count,
         duplicate_count, null_key_count, status)
    )

validation_df = spark.createDataFrame(
    validation_results,
    [
        "dataset",
        "bronze_rows",
        "silver_rows",
        "duplicate_keys",
        "null_keys",
        "validation_status"
    ]
)

display(validation_df)