# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Bronze Layer

source_customers = spark.table("workspace.default.rt_source_customers")
source_products = spark.table("workspace.default.rt_source_products")
source_orders = spark.table("workspace.default.rt_source_orders")

print("Source tables loaded successfully")
print("Customers:", source_customers.count())
print("Products :", source_products.count())
print("Orders   :", source_orders.count())


# COMMAND ----------

source_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_bronze_customers")

source_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_bronze_products")

source_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_bronze_orders")

print("Bronze Delta tables created successfully")

# COMMAND ----------

source_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_bronze_customers")

source_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_bronze_products")

source_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_bronze_orders")

print("Bronze Delta tables created successfully")

# COMMAND ----------

bronze_tables = {
    "rt_bronze_customers": "workspace.default.rt_bronze_customers",
    "rt_bronze_products": "workspace.default.rt_bronze_products",
    "rt_bronze_orders": "workspace.default.rt_bronze_orders"
}

for name, table in bronze_tables.items():
    df = spark.table(table)
    print(f"{name}: {df.count()} rows")
    df.printSchema()
    print("-" * 50)