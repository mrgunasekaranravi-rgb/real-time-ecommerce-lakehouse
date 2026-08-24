# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import random

print("Real-Time E-Commerce Lakehouse setup started")

# COMMAND ----------

customers_data = [
    (1001, "Arun Kumar", "arun.kumar@email.com", "Chennai", "India", date(2025, 1, 10)),
    (1002, "Priya Shah", "priya.shah@email.com", "Bengaluru", "India", date(2025, 1, 15)),
    (1003, "James Wilson", "james.wilson@email.com", "London", "UK", date(2025, 2, 2)),
    (1004, "Emma Brown", "emma.brown@email.com", "Manchester", "UK", date(2025, 2, 14)),
    (1005, "Daniel Smith", "daniel.smith@email.com", "New York", "USA", date(2025, 3, 1)),
    (1006, "Sophie Taylor", "sophie.taylor@email.com", "Toronto", "Canada", date(2025, 3, 12)),
    (1007, "Michael Jones", "michael.jones@email.com", "Sydney", "Australia", date(2025, 3, 18)),
    (1008, "Olivia Davis", "olivia.davis@email.com", "Dubai", "UAE", date(2025, 4, 5))
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

display(customers_df)

# COMMAND ----------

products_data = [
    (2001, "Laptop Pro 15", "Electronics", 85000.00),
    (2002, "Wireless Mouse", "Electronics", 1500.00),
    (2003, "Mechanical Keyboard", "Electronics", 4500.00),
    (2004, "Office Chair", "Furniture", 12000.00),
    (2005, "Standing Desk", "Furniture", 25000.00),
    (2006, "Smartphone X", "Electronics", 65000.00),
    (2007, "Noise Cancelling Headphones", "Electronics", 18000.00),
    (2008, "Coffee Machine", "Home Appliances", 9500.00)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False)
])

products_df = spark.createDataFrame(products_data, products_schema)

display(products_df)

# COMMAND ----------

from datetime import datetime

orders_data = [
    (5001, 1001, 2001, 1, datetime(2026, 8, 24, 9, 5, 10),  "Chennai",   "COMPLETED"),
    (5002, 1002, 2002, 2, datetime(2026, 8, 24, 9, 10, 25), "Bengaluru", "COMPLETED"),
    (5003, 1003, 2006, 1, datetime(2026, 8, 24, 9, 15, 40), "Hyderabad", "COMPLETED"),
    (5004, 1004, 2004, 1, datetime(2026, 8, 24, 9, 20, 15), "Chennai",   "COMPLETED"),
    (5005, 1005, 2007, 2, datetime(2026, 8, 24, 9, 25, 30), "Mumbai",    "COMPLETED"),
    (5006, 1001, 2003, 1, datetime(2026, 8, 24, 9, 30, 45), "Chennai",   "COMPLETED"),
    (5007, 1002, 2005, 1, datetime(2026, 8, 24, 9, 35, 20), "Bengaluru", "PENDING"),
    (5008, 1003, 2008, 2, datetime(2026, 8, 24, 9, 40, 10), "Hyderabad", "COMPLETED"),
    (5009, 1004, 2002, 3, datetime(2026, 8, 24, 9, 45, 35), "Chennai",   "CANCELLED"),
    (5010, 1005, 2001, 1, datetime(2026, 8, 24, 9, 50, 50), "Mumbai",    "COMPLETED")
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("city", StringType(), False),
    StructField("order_status", StringType(), False)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

display(orders_df)

# COMMAND ----------

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_source_customers")

products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_source_products")

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_source_orders")

print("Source Delta tables created successfully")