# Databricks notebook source
# Real-Time E-Commerce Lakehouse - Data Quality Framework

from pyspark.sql import functions as F
from datetime import datetime
import uuid

dq_run_id = str(uuid.uuid4())
dq_run_time = datetime.now()

silver_customers = spark.table("workspace.default.rt_silver_customers")
silver_products = spark.table("workspace.default.rt_silver_products")
silver_orders = spark.table("workspace.default.rt_silver_orders")

print("DQ Run ID   :", dq_run_id)
print("DQ Run Time :", dq_run_time)
print("Customers   :", silver_customers.count())
print("Products    :", silver_products.count())
print("Orders      :", silver_orders.count())
print("DQ framework initialized successfully")

# COMMAND ----------

def null_check(df, table_name, columns):
    results = []

    total_rows = df.count()

    for column_name in columns:
        failed_rows = df.filter(F.col(column_name).isNull()).count()

        results.append({
            "table_name": table_name,
            "rule_type": "NULL_CHECK",
            "rule_name": f"{column_name}_not_null",
            "total_rows": total_rows,
            "failed_rows": failed_rows,
            "status": "PASS" if failed_rows == 0 else "FAIL"
        })

    return results


def duplicate_check(df, table_name, key_columns):
    duplicate_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return {
        "table_name": table_name,
        "rule_type": "DUPLICATE_CHECK",
        "rule_name": "_".join(key_columns) + "_unique",
        "total_rows": df.count(),
        "failed_rows": duplicate_groups,
        "status": "PASS" if duplicate_groups == 0 else "FAIL"
    }


def business_rule_check(df, table_name, rule_name, failure_condition):
    failed_rows = df.filter(failure_condition).count()

    return {
        "table_name": table_name,
        "rule_type": "BUSINESS_RULE",
        "rule_name": rule_name,
        "total_rows": df.count(),
        "failed_rows": failed_rows,
        "status": "PASS" if failed_rows == 0 else "FAIL"
    }

print("Reusable DQ functions created successfully")

# COMMAND ----------

def referential_integrity_check(
    child_df,
    parent_df,
    child_key,
    parent_key,
    child_table_name,
    rule_name
):
    invalid_rows = (
        child_df.alias("child")
        .join(
            parent_df.alias("parent"),
            F.col(f"child.{child_key}") == F.col(f"parent.{parent_key}"),
            "left_anti"
        )
        .count()
    )

    return {
        "table_name": child_table_name,
        "rule_type": "REFERENTIAL_INTEGRITY",
        "rule_name": rule_name,
        "total_rows": child_df.count(),
        "failed_rows": invalid_rows,
        "status": "PASS" if invalid_rows == 0 else "FAIL"
    }

print("Referential integrity check function created successfully")

# COMMAND ----------

from pyspark.sql import functions as F

silver_customers = spark.table("workspace.default.rt_silver_customers")
silver_products = spark.table("workspace.default.rt_silver_products")
silver_orders = spark.table("workspace.default.rt_silver_orders")

print("Customers:", silver_customers.count())
print("Products :", silver_products.count())
print("Orders   :", silver_orders.count())

# COMMAND ----------

dq_results = []

# Customers null checks
dq_results.extend(
    null_check(
        silver_customers,
        "rt_silver_customers",
        ["customer_id", "customer_name", "email"]
    )
)

# Products null checks
dq_results.extend(
    null_check(
        silver_products,
        "rt_silver_products",
        ["product_id", "product_name", "unit_price"]
    )
)

# Orders null checks
dq_results.extend(
    null_check(
        silver_orders,
        "rt_silver_orders",
        ["order_id", "customer_id", "product_id", "quantity", "order_status"]
    )
)

# Duplicate checks
dq_results.append(
    duplicate_check(
        silver_customers,
        "rt_silver_customers",
        ["customer_id"]
    )
)

dq_results.append(
    duplicate_check(
        silver_products,
        "rt_silver_products",
        ["product_id"]
    )
)

dq_results.append(
    duplicate_check(
        silver_orders,
        "rt_silver_orders",
        ["order_id"]
    )
)

# Business rules
dq_results.append(
    business_rule_check(
        silver_orders,
        "rt_silver_orders",
        "quantity_must_be_positive",
        F.col("quantity") <= 0
    )
)

dq_results.append(
    business_rule_check(
        silver_orders,
        "rt_silver_orders",
        "valid_order_status",
        ~F.col("order_status").isin(["COMPLETED", "PENDING", "CANCELLED"])
    )
)

dq_results.append(
    business_rule_check(
        silver_products,
        "rt_silver_products",
        "unit_price_must_be_positive",
        F.col("unit_price") <= 0
    )
)

# Referential integrity
dq_results.append(
    referential_integrity_check(
        silver_orders,
        silver_customers,
        "customer_id",
        "customer_id",
        "rt_silver_orders",
        "customer_id_must_exist"
    )
)

dq_results.append(
    referential_integrity_check(
        silver_orders,
        silver_products,
        "product_id",
        "product_id",
        "rt_silver_orders",
        "product_id_must_exist"
    )
)

dq_results_df = spark.createDataFrame(dq_results)

display(
    dq_results_df.orderBy(
        "table_name",
        "rule_type",
        "rule_name"
    )
)

# COMMAND ----------

from datetime import datetime

bad_orders_data = [
    # Invalid quantity
    (9001, 1001, 2001, 0, datetime(2026, 8, 24, 14, 45, 0), "Chennai", "COMPLETED"),

    # Invalid customer reference
    (9002, 9999, 2002, 1, datetime(2026, 8, 24, 14, 46, 0), "Chennai", "COMPLETED"),

    # Invalid product reference
    (9003, 1002, 9999, 1, datetime(2026, 8, 24, 14, 47, 0), "Bengaluru", "COMPLETED"),

    # Invalid order status
    (9004, 1003, 2003, 1, datetime(2026, 8, 24, 14, 48, 0), "London", "UNKNOWN")
]

bad_orders_df = spark.createDataFrame(
    bad_orders_data,
    silver_orders.schema
)

display(bad_orders_df)

# COMMAND ----------

customer_ids = silver_customers.select("customer_id").distinct()
product_ids = silver_products.select("product_id").distinct()

quarantine_orders = (
    bad_orders_df
    .join(
        customer_ids.withColumnRenamed("customer_id", "valid_customer_id"),
        bad_orders_df.customer_id == F.col("valid_customer_id"),
        "left"
    )
    .join(
        product_ids.withColumnRenamed("product_id", "valid_product_id"),
        bad_orders_df.product_id == F.col("valid_product_id"),
        "left"
    )
    .withColumn(
        "dq_error_reason",
        F.concat_ws(
            " | ",
            F.when(F.col("quantity") <= 0, F.lit("INVALID_QUANTITY")),
            F.when(F.col("valid_customer_id").isNull(), F.lit("INVALID_CUSTOMER")),
            F.when(F.col("valid_product_id").isNull(), F.lit("INVALID_PRODUCT")),
            F.when(
                ~F.col("order_status").isin(["COMPLETED", "PENDING", "CANCELLED"]),
                F.lit("INVALID_STATUS")
            )
        )
    )
    .filter(F.col("dq_error_reason") != "")
    .drop("valid_customer_id", "valid_product_id")
)

display(quarantine_orders)

# COMMAND ----------

import uuid

# Generate unique ID for this Data Quality run
dq_run_id = str(uuid.uuid4())

# Save quarantine records
quarantine_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_dq_quarantine_orders")

# Add audit information to DQ results
dq_results_with_run = (
    dq_results_df
    .withColumn("dq_run_id", F.lit(dq_run_id))
    .withColumn("dq_run_time", F.current_timestamp())
)

# Save Data Quality results
dq_results_with_run.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rt_data_quality_results")

# Validation
print("DQ tables saved successfully")
print("DQ Run ID:", dq_run_id)
print(
    "Quarantine rows:",
    spark.table("workspace.default.rt_dq_quarantine_orders").count()
)
print(
    "DQ result rows:",
    spark.table("workspace.default.rt_data_quality_results").count()
)


# COMMAND ----------

dq_summary = (
    spark.table("workspace.default.rt_data_quality_results")
    .groupBy("status")
    .agg(
        F.count("*").alias("rule_count"),
        F.sum("failed_rows").alias("total_failed_rows")
    )
    .orderBy("status")
)

display(dq_summary)

# COMMAND ----------

print("========== DATA QUALITY FRAMEWORK SUMMARY ==========")
print("DQ Rules Executed   :", spark.table("workspace.default.rt_data_quality_results").count())
print("Failed DQ Checks    :", spark.table("workspace.default.rt_data_quality_results")
      .filter(F.col("status") == "FAIL").count())
print("Quarantined Records :", spark.table("workspace.default.rt_dq_quarantine_orders").count())
print("Framework Status    : COMPLETED")
print("====================================================")