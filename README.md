# Real-Time E-Commerce Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Delta-00ADD8)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Status](https://img.shields.io/badge/Project-Completed-success)
[![Python CI](https://github.com/mrgunasekaranravi-rgb/real-time-ecommerce-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/mrgunasekaranravi-rgb/real-time-ecommerce-lakehouse/actions/workflows/ci.yml)

A production-style end-to-end Data Engineering project built using Databricks, PySpark, Delta Lake, and Medallion Architecture.

The project demonstrates how raw e-commerce data can be ingested, transformed, incrementally processed, modeled, validated, monitored, optimized, and orchestrated through a modern Lakehouse architecture.

## Business Use Case

An e-commerce company receives continuous data from orders, customers, products, and transactions. The goal of this project is to build a scalable Lakehouse pipeline that transforms raw operational data into reliable, analytics-ready datasets.

The solution supports incremental data processing, historical customer tracking, data quality validation, business aggregations, audit monitoring, and optimized analytical workloads.

## Project Highlights

- Built an end-to-end Lakehouse pipeline using Databricks, PySpark, and Delta Lake
- Implemented Bronze, Silver, and Gold layers using Medallion Architecture
- Designed CDC and incremental data processing for efficient data updates
- Implemented SCD Type 2 for historical dimension tracking
- Built business-ready Gold layer aggregations for analytics
- Developed reusable data quality checks and quarantine handling
- Added audit logging and pipeline monitoring for observability
- Applied Delta Lake and Spark performance optimization techniques
- Created end-to-end pipeline orchestration and final validation

## Architecture

![Real-Time E-Commerce Lakehouse Architecture](architecture.png)

Source Data
→ Bronze Layer
→ Silver Layer
→ CDC / Incremental Processing
→ SCD Type 2 Dimensions
→ Gold Layer
→ Data Quality
→ Audit & Monitoring
→ Performance Optimization
→ Pipeline Orchestration

## End-to-End Data Flow

The pipeline processes e-commerce data through multiple Lakehouse stages:

1. **Source Generation** — Simulates raw e-commerce data for customers, products, orders, and transactions.
2. **Bronze Layer** — Ingests raw source data into Delta tables while preserving the original structure.
3. **Silver Layer** — Cleans, standardizes, validates, and deduplicates the Bronze data.
4. **CDC & Incremental Processing** — Processes only new or changed records instead of reprocessing the complete dataset.
5. **SCD Type 2** — Maintains historical changes in dimension data for analytical tracking.
6. **Gold Layer** — Creates business-ready dimensions, facts, and aggregated datasets.
7. **Data Quality** — Validates nulls, duplicates, business rules, and referential integrity while quarantining invalid records.
8. **Audit & Monitoring** — Captures pipeline execution status, row counts, duration, and error information.
9. **Performance Optimization** — Applies Delta Lake and Spark optimization techniques for efficient processing.
10. **Pipeline Orchestration** — Coordinates the complete end-to-end execution flow.
11. **Project Validation** — Performs final validation of the Lakehouse pipeline and generated datasets.

## Project Outcomes

This project demonstrates the ability to design and implement a production-style Lakehouse data engineering solution with:

- Scalable Bronze, Silver, and Gold data layers
- Incremental processing using CDC patterns
- Historical dimension tracking using SCD Type 2
- Reusable data quality validation and quarantine handling
- Business-ready fact, dimension, and aggregation datasets
- Pipeline audit logging and operational monitoring
- Delta Lake and Spark performance optimization
- End-to-end pipeline orchestration and validation
  
## Tech Stack

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Spark SQL
- Python
- Git
- GitHub

## Key Features

- Medallion Architecture (Bronze, Silver, Gold)
- Raw data ingestion
- Data cleansing and transformation
- Delta Lake tables
- CDC and incremental processing
- MERGE-based upserts
- Slowly Changing Dimension Type 2 (SCD Type 2)
- Business-level Gold aggregations
- Data quality validation framework
- Quarantine handling for invalid records
- Referential integrity validation
- Duplicate and null checks
- Audit logging
- Pipeline monitoring
- Performance optimization
- Delta table optimization
- End-to-end pipeline orchestration
- Final project validation

## Project Notebooks

| # | Notebook | Purpose |
|---|---|---|
| 01 | Setup and Source Generation | Environment setup and source data generation |
| 02 | Bronze Ingestion | Raw data ingestion into the Bronze layer |
| 03 | Silver Transformation | Data cleaning and Silver layer transformations |
| 04 | CDC Incremental Processing | Incremental data processing and CDC |
| 05 | SCD Type 2 Dimensions | Historical dimension tracking using SCD Type 2 |
| 06 | Gold Business Aggregations | Business-ready Gold layer aggregations |
| 07 | Data Quality Framework | Data quality rules, validation and quarantine |
| 08 | Audit Monitoring | Pipeline audit logging and monitoring |
| 09 | Performance Optimization | Delta and Spark performance optimization |
| 10 | Pipeline Orchestration | End-to-end pipeline execution |
| 11 | Project Validation | Final validation of the Lakehouse pipeline |

## Medallion Architecture

### Bronze Layer
Stores raw source data with minimal transformation and preserves the original data for downstream processing.

### Silver Layer
Cleans, standardizes, validates, deduplicates, and prepares data for analytical processing.

### Gold Layer
Provides business-ready datasets and aggregated metrics suitable for analytics and reporting.

## Data Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

- ETL / ELT
- Lakehouse Architecture
- Delta Lake
- Medallion Architecture
- Incremental Data Processing
- Change Data Capture (CDC)
- SCD Type 2
- Data Quality
- Audit Logging
- Pipeline Monitoring
- Spark Query Optimization
- Data Validation
- Pipeline Orchestration

## Data Quality

The project contains a reusable Data Quality framework covering checks such as:

- Null validation
- Duplicate validation
- Business-rule validation
- Referential integrity validation
- Invalid-record quarantine

Data quality results are persisted for monitoring and auditing.

## Monitoring

Pipeline execution information is captured through an audit framework including:

- Run ID
- Pipeline name
- Source and target
- Start time
- End time
- Rows processed
- Status
- Duration
- Error information

## Performance Optimization

Performance techniques demonstrated include:

- Delta table optimization
- Query-plan analysis
- File optimization
- Efficient filtering and aggregation
- Spark execution analysis

## Pipeline Flow

```text
Source
   |
   v
Bronze
   |
   v
Silver
   |
   +----> CDC / Incremental Processing
   |
   +----> SCD Type 2
   |
   v
Gold
   |
   v
Data Quality
   |
   v
Audit & Monitoring
   |
   v
Performance Optimization
   |
   v
Validation
```

## How to Run

This project is designed to run in a Databricks environment using PySpark and Delta Lake.

### Prerequisites

- Databricks workspace
- Apache Spark / PySpark
- Delta Lake
- Python 3.x

### Execution Order

Run the project notebooks in the following sequence:

1. `01_Setup_and_Source_Generation.py`
2. `02_Bronze_Ingestion.py`
3. `03_Silver_Transformation.py`
4. `04_CDC_Incremental_Processing.py`
5. `05_SCD_Type2_Dimensions.py`
6. `06_Gold_Business_Aggregations.py`
7. `07_Data_Quality_Framework.py`
8. `08_Audit_Monitoring.py`
9. `09_Performance_Optimization.py`
10. `10_Pipeline_Orchestration.py`
11. `11_Project_Validation.py`

The first notebook initializes the project environment and generates the e-commerce source datasets. Subsequent notebooks progressively build the Bronze, Silver, CDC/SCD, Gold, Data Quality, Monitoring, Optimization, and Orchestration layers.

For a complete end-to-end execution, run `10_Pipeline_Orchestration.py` after the required setup has been completed, then use `11_Project_Validation.py` to validate the final Lakehouse datasets and pipeline outputs.


## Execution Evidence

The following screenshots demonstrate successful execution of the project in Databricks.

### Source Data Generation

Customer source data is generated using PySpark with an explicit schema and loaded into a Spark DataFrame.

![Source Customer Generation](screenshots/01_source_customer_generation.png)

### Delta Lake Source Tables

The generated source datasets are persisted as Delta Lake tables in Databricks for downstream Bronze layer ingestion and processing.

![Delta Table Creation](screenshots/02_delta_table_creation.png)

### Bronze Layer

Raw source data is ingested into the Bronze layer and validated before downstream processing.

![Bronze Ingestion](screenshots/03_bronze_ingestion.png)

![Bronze Validation](screenshots/04_bronze_validation.png)

### Silver Layer

Bronze data is cleaned, standardized, deduplicated, and transformed into validated Silver datasets.

![Silver Transformation](screenshots/05_silver_transformation.png)

![Silver Validation](screenshots/06_silver_validation.png)

### CDC & Incremental Processing

Delta Lake MERGE is used to process incremental changes efficiently while supporting idempotent pipeline execution.

![CDC Delta Merge](screenshots/07_cdc_delta_merge.png)

![CDC Merge Results](screenshots/08_cdc_merge_results.png)

![CDC Idempotency Validation](screenshots/09_cdc_idempotency_validation.png)

### SCD Type 2 — Historical Dimension Tracking

Slowly Changing Dimension Type 2 is implemented to preserve historical customer changes while maintaining the current version of each dimension record.

![SCD Type 2 Initialization](screenshots/10_scd2_dimension_initialization.png)

![SCD Type 2 Version Update](screenshots/11_scd2_version_update.png)

![SCD Type 2 Customer History](screenshots/12_scd2_customer_history.png)

### Gold Layer — Business Analytics

Curated Gold datasets provide business-ready fact tables, aggregations, and analytical outputs.

![Gold Fact Sales](screenshots/13_gold_fact_sales.png)

![Gold Top Products](screenshots/14_gold_top_products.png)

![Gold Layer Validation](screenshots/15_gold_layer_validation.png)

### Data Quality Framework

Reusable data quality rules validate pipeline outputs and identify invalid records for quarantine instead of allowing bad data into downstream analytical datasets.

![Data Quality Rules - Part 1](screenshots/16_data_quality_rules1.png)

![Data Quality Rules - Part 2](screenshots/16_data_quality_rules2.png)

![Data Quality Rules - Part 3](screenshots/16_data_quality_rules3.png)

![Invalid Records Quarantine](screenshots/17_invalid_records_quarantine.png)

![Data Quality Summary](screenshots/18_data_quality_summary.png)

### Audit & Monitoring

Operational observability captures pipeline execution metrics, audit history, and monitoring information for production-style tracking.

![Audit Run Metrics](screenshots/19_audit_run_metrics.png)

![Pipeline Audit History](screenshots/20_pipeline_audit_history.png)

![Monitoring Summary](screenshots/21_monitoring_summary.png)

### Performance Optimization

Spark and Delta Lake optimization techniques are applied to improve query and storage performance.

![Photon Query Optimization](screenshots/22_photon_query_optimization.png)

![Delta Optimize Compaction](screenshots/23_delta_optimize_compaction.png)

![Performance Optimization Summary](screenshots/24_performance_optimization_summary.png)

### Pipeline Orchestration & Readiness

Pipeline stages are registered and validated to confirm that the complete Lakehouse workflow is ready for end-to-end execution.

![Pipeline Stage Registry](screenshots/25_pipeline_stage_registry.png)

![Pipeline Table Readiness](screenshots/26_pipeline_table_readiness.png)

![Pipeline Readiness Status](screenshots/27_pipeline_readiness_status.png)

### Final Project Validation

Final reconciliation and technical validation confirm consistency across the Lakehouse layers and verify the completed project outputs.

![Bronze Silver Reconciliation](screenshots/28_bronze_silver_reconciliation.png)

![Final Technical Validation](screenshots/29_final_technical_validation.png)

![Project Validation Summary](screenshots/30_project_validation_summary.png)
