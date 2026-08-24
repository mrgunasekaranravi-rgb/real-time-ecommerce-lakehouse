# Real-Time E-Commerce Lakehouse

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Delta-00ADD8)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Status](https://img.shields.io/badge/Project-Completed-success)

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
