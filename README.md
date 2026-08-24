# Real-Time E-Commerce Lakehouse

A production-style end-to-end Data Engineering project built using Databricks, PySpark, Delta Lake, and Medallion Architecture.

The project demonstrates how raw e-commerce data can be ingested, transformed, incrementally processed, modeled, validated, monitored, optimized, and orchestrated through a modern Lakehouse architecture.

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
