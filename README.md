# Sales Data Cleaning & Analytics Pipeline

## Overview

This project demonstrates a structured data pipeline built in PostgreSQL that ingests raw sales data from CSV files, cleans and standardizes the data, and produces analytics-ready datasets for reporting and analysis.

The pipeline follows a multi-layered architecture commonly used in modern data warehouses.

Pipeline flow:

CSV Files → Raw Layer → Clean Layer → Business Layer

Each layer has a specific responsibility to ensure data quality, consistency, and usability for analytics.

## Project Architecture
### Raw Layer

The raw layer ingests external CSV data directly into the database.

**Responsibilities**

- Load source data from CSV files using PostgreSQL COPY

- Preserve original data structure

- Establish base relational structure

**Tables**

- raw_cust: raw customer data

- raw_sales: raw sales transactions

- raw_mark: raw marketing campaign data

**Procedure**

sales_raw_dataset()

This procedure:

- Drops existing raw tables

- Recreates the schema

- Loads CSV files into PostgreSQL tables

- Logs execution time for each load

### Clean Layer

The clean layer standardizes and validates raw data before it is used for analytics.

**Responsibilities**

- Apply data quality rules

- Standardize formats and values

- Flag missing or invalid records

- Derive additional attributes

**Tables**

- cl_cust: standardized customer data

- cl_sales: cleaned sales transactions

- cl_mark: cleaned marketing campaign data

Procedure

sales_clean_dataset()

Transformations include:

- Handling missing values

- Regex validation for product and quantity fields

- Derived metrics such as:
  -  total_sales
  -  transaction_type
  -  returned_units

- Data quality flags for monitoring issues

### Business Layer

The business layer produces analytics-ready datasets by combining and enriching clean data.

**Responsibilities**

- Integrate multiple datasets

- Apply business logic

- Create structures optimized for reporting and analysis

**Views**

- sales_analytics_view

This view joins:

- customer data

- sales transactions

- marketing campaign information

Additional analytics fields include:

- purchase_sequence – order of purchases per customer

- campaign attribution

- marketing performance metrics

- transaction classification

## Example Analytical Use Cases

The business layer enables analysis such as:

- Customer purchase behavior

- Sales performance by product

- Marketing campaign effectiveness

- Customer-level sales summaries

- Return rate analysis

## Technologies Used

- PostgreSQL

- SQL

- PL/pgSQL Stored Procedures

- CSV Data Sources

Key PostgreSQL features used:

- COPY for bulk ingestion

- window functions (ROW_NUMBER)

- common table expressions (CTEs)

- regex validation

- data quality flagging

## Project Structure
sales-data-pipeline
│
├── data
│   ├── customers.csv
│   ├── sales.csv
│   └── marketing.csv
│
├── sql
│   ├── sales_raw_dataset.sql
│   ├── sales_clean_dataset.sql
│   └── business_layer.sql
│
└── README.md

## How to Run the Pipeline

**Load raw data**

CALL sales_raw_dataset();

**Build clean layer**

CALL sales_clean_dataset();

**Create analytics views**

RUN business_layer.sql


## Key Concepts Demonstrated

This project showcases several data engineering practices:

- Layered data architecture

- ETL pipeline design

- Data quality validation

- Business logic modeling

- SQL performance-friendly ingestion

- Analytics-ready data modeling


## Future Improvements

Potential extensions for this project include:

- Incremental data loading

- Data quality audit tables

- Additional business metrics

- BI dashboard integration

- Scheduling pipeline execution
