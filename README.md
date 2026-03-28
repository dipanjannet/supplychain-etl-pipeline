Metadata-Driven Multi-Source Ingestion Pipeline in Azure Data Factory
A scalable, maintainable, and event-driven ingestion framework using Azure Data Factory (ADF) that supports multiple source types (CSV, REST API, Azure SQL) through a central configuration table.
Project Overview
This solution implements a metadata-driven ingestion pipeline for CCBJI's centralized analytics platform. Instead of creating separate pipelines for each source, we use one generic pipeline that reads configuration from a SQL table and dynamically processes different sources.
Key Features

Single Generic Pipeline – Handles CSV, REST API, and Azure SQL
Event-Driven for CSV – Triggers automatically when new files land
Support for Multiple Tables per Source – sap/sales, sap/orders, etc.
Medallion Architecture Ready – Bronze layer with clean partitioning
Incremental Load Support – For Azure SQL using watermark pattern
Config-Driven Onboarding – Add new sources/tables by inserting one row

Architecture Diagram

High-Level Flow:

Event Trigger → New CSV file arrives → Pipeline runs
Lookup → Reads active configurations from SQL table
ForEach → Processes each config row
If Conditions → Routes to appropriate Copy activity based on SourceType
Bronze Layer → Writes Parquet files with proper partitioning

```sql
Folder Structure
Bashscm-datalake/
├── ingestion/
│   ├── sap/
│   │   └── sales/
│   │       └── sales_transactions_20260328.csv
│   ├── azure_sql/
│   │   └── customer/
│   └── rest_api/
│       └── product/
└── storage/                  # Bronze Layer
    ├── sap/
    │   └── sales/
    │       └── sales_20260328.parquet
    ├── azure_sql/
    │   └── customer/
    └── rest_api/
        └── product/
Configuration Table (dbo.IngestionConfig)
SQLCREATE TABLE dbo.IngestionConfig (
    ConfigId            INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem        VARCHAR(50)  NOT NULL,     -- 'sap', 'azure_sql', 'rest_api'
    TableName           VARCHAR(100) NOT NULL,     -- 'sales', 'customer', 'product'
    SourceType          VARCHAR(20)  NOT NULL,     -- 'CSV', 'REST_API', 'AZURE_SQL'
    IsActive            BIT DEFAULT 1,

    LinkedServiceName   VARCHAR(100),
    SourceDatasetName   VARCHAR(100),
    SourceParams        NVARCHAR(MAX),            -- JSON for extra params

    WatermarkColumn     VARCHAR(100),             -- For incremental SQL
    WatermarkValue      DATETIME2(0) NULL,

    LastProcessedDate   DATE NULL,
    CreatedDate         DATETIME2 DEFAULT GETUTCDATE(),
    UpdatedDate         DATETIME2 DEFAULT GETUTCDATE()
);
Sample Configuration Data
SQL-- SAP Sales CSV
INSERT INTO dbo.IngestionConfig (SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName)
VALUES ('sap', 'sales', 'CSV', 'ls_ADLS_Landing', 'ds_Generic_CSV');

-- Customer from Azure SQL (Incremental)
INSERT INTO dbo.IngestionConfig (SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName, 
                                 SourceParams, WatermarkColumn)
VALUES ('azure_sql', 'customer', 'AZURE_SQL', 'ls_AzureSQL_Source', 'ds_Generic_AzureSQL', 
        '{"tableName":"dbo.Customer"}', 'last_modified');
Pipeline Design (pl_Generic_Ingestion)
The pipeline uses:

Lookup → Reads active configs
ForEach → Loops through each config
If Conditions → Routes based on SourceType (more stable than Switch)
Copy Activity → Performs the actual data movement
Stored Procedure → Updates watermark for SQL sources

Core Logic Walkthrough

Event Trigger fires when a new CSV file lands
Lookup_ActiveConfigs fetches all active rows
ForEach iterates over each row
Three IfCondition activities evaluate SourceType:
CSV → Uses triggerFileName from event
REST_API → Full load from API
AZURE_SQL → Full load first time, incremental thereafter

Data lands in storage/{SourceSystem}/{TableName}/ as Parquet
Watermark is updated only for successful Azure SQL loads

Edge Cases Handled

First run for SQL → Watermark is NULL → Full load
No active configs → ForEach receives empty array (safe)
Event Trigger with wrong file → Only matching config row processes
Multiple tables per source → Fully supported via TableName
Failed Copy → Watermark not updated (thanks to dependency)
Null/empty triggerFileName → Handled gracefully
Invalid JSON in SourceParams → Expressions use safe json() handling

Setup Instructions

Create Linked Services (ls_ADLS_Landing, ls_ADLS_Bronze, etc.)
Create Datasets (ds_Generic_CSV, ds_Generic_HTTP, ds_Generic_AzureSQL, ds_Bronze_Parquet)
Create the IngestionConfig table and insert sample data
Deploy the pipeline pl_Generic_Ingestion
Create Blob Events Trigger and map triggerFileName → @triggerBody().fileName

Future Enhancements

Add logging table for audit
Silver/Gold layer processing pipeline
Data quality checks using Mapping Data Flows
Purview integration for lineage
Retry + alerting using Logic Apps









# supplychain-etl-pipeline
End-to-end Azure Data Factory solution for centralized analytics platform. Orchestrates daily incremental ingestion from Azure SQL, REST APIs, and CSV files into a medallion architecture (Bronze → Silver → Gold) to enable Sales and Supply Chain reporting.


## Low Level Design

### 1. Objective
This pipeline implements an **incremental (delta) load** for the `Customer` fact table from Azure SQL Database to Azure Data Lake Storage Gen2 (ADLS Gen2) in **Parquet** format.

- **First run**: Performs a full load of all records.
- **Subsequent runs**: Loads only new or modified records based on the `last_modified` column (delta/incremental load).
- Goal: Minimize data movement, reduce cost, and ensure reliable daily/periodic ingestion while keeping the solution simple and maintainable on the Azure Free tier.

### 2. Architecture Overview

**Source**: Azure SQL Database (`Customer` table)  
**Sink**: ADLS Gen2 (Parquet files with Hive-style partitioning)  
**Orchestration**: Azure Data Factory (ADF) v2

**Key Components**:
- Watermark Control Table (`WatermarkControl`) in Azure SQL for tracking the last processed `last_modified` value.
- ADF Pipeline with Lookup + Copy + Script activities.
- Dynamic partitioning in the sink for better query performance in downstream tools (Spark, Synapse, Databricks, etc.).

### 3. Data Model

#### Source Table (`Customer`)
- Contains a `last_modified` column (DATETIME2) used as the watermark.
- Assumed to be append-mostly (fact table behavior).

#### Control Table (`WatermarkControl`)
```sql
CREATE TABLE WatermarkControl (
    TableName       NVARCHAR(100) PRIMARY KEY,
    WatermarkValue  DATETIME2(7) NULL
);

### 4. Visual Flow Summary
Trigger (Event or Schedule)
        ↓
Lookup_ActiveConfigs   ← Reads all active sources from config table
        ↓
ForEach_Sources (loops over each source)
   ├── Copy_Activity_Generic   ← Dynamic source + dynamic sink based on @item()
   │        (CSV / REST / SQL logic handled via expressions)
   └── Update_Watermark (if Copy succeeds)   ← Only meaningful for SQL
