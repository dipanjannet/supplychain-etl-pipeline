# Metadata-Driven Multi-Source Ingestion Pipeline in Azure Data Factory

![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-FF6A00?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Medallion Architecture](https://img.shields.io/badge/Medallion%20Architecture-Bronze%20Layer-0078D4?style=for-the-badge)

A scalable, maintainable, and **event-driven** ingestion framework using Azure Data Factory (ADF) that supports multiple source types (CSV, REST API, Azure SQL) through a central configuration table.

## Project Overview

This solution implements a **metadata-driven ingestion pipeline** for CCBJI's centralized analytics platform.

Instead of building separate pipelines for each source, we use **one generic pipeline** that reads configuration from a SQL table and dynamically processes different sources.

### Key Features

- **Single Generic Pipeline** — Handles CSV, REST API, and Azure SQL
- **Event-Driven** for CSV files (Blob Storage trigger)
- **Support for Multiple Tables** per source (e.g., `sap/sales`, `sap/orders`)
- **Medallion Architecture Ready** — Bronze layer with proper partitioning
- **Incremental Load Support** for Azure SQL using watermark pattern
- **Config-Driven Onboarding** — Add new sources/tables by inserting just **one row**

## High-Level Architecture

```mermaid
flowchart TD
    A[Blob Storage Event Trigger] --> B[pl_Generic_Ingestion Pipeline]
    B --> C[Lookup_ActiveConfigs]
    C --> D[ForEach - Config Rows]
    D --> E{SourceType?}
    E -->|CSV| F[Copy Activity - CSV to Parquet]
    E -->|AZURE_SQL| G[Copy Activity - Incremental/Full Load]
    E -->|REST_API| H[Copy Activity - REST API]
    F --> I[Bronze Layer Parquet]
    G --> I
    H --> I
    I --> J[Update Watermark - Stored Procedure]
```

### High-Level Flow:

Event Trigger fires when a new CSV file lands
Lookup reads active configurations from SQL table
ForEach processes each config row
If Conditions route to the appropriate Copy activity
Data lands in Bronze layer with proper partitioning

### Folder Structure
```
ccm-datalake/
├── ingestion/                      # Landing / Raw zone
│   ├── sap/
│   │   └── sales/
│   │       └── sales_transactions_20260328.csv
│   ├── azure_sql/
│   │   └── customer/
│   └── rest_api/
│       └── product/
└── storage/                        # Bronze Layer (Parquet)
    ├── sap/
    │   └── sales/
    │       └── sales_20260328.parquet
    ├── azure_sql/
    │   └── customer/
    └── rest_api/
        └── product/
```

Configuration Table
```
CREATE TABLE dbo.IngestionConfig (
    ConfigId            INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem        VARCHAR(50)  NOT NULL,     -- 'sap', 'azure_sql', 'rest_api'
    TableName           VARCHAR(100) NOT NULL,
    SourceType          VARCHAR(20)  NOT NULL,     -- 'CSV', 'REST_API', 'AZURE_SQL'
    IsActive            BIT DEFAULT 1,

    LinkedServiceName   VARCHAR(100),
    SourceDatasetName   VARCHAR(100),
    SourceParams        NVARCHAR(MAX),            -- JSON for extra params

    WatermarkColumn     VARCHAR(100),
    WatermarkValue      DATETIME2(0) NULL,

    LastProcessedDate   DATE NULL,
    CreatedDate         DATETIME2 DEFAULT GETUTCDATE(),
    UpdatedDate         DATETIME2 DEFAULT GETUTCDATE()
);
```

Sample Data
```
-- SAP Sales CSV
INSERT INTO dbo.IngestionConfig (SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName)
VALUES ('sap', 'sales', 'CSV', 'ls_ADLS_Landing', 'ds_Generic_CSV');

-- Customer from Azure SQL (Incremental)
INSERT INTO dbo.IngestionConfig (SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName, 
                                 SourceParams, WatermarkColumn)
VALUES ('azure_sql', 'customer', 'AZURE_SQL', 'ls_AzureSQL_Source', 'ds_Generic_AzureSQL', 
        '{"tableName":"dbo.Customer"}', 'last_modified');

```

Pipeline Design (pl_Generic_Ingestion)

The pipeline consists of:

Lookup → Reads active configurations
ForEach → Loops through each config
If Condition → Routes based on SourceType (more stable than Switch)
Copy Activity → Performs actual data movement
Stored Procedure → Updates watermark for SQL sources

Setup Instructions

Create required Linked Services (ls_ADLS_Landing, ls_ADLS_Bronze, ls_AzureSQL_Source, etc.)
Create Datasets (ds_Generic_CSV, ds_Generic_HTTP, ds_Generic_AzureSQL, ds_Bronze_Parquet)
Create the IngestionConfig table and insert sample rows
Deploy the pipeline pl_Generic_Ingestion
Create Blob Storage Event Trigger and map triggerFileName
