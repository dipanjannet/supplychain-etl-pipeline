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
