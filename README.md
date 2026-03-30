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
