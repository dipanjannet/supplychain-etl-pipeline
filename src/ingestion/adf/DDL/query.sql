CREATE TABLE dbo.IngestionConfig (
    ConfigId                INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem            VARCHAR(50)  NOT NULL,     -- e.g., 'sap', 'azure_sql', 'rest_api'
    TableName               VARCHAR(100) NOT NULL,     -- e.g., 'sales', 'customer', 'product'
    SourceType              VARCHAR(20)  NOT NULL,     -- 'CSV', 'REST_API', 'AZURE_SQL'
    IsActive                BIT DEFAULT 1,

    LinkedServiceName       VARCHAR(100) NULL,
    SourceDatasetName       VARCHAR(100) NULL,
    SourceParams            NVARCHAR(MAX) NULL,       -- JSON for additional params

    WatermarkColumn         VARCHAR(100) NULL,
    WatermarkValue          DATETIME2(0) NULL,

    -- Bronze path will be built as storage/{SourceSystem}/{TableName}/
    LastProcessedDate       DATE NULL,
    CreatedDate             DATETIME2 DEFAULT GETUTCDATE(),
    UpdatedDate             DATETIME2 DEFAULT GETUTCDATE()
);

-- SAP Sales
INSERT INTO dbo.IngestionConfig 
(SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName, SourceParams)
VALUES 
('sap', 'sales', 'CSV', 'ls_ADLS_Landing', 'ds_Generic_CSV', '{}');

-- Customer from Azure SQL
INSERT INTO dbo.IngestionConfig 
(SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName, SourceParams, WatermarkColumn)
VALUES 
('azure_sql', 'customer', 'AZURE_SQL', 'ls_AzureSQL_Source', 'ds_Generic_AzureSQL', 
 '{"tableName":"dbo.Customer"}', 'last_modified');

-- Product from REST API
INSERT INTO dbo.IngestionConfig 
(SourceSystem, TableName, SourceType, LinkedServiceName, SourceDatasetName, SourceParams)
VALUES 
('rest_api', 'product', 'REST_API', 'ls_HTTP_ProductAPI', 'ds_Generic_HTTP', 
 '{"relativeUrl":"/products"}');


select * from dbo.IngestionConfig;


CREATE OR ALTER PROCEDURE [dbo].[usp_UpdateWatermark]
    @ConfigId INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MaxWatermark DATETIME2(0);

    -- Get the max value from the source table (example for Customer table)
    SELECT @MaxWatermark = MAX(last_modified)
    FROM dbo.Customer;          -- Change table name if needed for other sources

    UPDATE dbo.IngestionConfig
    SET 
        WatermarkValue   = ISNULL(@MaxWatermark, GETUTCDATE()),
        LastProcessedDate = CAST(GETUTCDATE() AS DATE),
        UpdatedDate      = GETUTCDATE()
    WHERE ConfigId = @ConfigId;

END;