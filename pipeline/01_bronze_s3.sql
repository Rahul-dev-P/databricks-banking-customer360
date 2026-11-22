-- Bronze: Auto Loader Ingestion from Databricks Volume (Unity Catalog)
CREATE OR REFRESH STREAMING TABLE bronze.transactions_raw
COMMENT 'Raw streaming transactions ingested from UC volume banking_lakehouse.bronze.landing'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  CURRENT_TIMESTAMP() AS _ingest_timestamp
FROM STREAM read_files(
  '/Volumes/banking_lakehouse/bronze/bank-landing/transactions/',
  format => 'json',
  inferColumnTypes => 'true',
  schemaEvolutionMode => 'addNewColumns'
);