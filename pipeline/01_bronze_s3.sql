-- Bronze: S3 Auto Loader Ingestion
CREATE OR REFRESH STREAMING TABLE bronze.transactions_raw
COMMENT 'Raw streaming transactions ingested from Amazon S3'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  CURRENT_TIMESTAMP() AS _ingest_timestamp
FROM STREAM read_files(
  's3://bank-landing/transactions/',
  format => 'json',
  inferColumnTypes => 'true',
  schemaEvolutionMode => 'addNewColumns'
);