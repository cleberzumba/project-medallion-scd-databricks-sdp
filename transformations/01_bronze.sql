-- =========================================================
-- 01_bronze.sql — BRONZE: raw, incremental ingestion
-- ---------------------------------------------------------
-- One streaming table per source. STREAM read_files runs
-- Auto Loader underneath: each run reads only the files that
-- arrived since the last run (checkpoint managed by the pipeline).
--
-- ${source} is a pipeline configuration parameter, e.g.
--   /Volumes/medallion_dev/landing/raw
-- so the same code runs in dev, stage and prod.
--
-- _batch_key comes from the file name (customers_2026-01.json -> 2026-01).
-- The doubled backslash is intentional: Spark SQL unescapes '\\d' to '\d'.
--
-- pipelines.reset.allowed = false: a full refresh of the pipeline
-- rebuilds silver and gold but never wipes bronze, which may be the
-- only copy of data the source no longer keeps.
-- =========================================================

CREATE OR REFRESH STREAMING TABLE bronze.customers
COMMENT 'Customers from the CRM API (JSON), as received'
TBLPROPERTIES ('quality' = 'bronze', 'pipelines.reset.allowed' = 'false')
AS SELECT
  *,
  _metadata.file_name                                        AS _source_file,
  regexp_extract(_metadata.file_name, '(\\d{4}-\\d{2})', 1)  AS _batch_key,
  current_timestamp()                                        AS _ingested_at
FROM STREAM read_files('${source}/customers', format => 'json');


CREATE OR REFRESH STREAMING TABLE bronze.orders
COMMENT 'Orders from the CDC feed (Parquet), as received'
TBLPROPERTIES ('quality' = 'bronze', 'pipelines.reset.allowed' = 'false')
AS SELECT
  *,
  _metadata.file_name                                        AS _source_file,
  regexp_extract(_metadata.file_name, '(\\d{4}-\\d{2})', 1)  AS _batch_key,
  current_timestamp()                                        AS _ingested_at
FROM STREAM read_files('${source}/orders', format => 'parquet');


CREATE OR REFRESH STREAMING TABLE bronze.products
COMMENT 'Products from the legacy ERP export (CSV), as received'
TBLPROPERTIES ('quality' = 'bronze', 'pipelines.reset.allowed' = 'false')
AS SELECT
  *,
  _metadata.file_name                                        AS _source_file,
  regexp_extract(_metadata.file_name, '(\\d{4}-\\d{2})', 1)  AS _batch_key,
  current_timestamp()                                        AS _ingested_at
FROM STREAM read_files('${source}/products', format => 'csv', header => true);
