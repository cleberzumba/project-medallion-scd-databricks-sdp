-- =========================================================
-- 01_explore_sources.sql
-- ---------------------------------------------------------
-- Ad hoc checks of the uploaded files before running the
-- pipeline. Plain (batch) read_files: nothing is written.
-- Confirm that the columns the pipeline expects are there:
--   customers: customer_id, name, city, event_date
--   orders:    order_id, customer_id, product_id, amount, order_date
--   products:  product_id, product_name, category, price, launch_date
-- =========================================================

SELECT *, _metadata.file_name
FROM read_files('/Volumes/medallion_dev/landing/raw/customers', format => 'json');

SELECT *, _metadata.file_name
FROM read_files('/Volumes/medallion_dev/landing/raw/orders', format => 'parquet');

SELECT *, _metadata.file_name
FROM read_files('/Volumes/medallion_dev/landing/raw/products', format => 'csv', header => true);

-- The batch key the pipeline extracts from each file name (expect 2026-01, 2026-06).
-- Note the doubled backslash: Spark SQL unescapes '\\d' to the regex '\d'.
SELECT DISTINCT
  _metadata.file_name                                         AS file_name,
  regexp_extract(_metadata.file_name, '(\\d{4}-\\d{2})', 1)   AS batch_key
FROM read_files('/Volumes/medallion_dev/landing/raw/customers', format => 'json');
