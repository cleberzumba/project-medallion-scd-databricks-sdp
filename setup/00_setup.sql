-- =========================================================
-- 00_setup.sql
-- ---------------------------------------------------------
-- One-time setup of the three environments. Run in the SQL
-- editor before the first deployment.
--
-- One catalog per environment, same layout in each:
--   medallion_<env>
--   ├── landing        schema  = files as they arrive
--   │   └── raw        volume  = source files (customers/, orders/, products/)
--   ├── bronze         schema  = raw ingestion (streaming tables)
--   ├── silver         schema  = cleansed data, SCD dimensions, fact
--   └── gold           schema  = business aggregations (materialized views)
--
--   medallion_dev    where you develop and test freely
--   medallion_stage  final validation, data close to production
--   medallion_prod   production: only the automated deployment writes here
--
-- The pipeline and the job never name a catalog: each bundle target
-- (dev / stage / prod) passes its own, so the same code runs in all three.
-- =========================================================

-- ---------- development ----------
CREATE CATALOG IF NOT EXISTS medallion_dev   COMMENT 'Medallion SCD - development';
CREATE SCHEMA  IF NOT EXISTS medallion_dev.landing COMMENT 'Landing zone for source files';
CREATE SCHEMA  IF NOT EXISTS medallion_dev.bronze  COMMENT 'Raw ingestion, append-only';
CREATE SCHEMA  IF NOT EXISTS medallion_dev.silver  COMMENT 'Cleansed data, SCD dimensions and fact';
CREATE SCHEMA  IF NOT EXISTS medallion_dev.gold    COMMENT 'Business aggregations';
CREATE VOLUME  IF NOT EXISTS medallion_dev.landing.raw COMMENT 'Source files';

-- ---------- stage ----------
CREATE CATALOG IF NOT EXISTS medallion_stage COMMENT 'Medallion SCD - stage (pre-production validation)';
CREATE SCHEMA  IF NOT EXISTS medallion_stage.landing COMMENT 'Landing zone for source files';
CREATE SCHEMA  IF NOT EXISTS medallion_stage.bronze  COMMENT 'Raw ingestion, append-only';
CREATE SCHEMA  IF NOT EXISTS medallion_stage.silver  COMMENT 'Cleansed data, SCD dimensions and fact';
CREATE SCHEMA  IF NOT EXISTS medallion_stage.gold    COMMENT 'Business aggregations';
CREATE VOLUME  IF NOT EXISTS medallion_stage.landing.raw COMMENT 'Source files';

-- ---------- production ----------
CREATE CATALOG IF NOT EXISTS medallion_prod  COMMENT 'Medallion SCD - production';
CREATE SCHEMA  IF NOT EXISTS medallion_prod.landing COMMENT 'Landing zone for source files';
CREATE SCHEMA  IF NOT EXISTS medallion_prod.bronze  COMMENT 'Raw ingestion, append-only';
CREATE SCHEMA  IF NOT EXISTS medallion_prod.silver  COMMENT 'Cleansed data, SCD dimensions and fact';
CREATE SCHEMA  IF NOT EXISTS medallion_prod.gold    COMMENT 'Business aggregations';
CREATE VOLUME  IF NOT EXISTS medallion_prod.landing.raw COMMENT 'Source files';

-- Predictive optimization: Databricks runs OPTIMIZE / VACUUM / ANALYZE on
-- managed tables by itself. Usually already on by default; enabling it on the
-- catalog makes it explicit. If this fails for lack of privileges, check the
-- account setting: it is likely enabled there already.
ALTER CATALOG medallion_dev   ENABLE PREDICTIVE OPTIMIZATION;
ALTER CATALOG medallion_stage ENABLE PREDICTIVE OPTIMIZATION;
ALTER CATALOG medallion_prod  ENABLE PREDICTIVE OPTIMIZATION;

-- Next step: upload the files in sample_data/ to each environment's volume,
-- one folder per source, e.g.
--   /Volumes/medallion_dev/landing/raw/customers/customers_2026-01.json
--   /Volumes/medallion_dev/landing/raw/orders/orders_2026-01.parquet
--   /Volumes/medallion_dev/landing/raw/products/products_2026-01.csv
--
-- In dev, upload only the 2026-01 files first, run, then add the 2026-06
-- files and run again to watch the incremental load and SCD Type 2 history.
