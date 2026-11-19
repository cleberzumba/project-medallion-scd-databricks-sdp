# Medallion Architecture + SCD — Lakeflow Spark Declarative Pipelines

A declarative rebuild of [medallion-scd-azure-databricks-delta](https://github.com/cleberzumba/medallion-scd-azure-databricks-delta) using **Lakeflow Spark Declarative Pipelines (SDP, formerly Delta Live Tables)** in **SQL**, running on the **Databricks Free Edition**.

The original project builds the same Lakehouse imperatively: PySpark, Auto Loader with hand-configured checkpoints, `MERGE` statements for SCD, and a Lakeflow Job to order the steps. This version declares the tables and lets the pipeline handle ingestion state, execution order, change data capture and data quality.

---

## The scenario

A retail company receives data from three source systems, each in a different format:

| Source | Format | Simulates |
|--------|--------|-----------|
| **Customers** | JSON | CRM REST API |
| **Orders** | Parquet | CDC pipeline |
| **Products** | CSV | Legacy ERP export |

Two batches show the dimensions changing over time:

- **January 2026** — initial customers, products and orders.
- **June 2026** — John moves to Mountain View, Mary to Cupertino, a new customer joins, the iPhone drops from $999 to $899, the T-Shirt goes from $29.90 to $34.90, and a new product launches.

Because the product dimension keeps its history, a February order is priced at $999 and a June order at $899: the **point-in-time join** that SCD Type 2 exists for.

---

## Pipeline

```
/Volumes/medallion_dev/landing/raw/{customers,orders,products}
        │  STREAM read_files  (Auto Loader, incremental)
        ▼
BRONZE  bronze.customers · bronze.orders · bronze.products          streaming tables
        │  typing + expectations
        ▼
SILVER  silver.stg_customers · silver.stg_products · silver.stg_orders   streaming tables
        │  AUTO CDC
        ├── silver.dim_customers_scd1    SCD Type 1
        ├── silver.dim_customers_scd2    SCD Type 2 (address history)
        ├── silver.dim_products_scd2     SCD Type 2 (price history)
        └── silver.fact_orders           materialized view, one row per order
        ▼
GOLD    gold.sales_by_city · gold.sales_by_category
        gold.customer_ranking · gold.orders_price_at_purchase       materialized views
```

The pipeline builds this graph on its own from the table references: no task dependencies are declared anywhere.

---

## Repository structure

```
medallion-scd-databricks-sdp/
├── databricks.yml                    ← bundle: variables and dev / stage / prod targets
├── resources/
│   ├── medallion_scd.pipeline.yml    ← the declarative pipeline
│   └── medallion_scd.job.yml         ← Lakeflow Job: pipeline → quality checks
├── transformations/                  ← pipeline source code (SQL)
│   ├── 01_bronze.sql                 ← raw ingestion
│   ├── 02_silver.sql                 ← cleansing, SCD dimensions, fact
│   └── 03_gold.sql                   ← business aggregations
├── src/data_quality/checks.py        ← cross-table checks (job task 2)
├── setup/
│   ├── 00_setup.sql                  ← the three catalogs, schemas and volumes
│   └── 01_explore_sources.sql        ← ad hoc checks of the uploaded files
├── tests/                            ← pytest, local Spark, no Databricks needed
└── .github/workflows/ci-cd.yml       ← CI (tests) + CD (deploy stage → prod)
```

---

## How it compares with the imperative version

| Concern | Imperative version | This version |
|---------|-------------------|--------------|
| Incremental ingestion | `readStream.format("cloudFiles")` + schema location + checkpoint + `availableNow` + `writeStream` | `STREAM read_files(...)` |
| Execution order | `depends_on` in the job definition | Inferred from table references |
| SCD Type 1 / Type 2 | `MERGE` + `INSERT`, replayed batch by batch | `AUTO CDC ... STORED AS SCD TYPE 1 / 2` |
| Late or replayed batches | Batch replay order had to be managed by hand | `SEQUENCE BY` orders changes; an old batch cannot overwrite a newer one |
| Validity columns | `valid_from`, `valid_to`, `is_current` | `__START_AT`, `__END_AT` (current version: `__END_AT IS NULL`) |
| Data quality | Filters in code + post-run checks | Expectations (`WARN`, `DROP ROW`, `FAIL UPDATE`) with metrics in the event log |
| Table layout | External tables, `PARTITION BY` + `ZORDER BY` + `OPTIMIZE`/`VACUUM` | Managed tables, liquid clustering, maintenance handled by the platform |
| Configuration per environment | Constants at the top of each script | Bundle targets set the catalog and `${source}` per environment |

---

## Data quality

| Table | Expectation | Action |
|-------|-------------|--------|
| `stg_customers`, `stg_products` | batch key extracted from the file name | `FAIL UPDATE` |
| `stg_customers` | `customer_id`, `name`, `city`, `event_date` not null | `DROP ROW` |
| `stg_products` | `product_id` not null, `price > 0` | `DROP ROW` |
| `stg_orders` | `order_id`, `product_id` not null, `amount > 0` | `DROP ROW` |
| `stg_orders` | `customer_id` not null | `WARN` |
| `stg_customers`, `stg_products` | no rescued data (`_rescued_data IS NULL`) | `WARN` |

The batch-key check fails the flow on purpose: a file whose name has no `YYYY-MM` segment cannot be sequenced, and silently dropping it would lose data without any error.

Results appear per table in the pipeline UI and in the event log (`event_log()`).

### Cross-table checks

Expectations look at one row at a time. Rules that span rows or tables run as the second task of the job (`src/data_quality/checks.py`); any violation fails the job:

- SCD2: one current version per key, every key has a current version, no period ending before it starts, no overlapping periods
- SCD1: one row per customer
- Fact: one row per order, positive amounts, every customer and product exists in its dimension
- Gold: `sales_by_city` and `sales_by_category` totals reconcile with the fact; ranking is 1..N with no gaps; one price per order in the point-in-time view
- No table came out empty

---

## Environments and deployment

Three catalogs in the same workspace, one per environment:

| Target | Catalog | Who deploys | Purpose |
|--------|---------|-------------|---------|
| `dev` | `medallion_dev` | you, from the CLI | development; resources prefixed `[dev <user>]`, schedule paused |
| `stage` | `medallion_stage` | GitHub Actions | runs the full job on every push to `main` before prod |
| `prod` | `medallion_prod` | GitHub Actions only | production; daily schedule active |

The SQL never names a catalog: each target sets `catalog` and `source_path`, the pipeline uses them as its default catalog and as `${source}`.

```
pull request ─► pytest ─► bundle validate                                   (CI)
push to main ─► pytest ─► validate ─► deploy stage ─► run job on stage
                                                  └─► deploy prod ─► run job on prod   (CD)
```

If anything fails on stage (pipeline, expectations or cross-table checks), prod is not touched.

**Prototype in the UI → View as code → bundle.** The pipeline and the job were first built in the UI. Both screens offer *View as code* (YAML); that YAML was pasted into `resources/` with the hard-coded values swapped for `${var.catalog}`, `${var.source_path}` and `${resources.pipelines.medallion_scd_sdp.id}`.

---

## Setup (Databricks Free Edition)

### 1. Catalog, schemas and volume

Run `setup/00_setup.sql` in the SQL editor. It creates the same layout in `medallion_dev`, `medallion_stage` and `medallion_prod`:

```
medallion_<env>
├── landing.raw   (volume for the source files)
├── bronze
├── silver
└── gold
```

and enables predictive optimization on the three catalogs.

### 2. Upload the January files (dev)

Upload the 2026-01 sample files (from the `sample_data/` folder of the imperative repository) to the volume, one folder per source:

```
/Volumes/medallion_dev/landing/raw/customers/customers_2026-01.json
/Volumes/medallion_dev/landing/raw/orders/orders_2026-01.parquet
/Volumes/medallion_dev/landing/raw/products/products_2026-01.csv
```

Run `setup/01_explore_sources.sql` to confirm the column names and that the batch key comes out as `2026-01`.

For stage and prod, upload both batches (2026-01 and 2026-06) to `/Volumes/medallion_stage/landing/raw/...` and `/Volumes/medallion_prod/landing/raw/...`.

### 3. Create the pipeline

**Option A: bundle (the way CI does it)**

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      -t dev medallion_scd_job
```

**Option B: UI prototype**

Clone this repository as a Git folder, then go to **Jobs & Pipelines → Create → ETL pipeline**:

| Setting | Value |
|---------|-------|
| Name | `medallion_scd_sdp` |
| Source code | the `transformations/` folder of the Git folder |
| Default catalog | `medallion_dev` |
| Default schema | `bronze` |
| Compute | Serverless |
| Configuration | `source` = `/Volumes/medallion_dev/landing/raw` |

### 4. First run

Run the pipeline (or the job). Every table is built from the January data.

### 5. Second run: the incremental behaviour

Upload the 2026-06 files to the same folders and run the pipeline again:

- Bronze and staging process **only the June rows**.
- `dim_customers_scd2` gains a second version for John and Mary; the January version gets an `__END_AT`.
- `dim_products_scd2` gains a second version for the iPhone and the T-Shirt.
- `gold.orders_price_at_purchase` shows $999 for the February iPhone order and $899 for the June one.

---

### 6. CI/CD

In the GitHub repository, **Settings → Secrets and variables → Actions**, add:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | workspace URL |
| `DATABRICKS_TOKEN` | personal access token (in a company: a service principal's token) |

Create the `stage` and `production` environments under **Settings → Environments**; adding required reviewers to `production` turns the prod deploy into a manual approval.

---

## Tests

```bash
pip install -r requirements-dev.txt   # needs Java 17
pytest
```

The tests read the SQL straight from `transformations/`, replace `STREAM(x)` with `x` and run it on a local SparkSession, so the logic tested is the logic deployed:

- `test_bronze_silver.py`: batch key regex, typing, a file without a date yields a NULL `batch_date` (the `FAIL UPDATE` case)
- `test_gold.py`: fact deduplication, the four gold views over the README scenario, including the $999 / $899 point-in-time prices
- `test_checks.py`: the cross-table checks pass on healthy data and catch duplicated current versions, overlapping periods, orphan keys, negative amounts and gold out of sync

What only Databricks can test (AUTO CDC, expectations, Auto Loader state) is covered by the job run on stage.

---

## Design decisions

**SQL over Python.** Every table here is a declarative query; SQL keeps the pipeline readable for analysts and matches the way the tables are queried afterwards.

**AUTO CDC instead of MERGE.** The imperative version needed the batches replayed in chronological order to build the SCD history, and replaying them again corrupted it. `SEQUENCE BY` makes the order part of the definition: changes are applied by date, whatever order the files arrive in.

**History tracked only where it matters.** `TRACK HISTORY ON name, city` for customers and `ON product_name, category, price` for products: a new `event_date` with the same address updates the current row instead of creating a meaningless version.

**Products sequenced by batch date.** The product feed has no change timestamp of its own, so the date in the file name (`products_2026-06.csv` → 2026-06-01) is the sequence.

**Fact as a materialized view.** The same order can arrive in more than one batch; a window over `order_id` keeps one row per order and is recomputed consistently on every run.

**Bronze protected from full refresh.** `pipelines.reset.allowed = false` lets silver and gold be rebuilt from scratch without wiping the raw layer.

**Managed tables, liquid clustering and broadcast joins.** Every table is a Unity Catalog managed table, so predictive optimization handles `OPTIMIZE`/`VACUUM`. The dimensions are clustered by their key and `CLUSTER BY (customer_id, product_id)` on the fact replaces `PARTITION BY` + `ZORDER BY`. Gold joins broadcast the small dimensions so the fact is never shuffled for a join.

**One catalog per environment.** The code never names the catalog or the source path, so the same pipeline runs in all three. Isolation by catalog is what makes it possible to grant developers write access to `medallion_dev` only.

---

## Next steps

- Unity Catalog governance: grants per environment, tags, column masking and row filters
- Performance notebook: synthetic data to provoke shuffle, spill and skew, read in the Spark UI

---

## References

- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [AUTO CDC INTO (pipelines)](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into)
- [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations)
- [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Kimball Group — Slowly Changing Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
