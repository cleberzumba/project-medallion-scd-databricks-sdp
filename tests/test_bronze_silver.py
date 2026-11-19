"""Bronze batch-key extraction and silver staging logic."""
import re
from datetime import date

from conftest import TRANSFORMATIONS, pipeline_queries, save


def _batch_key_literal():
    sql = (TRANSFORMATIONS / "01_bronze.sql").read_text()
    return re.search(r"regexp_extract\(_metadata\.file_name, ('[^']+'), 1\)", sql).group(1)


def test_batch_key_extracted_from_file_name(spark):
    lit = _batch_key_literal()
    assert spark.sql(f"SELECT regexp_extract('customers_2026-01.json', {lit}, 1) k").first().k == "2026-01"


def test_batch_key_empty_when_file_name_has_no_date(spark):
    lit = _batch_key_literal()
    assert spark.sql(f"SELECT regexp_extract('customers_latest.json', {lit}, 1) k").first().k == ""


def test_all_bronze_tables_use_the_same_batch_key_pattern():
    sql = (TRANSFORMATIONS / "01_bronze.sql").read_text()
    assert len(set(re.findall(r"regexp_extract\(_metadata\.file_name, ('[^']+'), 1\)", sql))) == 1


def test_stg_customers_types_and_batch_date(spark):
    save(spark, "bronze.customers", [
        ("1", "John", "Palo Alto", "2026-01-15", None, "customers_2026-01.json", "2026-01"),
        ("9", "NoBatch", "Y", "2026-01-15", None, "customers_latest.json", ""),
    ], "customer_id string, name string, city string, event_date string, "
       "_rescued_data string, _source_file string, _batch_key string")
    rows = {r.customer_id: r for r in spark.sql(pipeline_queries("02_silver.sql")["silver.stg_customers"]).collect()}
    assert rows[1].batch_date == date(2026, 1, 1)
    assert rows[1].event_date == date(2026, 1, 15)
    # no batch key -> NULL batch_date -> the FAIL UPDATE expectation stops the pipeline
    assert rows[9].batch_date is None


def test_stg_products_casts_price(spark):
    save(spark, "bronze.products", [
        ("P1", "iPhone", "Electronics", "999.00", "2025-09-01", None, "products_2026-01.csv", "2026-01"),
    ], "product_id string, product_name string, category string, price string, launch_date string, "
       "_rescued_data string, _source_file string, _batch_key string")
    row = spark.sql(pipeline_queries("02_silver.sql")["silver.stg_products"]).first()
    assert row.price == 999.0 and row.launch_date == date(2025, 9, 1)


def test_stg_orders_has_no_rescued_data_column(spark):
    save(spark, "bronze.orders", [
        ("100", "1", "P1", "999.0", "2026-02-10", "orders_2026-01.parquet", "2026-01"),
    ], "order_id string, customer_id string, product_id string, amount string, order_date string, "
       "_source_file string, _batch_key string")
    df = spark.sql(pipeline_queries("02_silver.sql")["silver.stg_orders"])
    assert "_rescued_data" not in df.columns
    assert df.first().order_id == 100
