"""The cross-table checks pass on healthy data and catch each kind of corruption."""
from datetime import date

import pytest

from scenario import DIM_CUSTOMERS_SCD2, DIM_PRODUCTS_SCD2, build_silver_and_gold
from conftest import save
from src.data_quality.checks import build_checks, main, run_checks


@pytest.fixture
def healthy(spark):
    build_silver_and_gold(spark)
    return spark


def failed(results):
    return {name for name, n in results.items() if n}


def test_healthy_data_passes_every_check(healthy):
    assert failed(run_checks(healthy)) == set()


def test_catalog_is_used_in_every_table_name():
    for _, sql in build_checks("medallion_stage"):
        assert "medallion_stage." in sql
        assert " silver." not in sql and " gold." not in sql


def test_two_current_versions_detected(healthy):
    rows, schema = DIM_CUSTOMERS_SCD2
    rows = rows + [(3, "Paul", "Denver", date(2026, 7, 1), date(2026, 7, 1), None)]
    save(healthy, "silver.dim_customers_scd2", rows, schema)
    assert "dim_customers_scd2: one current version per key" in failed(run_checks(healthy))


def test_overlapping_periods_detected(healthy):
    rows, schema = DIM_PRODUCTS_SCD2
    rows = [r if not (r[0] == "P1" and r[6] is not None) else r[:6] + (date(2026, 8, 1),) for r in rows]
    save(healthy, "silver.dim_products_scd2", rows, schema)
    assert "dim_products_scd2: no overlapping periods" in failed(run_checks(healthy))


def test_end_before_start_detected(healthy):
    rows, schema = DIM_CUSTOMERS_SCD2
    rows = [(9, "Bad", "X", date(2026, 1, 1), date(2026, 5, 1), date(2026, 4, 1))] + rows
    save(healthy, "silver.dim_customers_scd2", rows, schema)
    assert "dim_customers_scd2: period ends after it starts" in failed(run_checks(healthy))


def test_orphan_product_and_negative_amount_detected(healthy):
    healthy.sql("""SELECT * FROM silver.fact_orders UNION ALL
                   SELECT 999, 1, 'P404', -5.0, DATE'2026-06-20', 2026, 6""") \
        .write.mode("overwrite").saveAsTable("silver.fact_orders_tmp")
    healthy.table("silver.fact_orders_tmp").write.mode("overwrite").saveAsTable("silver.fact_orders")
    errors = failed(run_checks(healthy))
    assert "fact_orders: product exists in dim_products" in errors
    assert "fact_orders: positive amounts" in errors
    # gold was not rebuilt, so it no longer matches the fact either
    assert "sales_by_city: totals match the fact" in errors


def test_main_raises_on_violation(healthy, monkeypatch):
    healthy.sql("SELECT * FROM gold.customer_ranking WHERE ranking <> 2") \
        .write.mode("overwrite").saveAsTable("gold.ranking_tmp")
    healthy.table("gold.ranking_tmp").write.mode("overwrite").saveAsTable("gold.customer_ranking")
    import src.data_quality.checks as checks
    monkeypatch.setattr(checks, "build_checks", lambda catalog: build_checks(None))
    with pytest.raises(AssertionError, match="rankings are 1..N"):
        main(["--catalog", "local"])
