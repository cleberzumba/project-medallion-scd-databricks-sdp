"""Fact deduplication and gold aggregations over the README scenario."""
import pytest

from scenario import build_silver_and_gold


@pytest.fixture(scope="module")
def built(spark):
    build_silver_and_gold(spark)
    return spark


def test_fact_keeps_one_row_per_order(built):
    assert built.table("silver.fact_orders").count() == 4


def test_sales_by_city_uses_current_address(built):
    rows = {r.city: (r.total_orders, round(r.total_amount, 2)) for r in built.table("gold.sales_by_city").collect()}
    assert rows == {"Mountain View": (1, 999.0), "Cupertino": (2, 928.9), "Seattle": (1, 34.9)}


def test_sales_by_category(built):
    rows = {r.category: (r.total_orders, round(r.total_amount, 2), r.unique_products)
            for r in built.table("gold.sales_by_category").collect()}
    assert rows == {"Electronics": (2, 1898.0, 1), "Apparel": (2, 64.8, 1)}


def test_customer_ranking_order(built):
    rows = sorted(built.table("gold.customer_ranking").collect(), key=lambda r: r.ranking)
    assert [(r.ranking, r.name) for r in rows] == [(1, "John"), (2, "Mary"), (3, "Paul")]


def test_point_in_time_price(built):
    rows = {r.order_id: r.list_price_at_order for r in built.table("gold.orders_price_at_purchase").collect()}
    assert rows == {100: 999.0, 101: 29.9, 200: 899.0, 201: 34.9}
