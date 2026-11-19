"""
The README scenario (January + June 2026 batches) shaped as the pipeline
leaves it: dimensions with __START_AT/__END_AT as AUTO CDC writes them.
"""
from datetime import date

from conftest import pipeline_queries, save

STG_ORDERS = (
    [(100, 1, "P1", 999.0, date(2026, 2, 10), date(2026, 1, 1), "orders_2026-01.parquet"),
     (101, 2, "P3", 29.9,  date(2026, 2, 11), date(2026, 1, 1), "orders_2026-01.parquet"),
     (100, 1, "P1", 999.0, date(2026, 2, 10), date(2026, 6, 1), "orders_2026-06.parquet"),  # replayed
     (200, 2, "P1", 899.0, date(2026, 6, 16), date(2026, 6, 1), "orders_2026-06.parquet"),
     (201, 3, "P3", 34.9,  date(2026, 6, 18), date(2026, 6, 1), "orders_2026-06.parquet")],
    "order_id int, customer_id int, product_id string, amount double, order_date date, "
    "batch_date date, _source_file string")

DIM_CUSTOMERS_SCD1 = (
    [(1, "John", "Mountain View", date(2026, 6, 10)),
     (2, "Mary", "Cupertino",     date(2026, 6, 10)),
     (3, "Paul", "Seattle",       date(2026, 1, 15))],
    "customer_id int, name string, city string, event_date date")

DIM_CUSTOMERS_SCD2 = (
    [(1, "John", "Palo Alto",     date(2026, 1, 15), date(2026, 1, 15), date(2026, 6, 10)),
     (1, "John", "Mountain View", date(2026, 6, 10), date(2026, 6, 10), None),
     (2, "Mary", "San Jose",      date(2026, 1, 15), date(2026, 1, 15), date(2026, 6, 10)),
     (2, "Mary", "Cupertino",     date(2026, 6, 10), date(2026, 6, 10), None),
     (3, "Paul", "Seattle",       date(2026, 1, 15), date(2026, 1, 15), None)],
    "customer_id int, name string, city string, event_date date, __START_AT date, __END_AT date")

DIM_PRODUCTS_SCD2 = (
    [("P1", "iPhone",  "Electronics", 999.0, date(2025, 9, 1), date(2026, 1, 1), date(2026, 6, 1)),
     ("P1", "iPhone",  "Electronics", 899.0, date(2025, 9, 1), date(2026, 6, 1), None),
     ("P3", "T-Shirt", "Apparel",     29.9,  date(2025, 1, 1), date(2026, 1, 1), date(2026, 6, 1)),
     ("P3", "T-Shirt", "Apparel",     34.9,  date(2025, 1, 1), date(2026, 6, 1), None)],
    "product_id string, product_name string, category string, price double, launch_date date, "
    "__START_AT date, __END_AT date")


def build_silver_and_gold(spark):
    """Loads the scenario and runs the real fact and gold queries over it."""
    save(spark, "silver.stg_orders", *STG_ORDERS)
    save(spark, "silver.dim_customers_scd1", *DIM_CUSTOMERS_SCD1)
    save(spark, "silver.dim_customers_scd2", *DIM_CUSTOMERS_SCD2)
    save(spark, "silver.dim_products_scd2", *DIM_PRODUCTS_SCD2)
    spark.sql(pipeline_queries("02_silver.sql")["silver.fact_orders"]) \
        .write.mode("overwrite").saveAsTable("silver.fact_orders")
    for table, sql in pipeline_queries("03_gold.sql").items():
        spark.sql(sql).write.mode("overwrite").saveAsTable(table)
