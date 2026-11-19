"""
Cross-table data quality checks.

The pipeline expectations validate one row at a time (null keys,
positive prices...). What they cannot see is a rule that spans rows or
tables: one current version per key, no overlapping SCD2 periods,
foreign keys, gold totals matching silver. Those live here.

Each check is (name, SQL). The SQL returns the VIOLATING rows: an empty
result means the check passed.

Runs as the second task of the Lakeflow Job, right after the pipeline:

    python checks.py --catalog medallion_stage

Any violation raises an error, the task fails and the job sends the
failure notification.
"""

import argparse
import sys


def _name(catalog, table):
    """Fully qualified name. Without a catalog (local tests) uses the default one."""
    return f"{catalog}.{table}" if catalog else table


def build_checks(catalog=None):
    t = lambda name: _name(catalog, name)  # noqa: E731

    fact = t("silver.fact_orders")
    scd1 = t("silver.dim_customers_scd1")
    cust = t("silver.dim_customers_scd2")
    prod = t("silver.dim_products_scd2")

    checks = []

    # ---------- SCD Type 2: valid history ----------
    for dim, key in [(cust, "customer_id"), (prod, "product_id")]:
        short = dim.split(".")[-1]
        checks += [
            (f"{short}: one current version per key",
             f"""SELECT {key}, COUNT(*) AS current_versions
                 FROM {dim} WHERE __END_AT IS NULL
                 GROUP BY {key} HAVING COUNT(*) > 1"""),

            (f"{short}: every key has a current version",
             f"""SELECT {key} FROM {dim}
                 GROUP BY {key}
                 HAVING SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) = 0"""),

            (f"{short}: period ends after it starts",
             f"""SELECT {key}, __START_AT, __END_AT FROM {dim}
                 WHERE __END_AT IS NOT NULL AND __END_AT < __START_AT"""),

            (f"{short}: no overlapping periods",
             f"""SELECT {key}, __START_AT, __END_AT, next_start FROM (
                   SELECT {key}, __START_AT, __END_AT,
                          LEAD(__START_AT) OVER (PARTITION BY {key} ORDER BY __START_AT) AS next_start
                   FROM {dim})
                 WHERE next_start IS NOT NULL
                   AND (__END_AT IS NULL OR next_start < __END_AT)"""),
        ]

    # ---------- SCD Type 1 ----------
    checks.append(
        ("dim_customers_scd1: one row per customer",
         f"""SELECT customer_id, COUNT(*) AS n FROM {scd1}
             GROUP BY customer_id HAVING COUNT(*) > 1"""))

    # ---------- fact ----------
    checks += [
        ("fact_orders: one row per order",
         f"""SELECT order_id, COUNT(*) AS n FROM {fact}
             GROUP BY order_id HAVING COUNT(*) > 1"""),

        ("fact_orders: positive amounts",
         f"SELECT order_id, amount FROM {fact} WHERE amount IS NULL OR amount <= 0"),

        ("fact_orders: customer exists in dim_customers",
         f"""SELECT f.order_id, f.customer_id FROM {fact} f
             LEFT ANTI JOIN {cust} c ON f.customer_id = c.customer_id
             WHERE f.customer_id IS NOT NULL"""),

        ("fact_orders: product exists in dim_products",
         f"""SELECT f.order_id, f.product_id FROM {fact} f
             LEFT ANTI JOIN {prod} p ON f.product_id = p.product_id"""),
    ]

    # ---------- gold reconciles with silver ----------
    checks += [
        ("sales_by_city: totals match the fact",
         f"""SELECT g.orders, g.amount, s.orders AS fact_orders, s.amount AS fact_amount
             FROM (SELECT SUM(total_orders) AS orders, ROUND(SUM(total_amount), 2) AS amount
                   FROM {t('gold.sales_by_city')}) g
             CROSS JOIN
                  (SELECT COUNT(*) AS orders, ROUND(SUM(f.amount), 2) AS amount
                   FROM {fact} f JOIN {cust} c
                     ON f.customer_id = c.customer_id AND c.__END_AT IS NULL) s
             WHERE g.orders <> s.orders OR g.amount <> s.amount"""),

        ("sales_by_category: totals match the fact",
         f"""SELECT g.orders, g.amount, s.orders AS fact_orders, s.amount AS fact_amount
             FROM (SELECT SUM(total_orders) AS orders, ROUND(SUM(total_amount), 2) AS amount
                   FROM {t('gold.sales_by_category')}) g
             CROSS JOIN
                  (SELECT COUNT(*) AS orders, ROUND(SUM(f.amount), 2) AS amount
                   FROM {fact} f JOIN {prod} p
                     ON f.product_id = p.product_id AND p.__END_AT IS NULL) s
             WHERE g.orders <> s.orders OR g.amount <> s.amount"""),

        ("customer_ranking: rankings are 1..N with no gaps",
         f"""SELECT MIN(ranking) AS min_r, MAX(ranking) AS max_r, COUNT(*) AS n
             FROM {t('gold.customer_ranking')}
             HAVING COUNT(*) > 0 AND (MIN(ranking) <> 1 OR MAX(ranking) <> COUNT(*)
                                      OR COUNT(DISTINCT ranking) <> COUNT(*))"""),

        ("orders_price_at_purchase: one price per order",
         f"""SELECT order_id, COUNT(*) AS n FROM {t('gold.orders_price_at_purchase')}
             GROUP BY order_id HAVING COUNT(*) > 1"""),
    ]

    # ---------- nothing came out empty ----------
    for table in [fact, scd1, cust, prod,
                  t("gold.sales_by_city"), t("gold.sales_by_category"),
                  t("gold.customer_ranking"), t("gold.orders_price_at_purchase")]:
        checks.append(
            (f"{table.split('.')[-1]}: not empty",
             f"SELECT 1 AS empty FROM (SELECT COUNT(*) AS n FROM {table}) WHERE n = 0"))

    return checks


def run_checks(spark, catalog=None):
    """Runs every check and returns {check name: number of violating rows}."""
    results = {}
    for name, sql in build_checks(catalog):
        violations = spark.sql(sql)
        n = violations.count()
        results[name] = n
        print(f"[{'OK  ' if n == 0 else 'FAIL'}] {name}" + (f"  ({n} rows)" if n else ""))
        if n:
            violations.show(10, truncate=False)
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description="Cross-table data quality checks")
    parser.add_argument("--catalog", required=True, help="e.g. medallion_dev")
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    results = run_checks(spark, args.catalog)
    failed = [name for name, n in results.items() if n]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        raise AssertionError(f"Data quality checks failed: {failed}")


if __name__ == "__main__":
    main(sys.argv[1:])
