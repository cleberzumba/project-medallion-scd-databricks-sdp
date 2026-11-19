"""
Shared fixtures. Tests run on a local SparkSession: no Databricks,
no Delta, no cluster. Tables are plain Parquet in a temp warehouse,
which is enough to execute the same SQL the pipeline runs.
"""
import re
import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

TRANSFORMATIONS = Path(__file__).resolve().parents[1] / "transformations"


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp()
    session = (SparkSession.builder.master("local[1]")
               .appName("medallion-sdp-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.sql.warehouse.dir", warehouse)
               .getOrCreate())
    for db in ["bronze", "silver", "gold"]:
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def pipeline_queries(file_name):
    """
    {table name: SELECT body} for every CREATE OR REFRESH ... AS SELECT in a
    pipeline file. STREAM(x) becomes x: locally we read in batch, which runs
    exactly the same transformation logic.
    """
    src = re.sub(r"--[^\n]*", "", (TRANSFORMATIONS / file_name).read_text())
    queries = {}
    for stmt in src.split(";"):
        name = re.search(r"CREATE OR REFRESH (?:STREAMING TABLE|MATERIALIZED VIEW)\s+([\w.]+)", stmt)
        body = re.search(r"\bAS\s+SELECT\b", stmt)
        if name and body:
            sql = stmt[body.start() + 2:]
            queries[name.group(1)] = re.sub(r"STREAM\(([\w.]+)\)", r"\1", sql)
    return queries


def save(spark, table, rows, schema):
    spark.createDataFrame(rows, schema).write.mode("overwrite").saveAsTable(table)
