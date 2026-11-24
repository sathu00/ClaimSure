"""
io_utils.py - shared input/output helpers for ClaimSure.

Supports:
- Local filesystem paths for development and testing.
- S3 paths (s3://...) when executed in AWS Glue.

Expected Bronze layout:
  <base_path>/bronze/<table>/ingest_date=<YYYY-MM-DD>/

Expected Parquet layout:
  <base_path>/<layer>/<table>/
"""

from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession

from claimsure.config import RAW_COLUMNS, TABLES


def _join_path(base_path: str, *parts: str) -> str:
    """Join path segments without damaging an s3:// URI."""
    base = str(base_path).rstrip("/")
    suffix = "/".join(str(p).strip("/") for p in parts)
    return f"{base}/{suffix}" if base else suffix


def table_path(base_path: str, layer: str, table: str) -> str:
    """Return the storage path for a layer/table pair."""
    if table not in TABLES and layer == "bronze":
        raise ValueError(f"Unknown ClaimSure table: {table}")
    return _join_path(base_path, layer, table)


def bronze_path(base_path: str, table: str, ingest_date: str) -> str:
    """Return the dated Bronze input path for one source table."""
    if table not in TABLES:
        raise ValueError(f"Unknown ClaimSure table: {table}")
    return _join_path(
        base_path, "bronze", table, f"ingest_date={ingest_date}"
    )


def read_bronze(
    spark: SparkSession,
    base_path: str,
    table: str,
    ingest_date: str,
) -> DataFrame:
    """
    Read a raw CSV dataset as strings.

    The CSV header is checked against config.RAW_COLUMNS. inferSchema is
    deliberately disabled so identifiers, dates and money values remain text
    until the validation rules explicitly parse them.
    """
    if table not in RAW_COLUMNS:
        raise ValueError(
            f"Unknown table {table!r}. Expected one of {sorted(RAW_COLUMNS)}"
        )

    path = bronze_path(base_path, table, ingest_date)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(path)
    )

    actual = df.columns
    expected = RAW_COLUMNS[table]

    missing = [column for column in expected if column not in actual]
    unexpected = [column for column in actual if column not in expected]

    if missing or unexpected:
        raise ValueError(
            f"CSV schema mismatch for {table} at {path}. "
            f"Missing columns: {missing or 'none'}. "
            f"Unexpected columns: {unexpected or 'none'}. "
            f"Expected order/names: {expected}. Actual: {actual}"
        )

    # Select the configured order, regardless of source column order.
    return df.select(*expected)


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame as Snappy-compressed Parquet.

    Spark handles both local paths and s3:// paths (with the appropriate
    Hadoop/S3 connector available in the runtime).
    """
    allowed_modes = {"overwrite", "append", "error", "errorifexists", "ignore"}
    if mode not in allowed_modes:
        raise ValueError(
            f"Unsupported write mode {mode!r}. Choose from {sorted(allowed_modes)}"
        )

    parsed = urlparse(str(path))
    if not parsed.scheme or parsed.scheme == "file":
        local_path = Path(parsed.path if parsed.scheme == "file" else path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode(mode)
        .option("compression", "snappy")
        .parquet(str(path))
    )


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read a previously written Parquet dataset."""
    return spark.read.parquet(str(path))
