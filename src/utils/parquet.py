"""Portable helpers for reading persisted Parquet datasets."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_parquet_data_files(
    spark: SparkSession,
    path: Path,
    *,
    schema: StructType | None = None,
) -> DataFrame:
    """Read physical Parquet files without trusting non-portable sink log URIs."""

    files = sorted(file for file in path.rglob("*.parquet") if file.is_file())
    if not files:
        raise FileNotFoundError(f"No Parquet data files found under: {path}")
    reader = spark.read if schema is None else spark.read.schema(schema)
    return reader.option("basePath", str(path)).parquet(*(str(file) for file in files))
