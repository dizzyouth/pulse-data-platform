"""Read a consistent PostgreSQL snapshot for the existing Spark quality engine."""

from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import psycopg
from psycopg import sql
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

from src.warehouse.load_gold import (
    TABLE_SPECS, WAREHOUSE_SCHEMA, connection_kwargs, validate_required_columns,
)


def warehouse_schema(spec) -> StructType:
    types = {"DATE": DateType(), "TEXT": StringType(), "BIGINT": LongType(),
             "DOUBLE PRECISION": DoubleType(), "TIMESTAMP WITH TIME ZONE": TimestampType()}
    # Keep nulls visible to the quality rules, even for NOT NULL columns.
    return StructType([StructField(col.name, types[col.postgres_type], True) for col in spec.columns])


@contextmanager
def warehouse_frames(spark):
    """Stream all four tables to temporary JSONL in one read-only snapshot.

    Server cursors bound Python memory; explicit schemas preserve empty tables,
    dates, timestamps, and nullable metrics. Files outlive lazy Spark evaluation.
    No JDBC driver or additional package is needed. Intended for local Spark.
    """
    with TemporaryDirectory(prefix="pulse-quality-warehouse-") as directory:
        paths = {}
        with psycopg.connect(**connection_kwargs()) as connection:
            connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            connection.read_only = True
            with connection.cursor() as cursor:
                for spec in TABLE_SPECS:
                    cursor.execute(
                        "SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                        (WAREHOUSE_SCHEMA, spec.name),
                    )
                    columns = dict(cursor.fetchall())
                    validate_required_columns(spec.name, tuple(columns), spec)
                    for col in spec.columns:
                        if columns[col.name] != col.postgres_type.lower():
                            raise ValueError(f"Warehouse {spec.name}.{col.name} has unexpected type {columns[col.name]}")
                    path = Path(directory, f"{spec.name}.jsonl")
                    with connection.cursor(name=f"quality_{spec.name}") as rows, path.open("w", encoding="utf-8") as output:
                        rows.execute(sql.SQL("SELECT row_to_json(snapshot)::text FROM {}.{} AS snapshot").format(
                            sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(spec.name),
                        ))
                        for (record,) in rows:
                            output.write(record + "\n")
                    paths[spec.name] = path
        yield {
            spec.name: spark.read.schema(warehouse_schema(spec)).option("mode", "FAILFAST").json(str(paths[spec.name]))
            for spec in TABLE_SPECS
        }
