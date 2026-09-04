"""Transactionally full-refresh Gold Parquet datasets into PostgreSQL."""

from __future__ import annotations

import argparse
import os
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import sql

from src.analytics.gold_build import GoldPaths, build_gold_spark_session, load_gold_paths
from src.utils.parquet import read_parquet_data_files

WAREHOUSE_SCHEMA = "analytics"


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    name: str
    postgres_type: str
    nullable: bool = False


@dataclass(frozen=True, slots=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...]
    index_column: str
    nonnegative_columns: tuple[str, ...] = ()
    rate_columns: tuple[str, ...] = ()

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


TABLE_SPECS = (
    TableSpec(
        "daily_sales",
        (
            ColumnSpec("event_date", "DATE"), ColumnSpec("country", "TEXT"),
            ColumnSpec("currency", "TEXT"), ColumnSpec("completed_orders", "BIGINT"),
            ColumnSpec("units_sold", "BIGINT"), ColumnSpec("gross_revenue", "DOUBLE PRECISION"),
            ColumnSpec("avg_order_value", "DOUBLE PRECISION", True),
        ),
        "event_date", ("completed_orders", "units_sold", "gross_revenue", "avg_order_value"),
    ),
    TableSpec(
        "customer_metrics",
        (
            ColumnSpec("customer_id", "TEXT"), ColumnSpec("first_event_at", "TIMESTAMP WITH TIME ZONE"),
            ColumnSpec("last_event_at", "TIMESTAMP WITH TIME ZONE"), ColumnSpec("products_viewed", "BIGINT"),
            ColumnSpec("cart_adds", "BIGINT"), ColumnSpec("checkouts_started", "BIGINT"),
            ColumnSpec("orders_created", "BIGINT"), ColumnSpec("payments_completed", "BIGINT"),
            ColumnSpec("orders_delivered", "BIGINT"), ColumnSpec("orders_refunded", "BIGINT"),
            ColumnSpec("total_units_purchased", "BIGINT"), ColumnSpec("total_revenue", "DOUBLE PRECISION"),
            ColumnSpec("distinct_orders", "BIGINT"),
        ),
        "customer_id",
        ("products_viewed", "cart_adds", "checkouts_started", "orders_created", "payments_completed", "orders_delivered", "orders_refunded", "total_units_purchased", "total_revenue", "distinct_orders"),
    ),
    TableSpec(
        "product_metrics",
        (
            ColumnSpec("product_id", "TEXT"), ColumnSpec("seller_id", "TEXT", True),
            ColumnSpec("views", "BIGINT"), ColumnSpec("cart_adds", "BIGINT"),
            ColumnSpec("orders_created", "BIGINT"), ColumnSpec("payments_completed", "BIGINT"),
            ColumnSpec("units_sold", "BIGINT"), ColumnSpec("gross_revenue", "DOUBLE PRECISION"),
            ColumnSpec("distinct_customers", "BIGINT"),
        ),
        "product_id", ("views", "cart_adds", "orders_created", "payments_completed", "units_sold", "gross_revenue", "distinct_customers"),
    ),
    TableSpec(
        "funnel_metrics",
        (
            ColumnSpec("event_date", "DATE"), ColumnSpec("country", "TEXT"),
            ColumnSpec("product_views", "BIGINT"), ColumnSpec("cart_adds", "BIGINT"),
            ColumnSpec("checkouts_started", "BIGINT"), ColumnSpec("orders_created", "BIGINT"),
            ColumnSpec("payments_completed", "BIGINT"), ColumnSpec("orders_delivered", "BIGINT"),
            ColumnSpec("refunds", "BIGINT"), ColumnSpec("view_to_cart_rate", "DOUBLE PRECISION", True),
            ColumnSpec("cart_to_checkout_rate", "DOUBLE PRECISION", True),
            ColumnSpec("checkout_to_order_rate", "DOUBLE PRECISION", True),
            ColumnSpec("order_to_payment_rate", "DOUBLE PRECISION", True),
        ),
        "event_date", ("product_views", "cart_adds", "checkouts_started", "orders_created", "payments_completed", "orders_delivered", "refunds"),
        ("view_to_cart_rate", "cart_to_checkout_rate", "checkout_to_order_rate", "order_to_payment_rate"),
    ),
)


def connection_kwargs(environ: Mapping[str, str] | None = None) -> dict[str, str | int]:
    environment = os.environ if environ is None else environ
    return {
        "dbname": environment.get("WAREHOUSE_DB", "pulse_analytics"),
        "user": environment.get("WAREHOUSE_USER", "pulse"),
        "password": environment.get("WAREHOUSE_PASSWORD", "pulse-local-development-only"),
        "host": environment.get("WAREHOUSE_HOST", "localhost"),
        "port": int(environment.get("WAREHOUSE_PORT", "5433")),
    }


def validate_required_columns(table: str, actual: Sequence[str], spec: TableSpec) -> None:
    missing = set(spec.required_columns) - set(actual)
    unexpected = set(actual) - set(spec.required_columns)
    if missing or unexpected:
        raise ValueError(f"Gold {table} columns mismatch; missing={sorted(missing)}, unexpected={sorted(unexpected)}")


def _create_table(cursor, table_name: str, spec: TableSpec) -> None:
    definitions = [
        sql.SQL("{} {}{}").format(
            sql.Identifier(column.name), sql.SQL(column.postgres_type),
            sql.SQL("") if column.nullable else sql.SQL(" NOT NULL"),
        ) for column in spec.columns
    ]
    cursor.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)
        )
    )


def _validate_table_data(cursor, table_name: str, spec: TableSpec) -> int:
    table = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(table))
    count = cursor.fetchone()[0]
    if count <= 0:
        raise ValueError(f"Warehouse table {spec.name} is empty")
    if spec.nonnegative_columns:
        conditions = sql.SQL(" OR ").join(
            sql.SQL("{} < 0").format(sql.Identifier(name)) for name in spec.nonnegative_columns
        )
        cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(table, conditions))
        if cursor.fetchone() is not None:
            raise ValueError(f"Warehouse table {spec.name} contains negative metrics")
    if spec.rate_columns:
        conditions = sql.SQL(" OR ").join(
            sql.SQL("({0} IS NOT NULL AND ({0} < 0 OR {0} > 1))").format(sql.Identifier(name))
            for name in spec.rate_columns
        )
        cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(table, conditions))
        if cursor.fetchone() is not None:
            raise ValueError(f"Warehouse table {spec.name} contains rates outside [0,1]")
    return count


def load_gold_to_warehouse(paths: GoldPaths | None = None) -> dict[str, int]:
    resolved_paths = load_gold_paths() if paths is None else paths
    path_by_name = {name: getattr(resolved_paths, name) for name in (spec.name for spec in TABLE_SPECS)}
    spark = build_gold_spark_session(app_name="pulse-gold-warehouse-load", master=os.getenv("SPARK_MASTER", "local[*]"))
    frames = {}
    try:
        for spec in TABLE_SPECS:
            frame = read_parquet_data_files(spark, Path(path_by_name[spec.name]))
            validate_required_columns(spec.name, frame.columns, spec)
            frames[spec.name] = frame.select(*spec.required_columns).cache()
            frames[spec.name].count()

        suffix = uuid.uuid4().hex[:12]
        counts = {}
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(WAREHOUSE_SCHEMA)))
                for spec in TABLE_SPECS:
                    staging = f"_staging_{spec.name}_{suffix}"
                    _create_table(cursor, staging, spec)
                    copy_statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                        sql.SQL(", ").join(map(sql.Identifier, spec.required_columns)),
                    )
                    with cursor.copy(copy_statement) as copy:
                        for row in frames[spec.name].toLocalIterator():
                            copy.write_row(tuple(row))
                    counts[spec.name] = _validate_table_data(cursor, staging, spec)
                for spec in TABLE_SPECS:
                    staging = f"_staging_{spec.name}_{suffix}"
                    cursor.execute("SELECT to_regclass(%s)", (f"{WAREHOUSE_SCHEMA}.{spec.name}",))
                    if cursor.fetchone()[0] is None:
                        cursor.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                            sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging), sql.Identifier(spec.name)
                        ))
                        cursor.execute(sql.SQL("CREATE INDEX {} ON {}.{} ({})").format(
                            sql.Identifier(f"idx_{spec.name}_{spec.index_column}"), sql.Identifier(WAREHOUSE_SCHEMA),
                            sql.Identifier(spec.name), sql.Identifier(spec.index_column),
                        ))
                    else:
                        target = sql.SQL("{}.{}").format(
                            sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(spec.name)
                        )
                        columns = sql.SQL(", ").join(map(sql.Identifier, spec.required_columns))
                        cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
                        cursor.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}.{}").format(
                            target, columns, columns, sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)
                        ))
                        cursor.execute(sql.SQL("DROP TABLE {}.{}").format(
                            sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)
                        ))
        return counts
    finally:
        for frame in frames.values():
            frame.unpersist(blocking=True)
        spark.stop()


def validate_warehouse() -> dict[str, int]:
    counts = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = %s", (WAREHOUSE_SCHEMA,))
            if cursor.fetchone() is None:
                raise ValueError(f"Warehouse schema {WAREHOUSE_SCHEMA} does not exist")
            for spec in TABLE_SPECS:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                    (WAREHOUSE_SCHEMA, spec.name),
                )
                columns = [row[0] for row in cursor.fetchall()]
                if not columns:
                    raise ValueError(f"Warehouse table {WAREHOUSE_SCHEMA}.{spec.name} does not exist")
                validate_required_columns(spec.name, columns, spec)
                counts[spec.name] = _validate_table_data(cursor, spec.name, spec)
    return counts


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("load", "validate"))
    args = parser.parse_args(argv)
    result = load_gold_to_warehouse() if args.command == "load" else validate_warehouse()
    print(", ".join(f"{name}={count}" for name, count in result.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
