"""Transactionally load Phase 6.2 operational Gold snapshots into PostgreSQL."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import uuid

import psycopg
from psycopg import sql

from src.operations.pipeline import (GOLD_SCHEMAS, OPERATIONS_GOLD_TABLES,
                                     build_operations_spark_session, load_operations_paths)
from src.utils.parquet import read_parquet_data_files
from src.warehouse.load_gold import (ColumnSpec, TableSpec, WAREHOUSE_SCHEMA,
                                     connection_kwargs, validate_required_columns)


OPERATIONS_GRAINS = {
    "order_operations_current": ("business_id", "order_id"),
    "order_operations_daily": ("business_id", "event_date", "reporting_timezone", "currency", "payment_type"),
    "confirmation_performance": ("business_id", "cohort_date", "provider", "currency", "payment_type"),
    "delivery_performance": ("business_id", "cohort_date", "courier", "currency", "payment_type"),
    "cod_collection_performance": ("business_id", "collection_date", "provider", "currency"),
    "remittance_performance": ("business_id", "period_end", "provider", "currency", "settlement_status"),
}


def _table_spec(name):
    type_names = {"string": "TEXT", "date": "DATE", "bigint": "BIGINT", "double": "DOUBLE PRECISION",
                  "timestamp": "TIMESTAMP WITH TIME ZONE"}
    columns = tuple(ColumnSpec(field.name, type_names[field.dataType.simpleString()], field.nullable)
                    for field in GOLD_SCHEMAS[name].fields)
    nonnegative = tuple(field.name for field in GOLD_SCHEMAS[name].fields if field.name in {
        "order_value", "delivery_attempts", "shipment_count", "cash_expected", "cash_collected",
        "orders_created", "confirmed_orders", "shipped_orders", "delivered_orders", "refused_orders",
        "returned_orders", "unreachable_orders", "eligible_orders", "rejected_orders",
        "average_time_to_confirm_hours", "average_time_to_ship_hours", "average_time_to_deliver_hours",
        "average_delivery_attempts", "collection_count", "remittance_count", "gross_collected",
        "provider_fees", "shipping_fees", "cod_fees", "net_remitted", "remittance_pending_amount",
    })
    rates = tuple(field.name for field in GOLD_SCHEMAS[name].fields if field.name.endswith("_rate"))
    index = next((candidate for candidate in ("event_date", "cohort_date", "collection_date", "period_end",
                                               "order_created_at") if candidate in GOLD_SCHEMAS[name].fieldNames()),
                 "business_id")
    return TableSpec(name, columns, index, nonnegative, rates)


OPERATIONS_TABLE_SPECS = tuple(_table_spec(name) for name in OPERATIONS_GOLD_TABLES)


def _create(cursor, table_name, spec):
    definitions = [sql.SQL("{} {}{}").format(sql.Identifier(column.name), sql.SQL(column.postgres_type),
        sql.SQL("") if column.nullable else sql.SQL(" NOT NULL")) for column in spec.columns]
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)))


def _validate(cursor, table_name, spec):
    table = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(table)); count = cursor.fetchone()[0]
    if count <= 0: raise ValueError(f"Warehouse table {spec.name} is empty")
    if "currency" in spec.required_columns:
        cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE currency !~ '^[A-Z]{{3}}$' LIMIT 1").format(table))
        if cursor.fetchone(): raise ValueError(f"Warehouse table {spec.name} contains invalid currency")
    if spec.nonnegative_columns:
        conditions = sql.SQL(" OR ").join(sql.SQL("{} < 0").format(sql.Identifier(name))
                                         for name in spec.nonnegative_columns)
        cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(table, conditions))
        if cursor.fetchone(): raise ValueError(f"Warehouse table {spec.name} contains negative metrics")
    grain = sql.SQL(", ").join(map(sql.Identifier, OPERATIONS_GRAINS[spec.name]))
    cursor.execute(sql.SQL("SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(table, grain))
    if cursor.fetchone(): raise ValueError(f"Warehouse table {spec.name} contains duplicate grain")
    return count


def load_operations_to_warehouse(paths=None):
    selected = paths or load_operations_paths()
    spark = build_operations_spark_session(app_name="pulse-operations-warehouse",
                                           master=os.getenv("SPARK_MASTER", "local[*]"))
    frames = {}
    try:
        for spec in OPERATIONS_TABLE_SPECS:
            frame = read_parquet_data_files(spark, Path(getattr(selected, spec.name)))
            validate_required_columns(spec.name, frame.columns, spec)
            frames[spec.name] = frame.select(*spec.required_columns).cache(); frames[spec.name].count()
        suffix, counts = uuid.uuid4().hex[:12], {}
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(WAREHOUSE_SCHEMA)))
                for spec in OPERATIONS_TABLE_SPECS:
                    staging = f"_staging_{spec.name}_{suffix}"; _create(cursor, staging, spec)
                    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                        sql.SQL(", ").join(map(sql.Identifier, spec.required_columns)))
                    with cursor.copy(copy_sql) as copy:
                        for row in frames[spec.name].toLocalIterator(): copy.write_row(tuple(row))
                    counts[spec.name] = _validate(cursor, staging, spec)
                for spec in OPERATIONS_TABLE_SPECS:
                    staging = f"_staging_{spec.name}_{suffix}"
                    cursor.execute("SELECT to_regclass(%s)", (f"{WAREHOUSE_SCHEMA}.{spec.name}",))
                    if cursor.fetchone()[0] is None:
                        cursor.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                            sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging), sql.Identifier(spec.name)))
                    else:
                        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                                       (WAREHOUSE_SCHEMA, spec.name))
                        validate_required_columns(spec.name, [row[0] for row in cursor.fetchall()], spec)
                        target = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(spec.name))
                        columns = sql.SQL(", ").join(map(sql.Identifier, spec.required_columns))
                        cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
                        cursor.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}.{}").format(
                            target, columns, columns, sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)))
                        cursor.execute(sql.SQL("DROP TABLE {}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)))
                    target = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(spec.name))
                    for column in ("business_id", spec.index_column):
                        cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                            sql.Identifier(f"idx_{spec.name}_{column}"), target, sql.Identifier(column)))
        return counts
    finally:
        for frame in frames.values(): frame.unpersist(blocking=True)
        spark.stop()


def validate_operations_warehouse():
    counts = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            for spec in OPERATIONS_TABLE_SPECS:
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                               (WAREHOUSE_SCHEMA, spec.name))
                validate_required_columns(spec.name, [row[0] for row in cursor.fetchall()], spec)
                counts[spec.name] = _validate(cursor, spec.name, spec)
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("command", choices=("load", "validate"))
    args = parser.parse_args(argv)
    output = load_operations_to_warehouse() if args.command == "load" else validate_operations_warehouse()
    print(", ".join(f"{name}={count}" for name, count in output.items())); return 0


if __name__ == "__main__": raise SystemExit(main())
