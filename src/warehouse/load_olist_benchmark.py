"""Transactionally load Phase 6.4A benchmark Gold JSONL into PostgreSQL."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
from typing import Any, Iterable
import uuid

import psycopg
from psycopg import sql

from src.benchmarks.olist import OLIST_BUSINESS_ID, PROJECT_ROOT
from src.warehouse.load_gold import WAREHOUSE_SCHEMA, connection_kwargs


TABLES = {
    "olist_orders_by_status": (
        ("business_id", "TEXT", False), ("native_status", "TEXT", False),
        ("order_count", "BIGINT", False),
    ),
    "olist_commerce_daily": (
        ("business_id", "TEXT", False), ("event_date", "DATE", False),
        ("currency", "TEXT", False), ("orders", "BIGINT", False),
        ("merchandise_value", "DOUBLE PRECISION", False),
        ("customer_freight_charge", "DOUBLE PRECISION", False),
        ("commerce_calculated_total", "DOUBLE PRECISION", False),
        ("payment_total", "DOUBLE PRECISION", False),
    ),
    "olist_payment_methods": (
        ("business_id", "TEXT", False), ("payment_method", "TEXT", False),
        ("currency", "TEXT", False), ("payment_rows", "BIGINT", False),
        ("payment_amount", "DOUBLE PRECISION", False),
    ),
    "olist_data_quality": (
        ("business_id", "TEXT", False), ("code", "TEXT", False),
        ("severity", "TEXT", False), ("classification", "TEXT", False),
        ("issue_count", "BIGINT", False), ("sample_ids_json", "TEXT", False),
    ),
    "olist_economic_completeness": (
        ("business_id", "TEXT", False), ("currency", "TEXT", False),
        ("economic_status", "TEXT", False), ("order_count", "BIGINT", False),
        ("cogs_available", "BOOLEAN", False), ("attribution_available", "BOOLEAN", False),
        ("profit_calculated", "BOOLEAN", False),
    ),
}

FILES = {
    "olist_orders_by_status": "orders_by_status.jsonl",
    "olist_commerce_daily": "commerce_daily.jsonl",
    "olist_payment_methods": "payment_methods.jsonl",
    "olist_data_quality": "data_quality.jsonl",
    "olist_economic_completeness": "economic_completeness.jsonl",
}

GRAINS = {
    "olist_orders_by_status": ("business_id", "native_status"),
    "olist_commerce_daily": ("business_id", "event_date", "currency"),
    "olist_payment_methods": ("business_id", "payment_method", "currency"),
    "olist_data_quality": ("business_id", "code"),
    "olist_economic_completeness": ("business_id", "currency", "economic_status"),
}


def _read(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"Benchmark Gold file is missing: {path.name}; run the benchmark first")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def benchmark_rows(root: Path) -> dict[str, list[dict[str, Any]]]:
    output = {}
    for table, filename in FILES.items():
        rows = _read(root / "gold" / filename)
        if table == "olist_data_quality":
            rows = [{"business_id": OLIST_BUSINESS_ID, "code": row["code"],
                     "severity": row["severity"], "classification": row["classification"],
                     "issue_count": row["count"],
                     "sample_ids_json": json.dumps(row.get("sample_ids", []), separators=(",", ":"))}
                    for row in rows]
        output[table] = rows
    return output


def _create(cursor, table_name: str, columns) -> None:
    definitions = [sql.SQL("{} {}{}").format(sql.Identifier(name), sql.SQL(kind),
                   sql.SQL("") if nullable else sql.SQL(" NOT NULL"))
                   for name, kind, nullable in columns]
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)))


def _value(value: Any, postgres_type: str) -> Any:
    if postgres_type == "DATE" and isinstance(value, str):
        return date.fromisoformat(value)
    if postgres_type == "BIGINT" and value is not None:
        return int(value)
    return value


def load_olist_benchmark(root: Path | None = None) -> dict[str, int]:
    selected = Path(root or (PROJECT_ROOT / "data" / "benchmarks" / "olist" / "full")).resolve()
    all_rows = benchmark_rows(selected)
    suffix = uuid.uuid4().hex[:12]
    counts: dict[str, int] = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(WAREHOUSE_SCHEMA)))
            for table, columns in TABLES.items():
                rows = all_rows[table]
                staging = f"_staging_{table}_{suffix}"
                _create(cursor, staging, columns)
                names = tuple(name for name, _, _ in columns)
                copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                    sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                    sql.SQL(", ").join(map(sql.Identifier, names)))
                with cursor.copy(copy_sql) as copy:
                    for row in rows:
                        copy.write_row(tuple(_value(row.get(name), kind) for name, kind, _ in columns))
                grain = sql.SQL(", ").join(map(sql.Identifier, GRAINS[table]))
                staging_ref = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging))
                cursor.execute(sql.SQL("SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(
                    staging_ref, grain))
                if cursor.fetchone():
                    raise ValueError(f"Benchmark table {table} contains duplicate grain")
                cursor.execute("SELECT to_regclass(%s)", (f"{WAREHOUSE_SCHEMA}.{table}",))
                if cursor.fetchone()[0] is None:
                    cursor.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging), sql.Identifier(table)))
                else:
                    target = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table))
                    cursor.execute(sql.SQL("DELETE FROM {} WHERE business_id = %s").format(target),
                                   (OLIST_BUSINESS_ID,))
                    columns_sql = sql.SQL(", ").join(map(sql.Identifier, names))
                    cursor.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                        target, columns_sql, columns_sql, staging_ref))
                    cursor.execute(sql.SQL("DROP TABLE {}").format(staging_ref))
                counts[table] = len(rows)
    return counts


def validate_olist_benchmark() -> dict[str, int]:
    counts: dict[str, int] = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            for table, columns in TABLES.items():
                reference = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table))
                cursor.execute(sql.SQL("SELECT count(*) FROM {} WHERE business_id = %s").format(reference),
                               (OLIST_BUSINESS_ID,))
                counts[table] = int(cursor.fetchone()[0])
                if counts[table] <= 0:
                    raise ValueError(f"Benchmark table {table} has no Olist rows")
                grain = sql.SQL(", ").join(map(sql.Identifier, GRAINS[table]))
                cursor.execute(sql.SQL("SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(
                    reference, grain))
                if cursor.fetchone():
                    raise ValueError(f"Benchmark table {table} contains duplicate grain")
                names = {name for name, _, _ in columns}
                if "currency" in names:
                    cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE currency !~ '^[A-Z]{{3}}$' LIMIT 1").format(reference))
                    if cursor.fetchone():
                        raise ValueError(f"Benchmark table {table} contains invalid currency")
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("load", "validate"))
    parser.add_argument("--root", type=Path)
    args = parser.parse_args(argv)
    output = load_olist_benchmark(args.root) if args.command == "load" else validate_olist_benchmark()
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
