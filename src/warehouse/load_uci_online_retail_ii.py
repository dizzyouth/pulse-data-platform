"""Transactional warehouse loader for UCI Online Retail II benchmark Gold."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any
import uuid

import psycopg
from psycopg import sql

from src.benchmarks.uci_online_retail_ii import PROJECT_ROOT, UCI_BUSINESS_ID
from src.warehouse.load_gold import WAREHOUSE_SCHEMA, connection_kwargs


TABLES = {
    "uci_retail_daily": (
        ("business_id", "TEXT", False), ("event_date", "DATE", False),
        ("currency", "TEXT", False), ("invoice_count", "BIGINT", False),
        ("normal_invoice_count", "BIGINT", False),
        ("cancellation_invoice_count", "BIGINT", False),
        ("anonymous_customer_invoice_count", "BIGINT", False),
        ("line_count", "BIGINT", False),
        ("positive_merchandise_value", "DOUBLE PRECISION", False),
        ("cancellation_value", "DOUBLE PRECISION", False),
        ("adjustment_value", "DOUBLE PRECISION", False),
        ("non_merchandise_value", "DOUBLE PRECISION", False),
        ("net_ledger_value", "DOUBLE PRECISION", False),
        ("zero_price_line_count", "BIGINT", False),
        ("negative_price_line_count", "BIGINT", False),
    ),
    "uci_invoice_summary": (
        ("business_id", "TEXT", False), ("scoped_invoice_id", "TEXT", False),
        ("worksheet", "TEXT", False), ("raw_invoice_id", "TEXT", False),
        ("native_invoice_type", "TEXT", False), ("invoice_at", "TIMESTAMPTZ", True),
        ("currency", "TEXT", False), ("country", "TEXT", False),
        ("anonymous_customer", "BOOLEAN", False), ("line_count", "BIGINT", False),
        ("positive_merchandise_value", "DOUBLE PRECISION", False),
        ("cancellation_value", "DOUBLE PRECISION", False),
        ("adjustment_value", "DOUBLE PRECISION", False),
        ("non_merchandise_value", "DOUBLE PRECISION", False),
        ("net_ledger_value", "DOUBLE PRECISION", False),
        ("commerce_projection_eligible", "BOOLEAN", False),
        ("projected_order_value", "DOUBLE PRECISION", False),
        ("projected_line_count", "BIGINT", False),
    ),
    "uci_line_classification": (
        ("business_id", "TEXT", False), ("line_classification", "TEXT", False),
        ("currency", "TEXT", False), ("line_count", "BIGINT", False),
        ("signed_line_value", "DOUBLE PRECISION", False),
    ),
    "uci_country_distribution": (
        ("business_id", "TEXT", False), ("country", "TEXT", False),
        ("currency", "TEXT", False), ("line_count", "BIGINT", False),
        ("invoice_count", "BIGINT", False),
        ("net_ledger_value", "DOUBLE PRECISION", False),
    ),
    "uci_data_quality": (
        ("business_id", "TEXT", False), ("code", "TEXT", False),
        ("severity", "TEXT", False), ("classification", "TEXT", False),
        ("issue_count", "BIGINT", False), ("sample_ids_json", "TEXT", False),
    ),
    "uci_economic_completeness": (
        ("business_id", "TEXT", False), ("currency", "TEXT", False),
        ("economic_status", "TEXT", False), ("invoice_count", "BIGINT", False),
        ("cogs_available", "BOOLEAN", False),
        ("merchant_shipping_cost_available", "BOOLEAN", False),
        ("attribution_available", "BOOLEAN", False), ("cod_available", "BOOLEAN", False),
        ("remittance_available", "BOOLEAN", False), ("profit_calculated", "BOOLEAN", False),
    ),
}

FILES = {
    "uci_retail_daily": "retail_daily.jsonl",
    "uci_invoice_summary": "invoice_summary.jsonl",
    "uci_line_classification": "line_classification.jsonl",
    "uci_country_distribution": "country_distribution.jsonl",
    "uci_data_quality": "data_quality.jsonl",
    "uci_economic_completeness": "economic_completeness.jsonl",
}

GRAINS = {
    "uci_retail_daily": ("business_id", "event_date", "currency"),
    "uci_invoice_summary": ("business_id", "scoped_invoice_id"),
    "uci_line_classification": ("business_id", "line_classification", "currency"),
    "uci_country_distribution": ("business_id", "country", "currency"),
    "uci_data_quality": ("business_id", "code"),
    "uci_economic_completeness": ("business_id", "currency", "economic_status"),
}


def _read(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"Benchmark Gold file is missing: {path.name}; run the benchmark first")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def benchmark_rows(root: Path) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for table, filename in FILES.items():
        rows = _read(root / "gold" / filename)
        if table == "uci_data_quality":
            rows = [{"business_id": UCI_BUSINESS_ID, "code": row["code"],
                     "severity": row["severity"], "classification": row["classification"],
                     "issue_count": row["count"],
                     "sample_ids_json": json.dumps(row.get("sample_ids", []), separators=(",", ":"))}
                    for row in rows]
        output[table] = rows
    return output


def _create(cursor, table_name: str, columns) -> None:
    definitions = [sql.SQL("{} {}{}").format(
        sql.Identifier(name), sql.SQL(kind), sql.SQL("") if nullable else sql.SQL(" NOT NULL"))
        for name, kind, nullable in columns]
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)))


def _value(value: Any, postgres_type: str) -> Any:
    if postgres_type == "DATE" and isinstance(value, str):
        return date.fromisoformat(value)
    if postgres_type == "TIMESTAMPTZ" and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if postgres_type == "BIGINT" and value is not None:
        return int(value)
    return value


def load_uci_benchmark(root: Path | None = None) -> dict[str, int]:
    selected = Path(root or (PROJECT_ROOT / "data" / "benchmarks" /
                             "uci_online_retail_ii" / "full")).resolve()
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
                        copy.write_row(tuple(_value(row.get(name), kind)
                                             for name, kind, _ in columns))
                grain = sql.SQL(", ").join(map(sql.Identifier, GRAINS[table]))
                staging_ref = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA),
                                                       sql.Identifier(staging))
                cursor.execute(sql.SQL(
                    "SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(
                        staging_ref, grain))
                if cursor.fetchone():
                    raise ValueError(f"Benchmark table {table} contains duplicate grain")
                cursor.execute("SELECT to_regclass(%s)", (f"{WAREHOUSE_SCHEMA}.{table}",))
                if cursor.fetchone()[0] is None:
                    cursor.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging), sql.Identifier(table)))
                else:
                    target = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA),
                                                     sql.Identifier(table))
                    cursor.execute(sql.SQL("DELETE FROM {} WHERE business_id = %s").format(target),
                                   (UCI_BUSINESS_ID,))
                    columns_sql = sql.SQL(", ").join(map(sql.Identifier, names))
                    cursor.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
                        target, columns_sql, columns_sql, staging_ref))
                    cursor.execute(sql.SQL("DROP TABLE {}").format(staging_ref))
                counts[table] = len(rows)
    return counts


def validate_uci_benchmark() -> dict[str, int]:
    counts: dict[str, int] = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            for table, columns in TABLES.items():
                reference = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA),
                                                     sql.Identifier(table))
                cursor.execute(sql.SQL("SELECT count(*) FROM {} WHERE business_id = %s").format(reference),
                               (UCI_BUSINESS_ID,))
                counts[table] = int(cursor.fetchone()[0])
                if counts[table] <= 0:
                    raise ValueError(f"Benchmark table {table} has no UCI rows")
                grain = sql.SQL(", ").join(map(sql.Identifier, GRAINS[table]))
                cursor.execute(sql.SQL(
                    "SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(
                        reference, grain))
                if cursor.fetchone():
                    raise ValueError(f"Benchmark table {table} contains duplicate grain")
                if "currency" in {name for name, _, _ in columns}:
                    cursor.execute(sql.SQL(
                        "SELECT 1 FROM {} WHERE currency <> 'GBP' LIMIT 1").format(reference))
                    if cursor.fetchone():
                        raise ValueError(f"Benchmark table {table} contains non-GBP rows")
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("load", "validate"))
    parser.add_argument("--root", type=Path)
    args = parser.parse_args(argv)
    output = load_uci_benchmark(args.root) if args.command == "load" else validate_uci_benchmark()
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
