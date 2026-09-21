"""Load sanitized Phase 6.5A pilot facts into the analytics warehouse."""

from __future__ import annotations

import argparse
import uuid

import psycopg
from psycopg import sql

from src.pilots.sama import load_private_pilot
from src.warehouse.load_gold import WAREHOUSE_SCHEMA, connection_kwargs


TABLES = {
    "sama_pilot_order_facts": (
        ("business_id", "TEXT", False), ("lightfunnel_order_id", "TEXT", False),
        ("cod_lead_id", "TEXT", True), ("cod_order_reference", "TEXT", True),
        ("intent_created_at", "TIMESTAMP WITH TIME ZONE", True), ("match_status", "TEXT", False),
        ("lead_status", "TEXT", True), ("final_order_status", "TEXT", True),
        ("initial_quantity", "BIGINT", True), ("initial_total", "DOUBLE PRECISION", True),
        ("initial_currency", "TEXT", True), ("final_quantity", "BIGINT", True),
        ("final_total", "DOUBLE PRECISION", True), ("currency", "TEXT", True),
        ("quantity_changed", "BOOLEAN", False), ("value_changed", "BOOLEAN", False),
        ("confirmed", "BOOLEAN", False), ("shipped", "BOOLEAN", False),
        ("delivered", "BOOLEAN", False), ("returned", "BOOLEAN", False),
        ("out_of_stock", "BOOLEAN", False), ("cancelled", "BOOLEAN", False),
        ("cash_collected", "DOUBLE PRECISION", False), ("cod_fee_native", "DOUBLE PRECISION", False),
        ("product_cogs_usd", "DOUBLE PRECISION", False), ("call_center_cost_usd", "DOUBLE PRECISION", False),
        ("logistics_cost_usd", "DOUBLE PRECISION", False),
        ("known_operational_cost_usd", "DOUBLE PRECISION", False),
        ("economic_status", "TEXT", False), ("native_sku", "TEXT", True),
        ("product_family", "TEXT", False), ("source_item_price", "DOUBLE PRECISION", True),
        ("compare_at_value", "DOUBLE PRECISION", True), ("utm_source", "TEXT", True),
        ("utm_medium", "TEXT", True), ("utm_campaign", "TEXT", True),
        ("utm_content", "TEXT", True), ("utm_term", "TEXT", True),
    ),
    "sama_pilot_identity_resolution": (
        ("business_id", "TEXT", False), ("lightfunnel_order_id", "TEXT", False),
        ("cod_lead_id", "TEXT", True), ("match_status", "TEXT", False),
        ("match_method", "TEXT", False), ("confidence", "DOUBLE PRECISION", False),
        ("time_delta_minutes", "DOUBLE PRECISION", True), ("linkage_key", "TEXT", True),
    ),
    "sama_pilot_data_quality": (
        ("business_id", "TEXT", False), ("check_name", "TEXT", False),
        ("category", "TEXT", False), ("issue_count", "BIGINT", False),
    ),
}

GRAINS = {
    "sama_pilot_order_facts": ("business_id", "lightfunnel_order_id"),
    "sama_pilot_identity_resolution": ("business_id", "lightfunnel_order_id"),
    "sama_pilot_data_quality": ("business_id", "check_name"),
}


def _create(cursor, table_name: str, columns) -> None:
    definitions = [sql.SQL("{} {}{}").format(sql.Identifier(name), sql.SQL(type_name),
                   sql.SQL("") if nullable else sql.SQL(" NOT NULL"))
                   for name, type_name, nullable in columns]
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)))


def _validate(cursor, table_name: str, columns, *, contract_name: str | None = None) -> int:
    logical_name = contract_name or table_name
    table = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(table))
    count = cursor.fetchone()[0]
    if count <= 0:
        raise ValueError(f"Warehouse table {table_name} is empty")
    grain = sql.SQL(", ").join(sql.Identifier(name) for name in GRAINS[logical_name])
    cursor.execute(sql.SQL("SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(table, grain))
    if cursor.fetchone():
        raise ValueError(f"Warehouse table {table_name} contains duplicate grain")
    names = {name for name, _, _ in columns}
    forbidden = {"phone", "email", "customer_name", "address", "tracking_number"}
    if names & forbidden:
        raise ValueError(f"Warehouse table {table_name} exposes a forbidden PII column")
    if logical_name == "sama_pilot_identity_resolution":
        cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE linkage_key IS NOT NULL AND linkage_key !~ %s LIMIT 1").format(table),
                       (r"^[0-9a-f]{64}$",))
        if cursor.fetchone():
            raise ValueError("Identity linkage keys must be HMAC-SHA256 hex digests")
    return count


def _rows(result):
    return {
        "sama_pilot_order_facts": result.order_facts,
        "sama_pilot_identity_resolution": result.identity_resolution,
        "sama_pilot_data_quality": result.data_quality,
    }


def load_sama_pilot_to_warehouse() -> dict[str, int]:
    """Idempotently replace only the three sanitized pilot source tables."""
    result = load_private_pilot()
    rows = _rows(result)
    suffix = uuid.uuid4().hex[:12]
    counts: dict[str, int] = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(WAREHOUSE_SCHEMA)))
            staging_names = {}
            for table_name, columns in TABLES.items():
                staging = f"_staging_{table_name}_{suffix}"
                staging_names[table_name] = staging
                _create(cursor, staging, columns)
                column_names = tuple(name for name, _, _ in columns)
                copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                    sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                    sql.SQL(", ").join(sql.Identifier(name) for name in column_names))
                with cursor.copy(copy_sql) as copy:
                    for row in rows[table_name]:
                        copy.write_row(tuple(row.get(name) for name in column_names))
                counts[table_name] = _validate(cursor, staging, columns, contract_name=table_name)
            for table_name, columns in TABLES.items():
                staging = staging_names[table_name]
                cursor.execute("SELECT to_regclass(%s)", (f"{WAREHOUSE_SCHEMA}.{table_name}",))
                if cursor.fetchone()[0] is None:
                    cursor.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging), sql.Identifier(table_name)))
                else:
                    cursor.execute("SELECT column_name FROM information_schema.columns "
                                   "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                                   (WAREHOUSE_SCHEMA, table_name))
                    existing = tuple(row[0] for row in cursor.fetchall())
                    expected = tuple(name for name, _, _ in columns)
                    if existing != expected:
                        raise ValueError(f"Warehouse table {table_name} schema does not match pilot contract")
                    target = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
                    selected = sql.SQL(", ").join(sql.Identifier(name) for name in expected)
                    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
                    cursor.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}.{}").format(
                        target, selected, selected, sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)))
                    cursor.execute(sql.SQL("DROP TABLE {}.{}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)))
                target = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
                for column in GRAINS[table_name]:
                    cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                        sql.Identifier(f"idx_{table_name}_{column}"), target, sql.Identifier(column)))
    return counts


def validate_sama_pilot_warehouse() -> dict[str, int]:
    counts = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            for table_name, columns in TABLES.items():
                counts[table_name] = _validate(cursor, table_name, columns)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("load", "validate"))
    args = parser.parse_args(argv)
    output = load_sama_pilot_to_warehouse() if args.command == "load" else validate_sama_pilot_warehouse()
    print(", ".join(f"{name}={count}" for name, count in output.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
