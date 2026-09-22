"""Load sanitized Phase 6.5B TikTok and campaign-outcome aggregates."""

from __future__ import annotations

import argparse
import json
import uuid

import psycopg
from psycopg import sql

from src.pilots.sama_tiktok import load_private_tiktok
from src.warehouse.load_gold import WAREHOUSE_SCHEMA, connection_kwargs


TABLES = {
    "sama_pilot_tiktok_ad_daily": (
        ("business_id", "TEXT", False), ("source_type", "TEXT", False),
        ("source_id", "TEXT", False), ("record_id", "TEXT", False),
        ("account_id", "TEXT", False), ("account_name", "TEXT", True),
        ("campaign_id", "TEXT", False), ("campaign_name", "TEXT", True),
        ("ad_group_id", "TEXT", False), ("ad_group_name", "TEXT", True),
        ("ad_id", "TEXT", False), ("ad_name", "TEXT", True),
        ("report_date", "DATE", False), ("reporting_timezone", "TEXT", False),
        ("currency", "TEXT", False), ("spend", "DOUBLE PRECISION", False),
        ("impressions", "BIGINT", False), ("reach", "BIGINT", True),
        ("frequency", "DOUBLE PRECISION", True), ("clicks", "BIGINT", False),
        ("platform_conversions", "DOUBLE PRECISION", False),
        ("platform_conversion_value", "DOUBLE PRECISION", False),
        ("link_clicks", "BIGINT", True), ("video_views", "BIGINT", True),
        ("details_json", "TEXT", False),
    ),
    "sama_pilot_tiktok_order_outcomes_daily": (
        ("business_id", "TEXT", False), ("campaign_id", "TEXT", False),
        ("campaign_name", "TEXT", True), ("report_date", "DATE", False),
        ("initial_orders", "BIGINT", False), ("matched_orders", "BIGINT", False),
        ("ambiguous_orders", "BIGINT", False), ("unmatched_orders", "BIGINT", False),
        ("confirmed_orders", "BIGINT", False), ("shipped_orders", "BIGINT", False),
        ("delivered_orders", "BIGINT", False), ("returned_orders", "BIGINT", False),
        ("out_of_stock_orders", "BIGINT", False),
    ),
    "sama_pilot_tiktok_data_quality": (
        ("business_id", "TEXT", False), ("check_name", "TEXT", False),
        ("category", "TEXT", False), ("issue_count", "BIGINT", False),
        ("observed_value", "DOUBLE PRECISION", True),
        ("expected_value", "DOUBLE PRECISION", True), ("status", "TEXT", False),
    ),
}

GRAINS = {
    "sama_pilot_tiktok_ad_daily": (
        "business_id", "account_id", "campaign_id", "ad_group_id", "ad_id", "report_date",
    ),
    "sama_pilot_tiktok_order_outcomes_daily": (
        "business_id", "campaign_id", "report_date",
    ),
    "sama_pilot_tiktok_data_quality": ("business_id", "check_name"),
}


def _create(cursor, table_name: str, columns) -> None:
    definitions = [sql.SQL("{} {}{}").format(
        sql.Identifier(name), sql.SQL(type_name), sql.SQL("") if nullable else sql.SQL(" NOT NULL")
    ) for name, type_name, nullable in columns]
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)
    ))


def _validate(cursor, table_name: str, columns, *, contract_name: str | None = None) -> int:
    logical_name = contract_name or table_name
    table = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(table))
    count = cursor.fetchone()[0]
    if count <= 0:
        raise ValueError(f"Warehouse table {logical_name} is empty")
    grain = sql.SQL(", ").join(sql.Identifier(name) for name in GRAINS[logical_name])
    cursor.execute(sql.SQL("SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(
        table, grain
    ))
    if cursor.fetchone():
        raise ValueError(f"Warehouse table {logical_name} contains duplicate grain")
    names = {name for name, _, _ in columns}
    forbidden = {"phone", "email", "customer_name", "address", "tracking_number"}
    if names & forbidden:
        raise ValueError(f"Warehouse table {logical_name} exposes a forbidden PII column")
    if logical_name == "sama_pilot_tiktok_ad_daily":
        cursor.execute(sql.SQL(
            "SELECT 1 FROM {} WHERE currency <> 'USD' OR spend < 0 OR impressions < 0 "
            "OR clicks < 0 OR platform_conversions < 0 OR clicks > impressions "
            "OR platform_conversion_value <> 0 LIMIT 1"
        ).format(table))
        if cursor.fetchone():
            raise ValueError("TikTok ad daily table violates currency, metric, or value safeguards")
    elif logical_name == "sama_pilot_tiktok_order_outcomes_daily":
        metric_names = tuple(name for name, _, _ in columns if name.endswith("_orders"))
        condition = sql.SQL(" OR ").join(
            sql.SQL("{} < 0").format(sql.Identifier(name)) for name in metric_names
        )
        cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(table, condition))
        if cursor.fetchone():
            raise ValueError("TikTok campaign outcome table contains negative counts")
    return count


def _rows(result):
    ad_rows = tuple({
        "business_id": row["business_id"],
        "source_type": row["source_type"],
        "source_id": row["source_id"],
        "record_id": row["record_id"],
        "account_id": row["account_id"],
        "account_name": row["details"].get("account_name"),
        "campaign_id": row["campaign_id"],
        "campaign_name": row["campaign_name"],
        "ad_group_id": row["ad_group_id"],
        "ad_group_name": row["ad_group_name"],
        "ad_id": row["ad_id"],
        "ad_name": row["ad_name"],
        "report_date": row["report_date"],
        "reporting_timezone": row["reporting_timezone"],
        "currency": row["currency"],
        "spend": row["spend"],
        "impressions": row["impressions"],
        "reach": row["reach"],
        "frequency": row["frequency"],
        "clicks": row["clicks"],
        "platform_conversions": row["platform_conversions"],
        "platform_conversion_value": row["platform_conversion_value"],
        "link_clicks": row["link_clicks"],
        "video_views": row["video_views"],
        "details_json": json.dumps(row["details"], sort_keys=True, separators=(",", ":")),
    } for row in result.marketing_records)
    return {
        "sama_pilot_tiktok_ad_daily": ad_rows,
        "sama_pilot_tiktok_order_outcomes_daily": result.order_outcomes_daily,
        "sama_pilot_tiktok_data_quality": result.data_quality,
    }


def load_sama_tiktok_to_warehouse() -> dict[str, int]:
    """Idempotently replace only the three sanitized Phase 6.5B source tables."""
    rows = _rows(load_private_tiktok())
    suffix = uuid.uuid4().hex[:12]
    counts: dict[str, int] = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(WAREHOUSE_SCHEMA)
            ))
            staging_names = {}
            for table_name, columns in TABLES.items():
                staging = f"_staging_{table_name}_{suffix}"
                staging_names[table_name] = staging
                _create(cursor, staging, columns)
                column_names = tuple(name for name, _, _ in columns)
                copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                    sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                    sql.SQL(", ").join(sql.Identifier(name) for name in column_names),
                )
                with cursor.copy(copy_sql) as copy:
                    for row in rows[table_name]:
                        copy.write_row(tuple(row.get(name) for name in column_names))
                counts[table_name] = _validate(cursor, staging, columns, contract_name=table_name)
            for table_name, columns in TABLES.items():
                staging = staging_names[table_name]
                cursor.execute("SELECT to_regclass(%s)", (f"{WAREHOUSE_SCHEMA}.{table_name}",))
                if cursor.fetchone()[0] is None:
                    cursor.execute(sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                        sql.Identifier(table_name),
                    ))
                else:
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                        (WAREHOUSE_SCHEMA, table_name),
                    )
                    existing = tuple(row[0] for row in cursor.fetchall())
                    expected = tuple(name for name, _, _ in columns)
                    if existing != expected:
                        raise ValueError(f"Warehouse table {table_name} schema does not match pilot contract")
                    target = sql.SQL("{}.{}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name)
                    )
                    selected = sql.SQL(", ").join(sql.Identifier(name) for name in expected)
                    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
                    cursor.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}.{}").format(
                        target, selected, selected, sql.Identifier(WAREHOUSE_SCHEMA),
                        sql.Identifier(staging),
                    ))
                    cursor.execute(sql.SQL("DROP TABLE {}.{}").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging)
                    ))
                target = sql.SQL("{}.{}").format(
                    sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name)
                )
                for column in GRAINS[table_name]:
                    cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                        sql.Identifier(f"idx_{table_name}_{column}"), target, sql.Identifier(column)
                    ))
    return counts


def validate_sama_tiktok_warehouse() -> dict[str, int]:
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
    output = (load_sama_tiktok_to_warehouse() if args.command == "load"
              else validate_sama_tiktok_warehouse())
    print(", ".join(f"{name}={count}" for name, count in output.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
