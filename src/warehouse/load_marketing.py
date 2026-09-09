"""Transactionally load Phase 6.1 marketing Gold snapshots into PostgreSQL."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import uuid

import psycopg
from psycopg import sql

from src.marketing.pipeline import GOLD_SCHEMAS, MARKETING_GOLD_TABLES, build_marketing_spark_session, load_marketing_paths
from src.utils.parquet import read_parquet_data_files
from src.warehouse.load_gold import ColumnSpec, TableSpec, WAREHOUSE_SCHEMA, connection_kwargs, validate_required_columns


def _table_spec(name: str) -> TableSpec:
    spark_schema = GOLD_SCHEMAS[name]
    type_names = {"string": "TEXT", "date": "DATE", "bigint": "BIGINT", "double": "DOUBLE PRECISION"}
    columns = tuple(ColumnSpec(field.name, type_names[field.dataType.simpleString()], field.nullable)
                    for field in spark_schema.fields)
    nonnegative = tuple(field.name for field in spark_schema.fields if field.name in {
        "spend", "impressions", "clicks", "platform_conversions", "platform_conversion_value",
        "link_clicks", "video_views", "landing_page_views", "cpc", "cpm", "cpa", "platform_roas",
    })
    return TableSpec(name, columns, "report_date", nonnegative, ("ctr",))


MARKETING_TABLE_SPECS = tuple(_table_spec(name) for name in MARKETING_GOLD_TABLES)
MARKETING_GRAINS = {
    "marketing_daily": ("business_id", "source_type", "source_id", "platform", "account_id",
                        "report_date", "reporting_timezone", "currency"),
    "campaign_performance": ("business_id", "source_type", "source_id", "platform", "account_id",
                             "campaign_id", "report_date", "reporting_timezone", "currency"),
    "ad_group_performance": ("business_id", "source_type", "source_id", "platform", "account_id",
                             "campaign_id", "ad_group_id", "report_date", "reporting_timezone", "currency"),
    "ad_performance": ("business_id", "source_type", "source_id", "platform", "account_id",
                       "campaign_id", "ad_group_id", "ad_id", "report_date", "reporting_timezone", "currency"),
}


def _create(cursor, table_name: str, spec: TableSpec) -> None:
    definitions = [sql.SQL("{} {}{}").format(
        sql.Identifier(column.name), sql.SQL(column.postgres_type),
        sql.SQL("") if column.nullable else sql.SQL(" NOT NULL"),
    ) for column in spec.columns]
    cursor.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name), sql.SQL(", ").join(definitions)))


def _validate(cursor, table_name: str, spec: TableSpec) -> int:
    table = sql.SQL("{}.{}").format(sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(table_name))
    cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(table))
    count = cursor.fetchone()[0]
    if count <= 0:
        raise ValueError(f"Warehouse table {spec.name} is empty")
    cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE currency !~ '^[A-Z]{{3}}$' LIMIT 1").format(table))
    if cursor.fetchone():
        raise ValueError(f"Warehouse table {spec.name} contains invalid currency")
    conditions = sql.SQL(" OR ").join(sql.SQL("{} < 0").format(sql.Identifier(name))
                                     for name in spec.nonnegative_columns)
    cursor.execute(sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(table, conditions))
    if cursor.fetchone():
        raise ValueError(f"Warehouse table {spec.name} contains negative metrics")
    grain = sql.SQL(", ").join(map(sql.Identifier, MARKETING_GRAINS[spec.name]))
    cursor.execute(sql.SQL("SELECT 1 FROM {} GROUP BY {} HAVING count(*) > 1 LIMIT 1").format(table, grain))
    if cursor.fetchone():
        raise ValueError(f"Warehouse table {spec.name} contains duplicate daily grain")
    return count


def load_marketing_to_warehouse(paths=None) -> dict[str, int]:
    selected = paths or load_marketing_paths()
    spark = build_marketing_spark_session(app_name="pulse-marketing-warehouse", master=os.getenv("SPARK_MASTER", "local[*]"))
    frames = {}
    try:
        for spec in MARKETING_TABLE_SPECS:
            frame = read_parquet_data_files(spark, Path(getattr(selected, spec.name)))
            validate_required_columns(spec.name, frame.columns, spec)
            frames[spec.name] = frame.select(*spec.required_columns).cache()
            frames[spec.name].count()
        suffix = uuid.uuid4().hex[:12]
        counts = {}
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(WAREHOUSE_SCHEMA)))
                for spec in MARKETING_TABLE_SPECS:
                    staging = f"_staging_{spec.name}_{suffix}"
                    _create(cursor, staging, spec)
                    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                        sql.Identifier(WAREHOUSE_SCHEMA), sql.Identifier(staging),
                        sql.SQL(", ").join(map(sql.Identifier, spec.required_columns)))
                    with cursor.copy(copy_sql) as copy:
                        for row in frames[spec.name].toLocalIterator():
                            copy.write_row(tuple(row))
                    counts[spec.name] = _validate(cursor, staging, spec)
                for spec in MARKETING_TABLE_SPECS:
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
                    cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                        sql.Identifier(f"idx_{spec.name}_business_id"), target, sql.Identifier("business_id")))
                    cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                        sql.Identifier(f"idx_{spec.name}_report_date"), target, sql.Identifier("report_date")))
        return counts
    finally:
        for frame in frames.values():
            frame.unpersist(blocking=True)
        spark.stop()


def validate_marketing_warehouse() -> dict[str, int]:
    counts = {}
    with psycopg.connect(**connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            for spec in MARKETING_TABLE_SPECS:
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                               (WAREHOUSE_SCHEMA, spec.name))
                validate_required_columns(spec.name, [row[0] for row in cursor.fetchall()], spec)
                counts[spec.name] = _validate(cursor, spec.name, spec)
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("load", "validate"))
    args = parser.parse_args(argv)
    output = load_marketing_to_warehouse() if args.command == "load" else validate_marketing_warehouse()
    print(", ".join(f"{name}={count}" for name, count in output.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
