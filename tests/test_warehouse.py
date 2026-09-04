"""Fast unit tests for the PostgreSQL Gold serving contract."""

from __future__ import annotations

import unittest
import os
from unittest.mock import patch

import psycopg

from src.warehouse.load_gold import (
    TABLE_SPECS,
    WAREHOUSE_SCHEMA,
    connection_kwargs,
    validate_required_columns,
    load_gold_to_warehouse,
    validate_warehouse,
)


class WarehouseContractTests(unittest.TestCase):
    def test_schema_and_table_names_are_explicit(self) -> None:
        self.assertEqual(WAREHOUSE_SCHEMA, "analytics")
        self.assertEqual(
            tuple(spec.name for spec in TABLE_SPECS),
            ("daily_sales", "customer_metrics", "product_metrics", "funnel_metrics"),
        )

    def test_every_table_has_explicit_supported_postgres_types(self) -> None:
        supported = {"DATE", "TEXT", "BIGINT", "DOUBLE PRECISION", "TIMESTAMP WITH TIME ZONE"}
        self.assertTrue(TABLE_SPECS)
        for spec in TABLE_SPECS:
            self.assertTrue(spec.columns)
            self.assertEqual(len(spec.required_columns), len(set(spec.required_columns)))
            self.assertIn(spec.index_column, spec.required_columns)
            self.assertTrue(all(column.postgres_type in supported for column in spec.columns))

    def test_required_columns_accept_exact_mapping(self) -> None:
        for spec in TABLE_SPECS:
            validate_required_columns(spec.name, spec.required_columns, spec)

    def test_required_columns_reject_missing_and_unexpected_columns(self) -> None:
        spec = TABLE_SPECS[0]
        with self.assertRaisesRegex(ValueError, "missing"):
            validate_required_columns(spec.name, spec.required_columns[1:], spec)
        with self.assertRaisesRegex(ValueError, "unexpected"):
            validate_required_columns(spec.name, (*spec.required_columns, "surprise"), spec)

    def test_host_connection_defaults_and_overrides(self) -> None:
        defaults = connection_kwargs({})
        self.assertEqual(defaults["host"], "localhost")
        self.assertEqual(defaults["port"], 5433)
        configured = connection_kwargs({"WAREHOUSE_HOST": "warehouse-postgres", "WAREHOUSE_PORT": "5432"})
        self.assertEqual(configured["host"], "warehouse-postgres")
        self.assertEqual(configured["port"], 5432)


@unittest.skipUnless(
    os.environ.get("RUN_WAREHOUSE_INTEGRATION_TESTS") == "1",
    "Warehouse integration tests disabled",
)
class WarehouseIntegrationTests(unittest.TestCase):
    def test_full_refresh_is_rerunnable_without_duplicates(self) -> None:
        before = validate_warehouse()
        self.assertEqual(load_gold_to_warehouse(), before)
        self.assertEqual(validate_warehouse(), before)

    def test_failed_refresh_rolls_back_every_table(self) -> None:
        before = validate_warehouse()
        from src.warehouse import load_gold as loader

        real_validate = loader._validate_table_data
        calls = 0

        def fail_during_staging(cursor, table_name, spec):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("deliberate integration failure")
            return real_validate(cursor, table_name, spec)

        with (
            patch.object(loader, "_validate_table_data", side_effect=fail_during_staging),
            self.assertRaisesRegex(RuntimeError, "deliberate integration failure"),
        ):
            load_gold_to_warehouse()

        self.assertEqual(validate_warehouse(), before)
        with psycopg.connect(**connection_kwargs()) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename LIKE '_staging_%%'",
                    (WAREHOUSE_SCHEMA,),
                )
                self.assertEqual(cursor.fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
