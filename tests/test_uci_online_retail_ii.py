"""Phase 6.4B UCI Online Retail II portability benchmark contracts."""

from __future__ import annotations

from datetime import timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import zipfile

from src.benchmarks.uci_online_retail_ii import (
    EXPECTED_COLUMNS, MANIFEST_PATH, SourceRow, UCI_BUSINESS_ID,
    UciManifest, UciOnlineRetailAdapter, XlsxReader, classify_line,
    classify_stock_code, deterministic_line_id, normalize_source_row,
    run_benchmark, scoped_invoice_id,
)
from src.onboarding.adapters import adapter_for
from src.onboarding.registry import BusinessRegistry, validate_business
from src.warehouse.load_uci_online_retail_ii import benchmark_rows


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_tiny_xlsx(path: Path) -> None:
    shared = (*EXPECTED_COLUMNS, "9001", "SKU-X", "SYNTHETIC ITEM", "United Kingdom")
    shared_xml = "".join(f"<si><t>{value}</t></si>" for value in shared)
    header = "".join(
        f'<c r="{chr(65 + index)}1" t="s"><v>{index}</v></c>'
        for index in range(len(EXPECTED_COLUMNS))
    )
    data = (
        '<c r="A2" t="s"><v>8</v></c>'
        '<c r="B2" t="s"><v>9</v></c>'
        '<c r="C2" t="s"><v>10</v></c>'
        '<c r="D2"><v>2</v></c>'
        '<c r="E2"><v>40148.5</v></c>'
        '<c r="F2"><v>4.25</v></c>'
        '<c r="G2"><v>12345</v></c>'
        '<c r="H2" t="s"><v>11</v></c>'
    )
    worksheet = (
        f'<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData><row r="1">{header}</row><row r="2">{data}</row></sheetData></worksheet>'
    )
    workbook = (
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Synthetic" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    relationships = (
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", relationships)
        archive.writestr("xl/sharedStrings.xml", (
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            f"{shared_xml}</sst>"))
        archive.writestr("xl/worksheets/sheet1.xml", worksheet)


class UciManifestAndParserTests(unittest.TestCase):
    def test_manifest_declares_local_workbook_and_two_sheets(self):
        manifest = UciManifest.load()
        self.assertEqual(tuple(manifest.worksheets), ("Year 2009-2010", "Year 2010-2011"))
        self.assertEqual(manifest.raw["currency"], "GBP")
        self.assertEqual(manifest.raw["expected_local_path"],
                         "data/public/uci_online_retail_ii/online_retail_II.xlsx")
        self.assertFalse(manifest.validate(manifest.root(fixture=True), fixture=True))

    def test_missing_workbook_has_clear_failure(self):
        with TemporaryDirectory() as directory:
            missing = Path(directory) / "not-downloaded.xlsx"
            errors = UciManifest.load().validate(missing, fixture=False)
        self.assertIn("workbook is missing", errors[0])

    def test_stdlib_xlsx_parser_reads_shared_strings_numbers_and_date_serial(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "tiny.xlsx"
            _write_tiny_xlsx(path)
            reader = XlsxReader(path)
            self.assertEqual(reader.sheet_names, ("Synthetic",))
            rows = list(reader.iter_raw_rows("Synthetic"))
        self.assertEqual(rows[0][1], EXPECTED_COLUMNS)
        source = SourceRow("Synthetic", rows[1][0], dict(zip(EXPECTED_COLUMNS, rows[1][1])))
        line, errors = normalize_source_row(
            source, currency="GBP", zone=__import__("zoneinfo").ZoneInfo("Europe/London"))
        self.assertFalse(errors)
        self.assertEqual(line["source_line_value"], 8.5)
        self.assertEqual(line["invoice_at"].tzinfo, timezone.utc)

    def test_business_and_adapter_are_registered_offline(self):
        registry = BusinessRegistry()
        report = validate_business(registry, UCI_BUSINESS_ID)
        self.assertTrue(report.valid)
        config = registry.sources_for(UCI_BUSINESS_ID)[0]
        adapter = adapter_for(config)
        self.assertIsInstance(adapter, UciOnlineRetailAdapter)
        self.assertTrue(adapter.healthcheck().healthy)


class UciIdentityAndClassificationTests(unittest.TestCase):
    def test_invoice_identity_is_worksheet_scoped(self):
        first = scoped_invoice_id("Year 2009-2010", "1001")
        second = scoped_invoice_id("Year 2010-2011", "1001")
        self.assertNotEqual(first, second)
        self.assertIn("Year_2009_2010", first)

    def test_line_identity_uses_source_row_not_stock_code(self):
        first = deterministic_line_id("Year 2009-2010", "1001", 2)
        second = deterministic_line_id("Year 2009-2010", "1001", 3)
        self.assertNotEqual(first, second)

    def test_special_stock_codes_are_provider_classifications(self):
        self.assertEqual(classify_stock_code("POST"), "POSTAGE_OR_CHARGE")
        self.assertEqual(classify_stock_code("BANK CHARGES"), "BANK_CHARGE")
        self.assertEqual(classify_stock_code("ADJUST"), "MANUAL_ADJUSTMENT")
        self.assertEqual(classify_stock_code("SKU-1"), "PRODUCT")

    def test_cancellation_and_negative_adjustment_are_not_conflated(self):
        self.assertEqual(classify_line(cancellation=True, quantity=-1, price=5,
                                       stock_category="PRODUCT"),
                         "CANCELLATION_OR_REVERSAL")
        self.assertEqual(classify_line(cancellation=False, quantity=-1, price=5,
                                       stock_category="PRODUCT"),
                         "NON_C_NEGATIVE_ADJUSTMENT")


class UciFixtureBenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report = run_benchmark(fixture=True, write_outputs=False)
        cls.quality = {row["code"]: row for row in cls.report["quality"]}

    def test_fixture_is_offline_business_isolated_and_gbp(self):
        self.assertFalse(self.report["network_access"])
        self.assertEqual(self.report["business_id"], UCI_BUSINESS_ID)
        self.assertEqual(self.report["currency"], "GBP")
        self.assertEqual(self.report["metrics"]["source_rows"], 12)

    def test_cross_sheet_invoice_collision_and_fanout_are_protected(self):
        metrics = self.report["metrics"]
        self.assertEqual(metrics["distinct_raw_invoice_ids"], 10)
        self.assertEqual(metrics["worksheet_scoped_invoices"], 11)
        self.assertEqual(metrics["raw_invoice_ids_reused_across_worksheets"], 1)
        self.assertTrue(all(self.report["fanout"].values()))

    def test_normal_multi_line_and_optional_customer_projection(self):
        projection = self.report["canonical_projection"]
        self.assertEqual(projection["eligible_order_count"], 4)
        self.assertEqual(projection["eligible_line_count"], 5)
        self.assertEqual(projection["commerce_order_models"], 4)
        self.assertEqual(self.report["metrics"]["invoices_missing_customer"], 1)
        self.assertEqual(self.quality["missing_customer_id"]["classification"],
                         "SOURCE_COMPLETENESS")

    def test_c_invoice_is_preserved_without_fabricated_original_link(self):
        self.assertEqual(self.report["metrics"]["worksheet_scoped_cancellation_invoices"], 2)
        self.assertEqual(self.report["metrics"]["stripped_c_invoice_direct_matches"], 0)
        self.assertEqual(self.report["line_classifications"]["CANCELLATION_OR_REVERSAL"], 2)
        self.assertNotIn("original_invoice_id", json.dumps(self.report))

    def test_negative_zero_and_unusual_rows_are_preserved(self):
        metrics = self.report["metrics"]
        self.assertEqual(metrics["negative_quantity_rows"], 4)
        self.assertEqual(metrics["non_c_negative_quantity_rows"], 3)
        self.assertEqual(metrics["positive_quantity_rows_on_c_invoice"], 1)
        self.assertEqual(metrics["zero_price_rows"], 3)
        self.assertEqual(metrics["negative_price_rows"], 1)
        self.assertEqual(self.quality["positive_quantity_on_c_invoice"]["severity"], "WARNING")

    def test_post_is_charge_not_shipping_cost_and_price_is_not_cogs(self):
        self.assertEqual(self.report["stock_code_classifications"]["POSTAGE_OR_CHARGE"], 1)
        economics = self.report["economics"]
        self.assertFalse(economics["merchant_shipping_cost_available"])
        self.assertFalse(economics["cogs_available"])
        self.assertFalse(economics["profit_calculated"])

    def test_no_fulfillment_cod_remittance_or_attribution_fabrication(self):
        projection = self.report["canonical_projection"]
        self.assertEqual(projection["operational_events"], 0)
        self.assertEqual(projection["cod_rows"], 0)
        self.assertEqual(projection["remittance_rows"], 0)
        self.assertEqual(projection["attribution_rows"], 0)

    def test_signed_value_and_anomaly_engine_reconcile(self):
        self.assertTrue(self.report["reconciliation"]["signed_value_reconciled"])
        self.assertEqual(self.report["metrics"]["net_source_ledger_value"], 42.5)
        self.assertEqual(len(self.report["anomaly"]), 3)
        self.assertTrue(all(row["thresholds_unchanged"] for row in self.report["anomaly"]))

    def test_written_outputs_feed_warehouse_contracts(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            run_benchmark(fixture=True, output_root=root, write_outputs=True)
            rows = benchmark_rows(root)
        self.assertEqual(set(rows), {
            "uci_retail_daily", "uci_invoice_summary", "uci_line_classification",
            "uci_country_distribution", "uci_data_quality", "uci_economic_completeness",
        })
        self.assertEqual(len(rows["uci_invoice_summary"]), 11)
        self.assertTrue(all(row["currency"] == "GBP" for row in rows["uci_retail_daily"]))

    def test_git_ignore_keeps_public_and_generated_data_out_of_git(self):
        ignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("data/*", ignore)
        self.assertIn("!data/fixtures/uci_online_retail_ii/", ignore)
        self.assertNotIn("online_retail_II.xlsx", "\n".join(
            path.as_posix() for path in PROJECT_ROOT.glob("data/fixtures/uci_online_retail_ii/*")))

    def test_manual_airflow_dag_has_expected_tasks_and_no_schedule(self):
        source = (PROJECT_ROOT / "airflow" / "dags" /
                  "pulse_uci_online_retail_ii_benchmark.py").read_text(encoding="utf-8")
        self.assertIn('dag_id="pulse_uci_online_retail_ii_benchmark"', source)
        self.assertIn("schedule=None", source)
        for task_id in ("validate_local_files", "run_benchmark", "load_benchmark",
                        "run_dbt", "test_dbt"):
            self.assertIn(f'task_id="{task_id}"', source)


@unittest.skipUnless(os.getenv("RUN_UCI_FULL_BENCHMARK") == "1",
                     "set RUN_UCI_FULL_BENCHMARK=1 for the local public workbook")
class UciFullBenchmarkAcceptanceTests(unittest.TestCase):
    def test_full_local_acceptance_reconciles_manifest(self):
        report = run_benchmark(write_outputs=False, enforce_acceptance=True)
        self.assertTrue(all(item["passed"] for item in report["acceptance"]))
        self.assertTrue(report["reconciliation"]["signed_value_reconciled"])
        self.assertEqual(report["metrics"]["invoices_with_multiple_known_customers"], 0)
        self.assertEqual(report["metrics"]["invoices_with_multiple_countries"], 0)


@unittest.skipUnless(os.getenv("RUN_WAREHOUSE_INTEGRATION_TESTS") == "1",
                     "set RUN_WAREHOUSE_INTEGRATION_TESTS=1 for PostgreSQL idempotence")
class UciWarehouseIntegrationTests(unittest.TestCase):
    def test_fixture_reload_is_idempotent(self):
        from src.warehouse.load_uci_online_retail_ii import load_uci_benchmark

        with TemporaryDirectory() as directory:
            root = Path(directory)
            run_benchmark(fixture=True, output_root=root, write_outputs=True)
            first = load_uci_benchmark(root)
            second = load_uci_benchmark(root)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
