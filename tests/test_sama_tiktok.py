"""Phase 6.5B synthetic and opt-in real-source acceptance tests."""

from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from zipfile import ZipFile, ZIP_DEFLATED

from src.pilots.sama import build_pilot
from src.pilots.sama_tiktok import (
    FINAL_TIKTOK_FILE,
    ID_COLUMNS,
    REQUIRED_COLUMNS,
    _xlsx_rows_exact,
    build_tiktok_pilot,
    load_private_tiktok,
)
from src.warehouse.load_sama_tiktok import GRAINS, TABLES


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMA_FIXTURE = PROJECT_ROOT / "data/fixtures/sama_pilot/sama_pilot_synthetic.json"
TIKTOK_FIXTURE = PROJECT_ROOT / "data/fixtures/sama_pilot/sama_tiktok_synthetic.json"


def _phase_inputs():
    fixture = json.loads(SAMA_FIXTURE.read_text(encoding="utf-8"))
    light = deepcopy(fixture["lightfunnels"])
    for row in light:
        row["UTM Attributes"] = (
            "source=tiktokid=1000000000000001"
            "campaign=Target Campaign Amedium=cpc"
        )
    result = build_pilot(light, fixture["leads"], fixture["orders"],
                         hmac_key="synthetic-tiktok-test-key")
    return light, result


def _source_rows():
    fixture = json.loads(TIKTOK_FIXTURE.read_text(encoding="utf-8"))
    return [(row, {column: "text" for column in row}) for row in fixture["rows"]]


def _write_minimal_xlsx(path: Path, row: dict[str, str]) -> None:
    headers = list(row)

    def column_name(index: int) -> str:
        output = ""
        current = index + 1
        while current:
            current, remainder = divmod(current - 1, 26)
            output = chr(65 + remainder) + output
        return output

    def xml_row(number: int, values) -> str:
        cells = []
        for index, value in enumerate(values):
            escaped = (str(value).replace("&", "&amp;").replace("<", "&lt;")
                       .replace(">", "&gt;"))
            cells.append(
                f'<c r="{column_name(index)}{number}" t="inlineStr"><is><t>{escaped}</t></is></c>'
            )
        return f'<row r="{number}">' + "".join(cells) + "</row>"

    sheet = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetData>' + xml_row(1, headers) + xml_row(2, [row[name] for name in headers])
        + '</sheetData></worksheet>'
    )
    workbook = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    relationships = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", relationships)
        archive.writestr("xl/worksheets/sheet1.xml", sheet)


class SamaTikTokTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.light_rows, cls.phase_result = _phase_inputs()
        cls.result = build_tiktok_pilot(_source_rows(), cls.light_rows, cls.phase_result)
        cls.checks = {row["check_name"]: row for row in cls.result.data_quality}

    def test_exact_xlsx_ids_remain_strings_without_float_conversion(self):
        source = _source_rows()[0][0]
        with TemporaryDirectory() as root:
            path = Path(root) / "synthetic.xlsx"
            _write_minimal_xlsx(path, source)
            row, storage = next(_xlsx_rows_exact(path))
        for name in ID_COLUMNS:
            self.assertEqual(row[name], source[name])
            self.assertEqual(storage[name], "text")
        self.assertEqual(row["Account ID"], "4000000000000000001")

    def test_ad_day_grain_and_export_summary_reconcile(self):
        profile = self.result.source_profile
        self.assertEqual(profile["source_rows"], 3)
        self.assertEqual(profile["summary_rows"], 1)
        self.assertEqual(profile["canonical_marketing_rows"], 3)
        self.assertEqual(profile["campaigns"], 2)
        self.assertEqual(self.checks["duplicate_ad_day_keys"]["issue_count"], 0)
        self.assertEqual(self.checks["source_aggregate_reconciliation"]["issue_count"], 0)

    def test_duplicate_grain_is_flagged_and_not_double_counted(self):
        duplicated = [*_source_rows()[:-1], _source_rows()[0], _source_rows()[-1]]
        result = build_tiktok_pilot(duplicated, self.light_rows, self.phase_result)
        checks = {row["check_name"]: row for row in result.data_quality}
        self.assertEqual(len(result.marketing_records), 3)
        self.assertEqual(checks["duplicate_ad_day_keys"]["issue_count"], 1)

    def test_zero_spend_attributed_activity_is_preserved(self):
        row = next(item for item in self.result.marketing_records
                   if item["report_date"] == "2026-03-02")
        self.assertEqual(row["spend"], 0)
        self.assertEqual(row["impressions"], 0)
        self.assertEqual(row["platform_conversions"], 1)
        self.assertEqual(row["details"]["checkouts_initiated"], 1)
        self.assertEqual(self.checks["zero_spend_rows_with_attributed_activity"]["issue_count"], 1)

    def test_all_zero_row_is_observed_not_discarded(self):
        outside = next(item for item in self.result.marketing_records
                       if item["campaign_id"] == "1000000000000002")
        self.assertEqual(outside["spend"], 0)
        self.assertEqual(outside["platform_conversions"], 0)
        self.assertEqual(self.checks["fully_inactive_ad_day_rows"]["issue_count"], 1)

    def test_provider_ratios_are_native_diagnostics_not_conversion_value(self):
        row = self.result.marketing_records[0]
        self.assertEqual(row["platform_conversion_value"], 0)
        self.assertFalse(row["details"]["platform_conversion_value_available"])
        self.assertEqual(row["details"]["platform_reported_purchase_roas"], 2.5)
        self.assertAlmostEqual(row["details"]["hook_rate"], 0.70)
        self.assertAlmostEqual(row["details"]["hold_rate"], 0.50)

    def test_downstream_outcomes_exist_only_at_campaign_day_grain(self):
        self.assertTrue(self.result.order_outcomes_daily)
        for row in self.result.order_outcomes_daily:
            self.assertNotIn("ad_group_id", row)
            self.assertNotIn("ad_id", row)
        self.assertEqual(
            GRAINS["sama_pilot_tiktok_order_outcomes_daily"],
            ("business_id", "campaign_id", "report_date"),
        )

    def test_outside_cohort_and_platform_business_gap_are_explicit(self):
        self.assertEqual(self.checks["tiktok_campaigns_outside_target_cohort"]["issue_count"], 1)
        self.assertEqual(self.checks["ambiguous_identity_matches"]["category"], "OBSERVATION")
        self.assertEqual(self.checks["unmatched_identity_records"]["category"], "OBSERVATION")
        self.assertEqual(
            self.checks["lightfunnels_tiktok_orders_missing_campaign_id"]["issue_count"], 0
        )
        gap = self.checks["platform_conversions_minus_lightfunnels_orders"]
        self.assertEqual(gap["observed_value"], 9)
        self.assertEqual(gap["expected_value"], 10)

    def test_campaign_mart_protects_currency_and_attribution_grain(self):
        sql = (PROJECT_ROOT / "dbt/models/marts/sama_pilot_tiktok_campaign_outcomes.sql").read_text(
            encoding="utf-8"
        ).lower()
        for forbidden in ("revenue", "contribution", "business_roas", "ad_group_id", "ad_id"):
            self.assertNotIn(forbidden, sql)
        self.assertIn("cost_per_delivered_order_usd", sql)
        self.assertIn("campaign_id", sql)

    def test_warehouse_contract_is_aggregate_and_pii_free(self):
        self.assertEqual(set(TABLES), {
            "sama_pilot_tiktok_ad_daily",
            "sama_pilot_tiktok_order_outcomes_daily",
            "sama_pilot_tiktok_data_quality",
        })
        columns = {name for specs in TABLES.values() for name, _, _ in specs}
        self.assertFalse({"phone", "email", "address", "tracking_number"} & columns)

    def test_ci_does_not_require_private_source(self):
        self.assertNotIn(str(FINAL_TIKTOK_FILE), TIKTOK_FIXTURE.read_text(encoding="utf-8"))
        self.assertTrue(REQUIRED_COLUMNS <= set(_source_rows()[0][0]))


@unittest.skipUnless(os.environ.get("RUN_SAMA_TIKTOK_FULL_ACCEPTANCE") == "1",
                     "Set RUN_SAMA_TIKTOK_FULL_ACCEPTANCE=1 to validate the ignored final XLSX")
class SamaTikTokRealDataAcceptanceTests(unittest.TestCase):
    def test_final_private_xlsx_reconciles_without_printing_rows(self):
        result = load_private_tiktok(hmac_key="phase65b-acceptance-only")
        profile = result.source_profile
        self.assertEqual(profile["source_rows"], 3840)
        self.assertEqual(profile["canonical_marketing_rows"], 3840)
        self.assertEqual(profile["campaigns"], 29)
        self.assertEqual(profile["ad_groups"], 176)
        self.assertEqual(profile["ads"], 449)
        self.assertAlmostEqual(profile["spend"], 7211.80)
        self.assertEqual(profile["target_campaigns"], 12)
        self.assertEqual(profile["outside_target_campaigns"], 17)
        self.assertEqual(profile["tiktok_lightfunnels_orders"], 1069)
        self.assertEqual(profile["campaign_day_boundary_observations"], 1)


if __name__ == "__main__":
    unittest.main()
