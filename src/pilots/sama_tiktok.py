"""Privacy-safe Phase 6.5B TikTok daily marketing pilot.

The adapter reads only the final daily ad-level XLSX, preserves identifiers as
text, validates every canonical row through ``MarketingRecord``, and aggregates
the existing Phase 6.5A order facts to campaign/day before persistence.  Raw
Lightfunnels rows and customer data never leave this process.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable, Iterator
from uuid import NAMESPACE_URL, uuid5
import xml.etree.ElementTree as ET
import zipfile
from zoneinfo import ZoneInfo

from src.marketing.models import MarketingRecord
from src.pilots.sama import (
    BUSINESS_ID,
    COHORT_END,
    COHORT_START,
    COD_LEADS_FILE,
    COD_ORDERS_FILE,
    LIGHTFUNNELS_FILE,
    PRIVATE_ROOT,
    SOURCE_TIMEZONE,
    _csv_rows,
    _identifier,
    _utm_attributes,
    _xlsx_rows,
    build_pilot,
)


FINAL_TIKTOK_FILE = "Tiktok Ads_Daily ad level.xlsx"
SOURCE_TYPE = "tiktok_ads"
SOURCE_ID = "sama_tiktok_xlsx"
PLATFORM = "tiktok_ads"
SCHEMA_VERSION = "tiktok_ads_daily_v1"
EXTRACTED_AT = datetime(2026, 8, 1, tzinfo=timezone.utc)

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

REQUIRED_COLUMNS = frozenset({
    "Campaign name", "Campaign ID", "Ad group name", "Ad group ID",
    "Ad name", "Ad ID", "Account name", "Account ID", "By Day",
    "Spend", "Impressions", "Reach", "Clicks (destination)", "Conversions",
    "CTR (destination)", "Frequency", "Cost per conversion",
    "CPC (destination)", "2-second video views", "6-second video views",
    "Video views at 25%", "Video views at 50%", "Video views at 75%",
    "Video views at 100%", "Average play time per user",
    "Average play time per video view", "Purchase ROAS (website)",
    "Checkouts initiated (website)", "Cost per checkout initiated (website)",
    "Unique checkout initiation rate (website)",
    "Value per checkout initiated (website)",
    "Checkout initiation value (website)", "Hold Rate", "Hook Rate", "Currency",
})

ID_COLUMNS = ("Campaign ID", "Ad group ID", "Ad ID", "Account ID")
ADDITIVE_COLUMNS = (
    "Spend", "Impressions", "Clicks (destination)", "Conversions",
    "Checkouts initiated (website)", "2-second video views", "6-second video views",
    "Video views at 25%", "Video views at 50%", "Video views at 75%",
    "Video views at 100%", "Checkout initiation value (website)",
)
RECONCILIATION_COLUMNS = (
    "Spend", "Impressions", "Clicks (destination)", "Conversions",
    "Checkouts initiated (website)",
)


@dataclass(frozen=True, slots=True)
class TikTokPilotResult:
    marketing_records: tuple[dict[str, Any], ...]
    order_outcomes_daily: tuple[dict[str, Any], ...]
    data_quality: tuple[dict[str, Any], ...]
    source_profile: dict[str, Any]


def _cell_column(reference: str) -> int:
    letters = "".join(character for character in reference if character.isalpha())
    result = 0
    for character in letters.upper():
        result = result * 26 + ord(character) - 64
    return result - 1


def _xlsx_rows_exact(path: Path) -> Iterator[tuple[dict[str, str], dict[str, str]]]:
    """Yield XLSX rows as source text plus cell storage types.

    Numeric identifiers are never converted through floating point.  Storage
    metadata remains available for DQ even when the exact numeric lexeme can be
    represented safely as a string.
    """
    ns = {"m": _MAIN_NS, "r": _REL_NS, "p": _PACKAGE_REL_NS}
    with zipfile.ZipFile(path) as archive:
        shared: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            shared = ["".join(node.text or "" for node in item.findall(".//m:t", ns))
                      for item in root.findall("m:si", ns)]
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        targets = {item.attrib["Id"]: item.attrib["Target"] for item in relationships}
        sheet = workbook.find("m:sheets/m:sheet", ns)
        if sheet is None:
            return
        target = targets[sheet.attrib[f"{{{_REL_NS}}}id"]]
        if target.startswith("/"):
            target = target.lstrip("/")
        elif not target.startswith("xl/"):
            target = "xl/" + target

        headers: list[str] | None = None
        with archive.open(target) as stream:
            for _, element in ET.iterparse(stream, events=("end",)):
                if element.tag != f"{{{_MAIN_NS}}}row":
                    continue
                cells: dict[int, tuple[str, str]] = {}
                for cell in element.findall("m:c", ns):
                    index = _cell_column(cell.attrib.get("r", "A1"))
                    cell_type = cell.attrib.get("t", "n")
                    value_node = cell.find("m:v", ns)
                    if cell_type == "inlineStr":
                        value = "".join(node.text or "" for node in cell.findall(".//m:t", ns))
                        storage = "text"
                    elif value_node is None:
                        value, storage = "", "blank"
                    elif cell_type == "s":
                        value, storage = shared[int(value_node.text or 0)], "text"
                    elif cell_type in {"str", "e"}:
                        value, storage = value_node.text or "", "text"
                    elif cell_type == "b":
                        value, storage = value_node.text or "", "boolean"
                    else:
                        value, storage = value_node.text or "", "numeric"
                    cells[index] = (value.strip(), storage)
                if cells:
                    width = max(cells) + 1
                    values = [cells.get(index, ("", "blank"))[0] for index in range(width)]
                    if headers is None:
                        headers = values
                    else:
                        row = {header: values[index] if index < len(values) else ""
                               for index, header in enumerate(headers) if header}
                        types = {header: cells.get(index, ("", "blank"))[1]
                                 for index, header in enumerate(headers) if header}
                        yield row, types
                element.clear()


def _decimal(value: Any, *, percentage: bool = False) -> Decimal:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--"}:
        return Decimal(0)
    has_percent = text.endswith("%")
    if has_percent:
        text = text[:-1]
    try:
        result = Decimal(text)
    except InvalidOperation:
        return Decimal("NaN")
    if percentage and has_percent:
        result /= Decimal(100)
    return result


def _number(value: Any, *, percentage: bool = False) -> float:
    parsed = _decimal(value, percentage=percentage)
    return float(parsed) if parsed.is_finite() else float("nan")


def _integer(value: Any) -> int:
    parsed = _decimal(value)
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        raise ValueError(f"Expected an integer metric, received {value!r}")
    return int(parsed)


def _date(value: Any) -> date | None:
    text = str(value or "").strip()
    for pattern in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    try:
        serial = Decimal(text)
    except InvalidOperation:
        return None
    if not serial.is_finite():
        return None
    return (datetime(1899, 12, 30) + timedelta(days=float(serial))).date()


def _clean_id(value: Any) -> str:
    return str(value or "").strip()


def _is_invalid_id(value: str) -> bool:
    return not value or value in {"-", "--"} or bool(
        re.search(r"[eE][+-]?\d+$", value) or re.fullmatch(r"\d+\.0+", value)
    )


def _dq_row(name: str, category: str, count: int, *, observed: float | None = None,
            expected: float | None = None) -> dict[str, Any]:
    status = "FAIL" if category == "DATA_DEFECT" and count else (
        "OBSERVED" if category == "OBSERVATION" and count else "PASS"
    )
    return {
        "business_id": BUSINESS_ID,
        "check_name": name,
        "category": category,
        "issue_count": int(count),
        "observed_value": observed,
        "expected_value": expected,
        "status": status,
    }


def _canonical_record(row: dict[str, str], report_date: date) -> dict[str, Any]:
    spend = _number(row["Spend"])
    impressions = _integer(row["Impressions"])
    clicks = _integer(row["Clicks (destination)"])
    conversions = _number(row["Conversions"])
    reach = _integer(row["Reach"])
    frequency = _number(row["Frequency"])
    record_key = "|".join((
        BUSINESS_ID, _clean_id(row["Account ID"]), _clean_id(row["Campaign ID"]),
        _clean_id(row["Ad group ID"]), _clean_id(row["Ad ID"]), report_date.isoformat(),
    ))
    details = {
        "account_name": row["Account name"].strip() or None,
        "native_ad_group_term": "adgroup",
        "destination_ctr": _number(row["CTR (destination)"], percentage=True),
        "destination_cpc": _number(row["CPC (destination)"]),
        "cost_per_conversion": _number(row["Cost per conversion"]),
        "video_views_2s": _integer(row["2-second video views"]),
        "video_views_6s": _integer(row["6-second video views"]),
        "video_views_25pct": _integer(row["Video views at 25%"]),
        "video_views_50pct": _integer(row["Video views at 50%"]),
        "video_views_75pct": _integer(row["Video views at 75%"]),
        "video_views_100pct": _integer(row["Video views at 100%"]),
        "average_play_time_per_user": _number(row["Average play time per user"]),
        "average_play_time_per_video_view": _number(row["Average play time per video view"]),
        "platform_reported_purchase_roas": _number(row["Purchase ROAS (website)"]),
        "checkouts_initiated": _number(row["Checkouts initiated (website)"]),
        "cost_per_checkout": _number(row["Cost per checkout initiated (website)"]),
        "unique_checkout_initiation_rate": _number(
            row["Unique checkout initiation rate (website)"], percentage=True
        ),
        "value_per_checkout": _number(row["Value per checkout initiated (website)"]),
        "checkout_initiation_value": _number(row["Checkout initiation value (website)"]),
        "hold_rate": _number(row["Hold Rate"], percentage=True),
        "hook_rate": _number(row["Hook Rate"], percentage=True),
        "platform_conversion_value_available": False,
    }
    for key, value in tuple(details.items()):
        if isinstance(value, float) and not math.isfinite(value):
            details[key] = None
    record = MarketingRecord(
        business_id=BUSINESS_ID,
        source_type=SOURCE_TYPE,
        source_id=SOURCE_ID,
        ingestion_id=str(uuid5(NAMESPACE_URL, f"sama-tiktok-ingestion|{BUSINESS_ID}")),
        record_id=str(uuid5(NAMESPACE_URL, f"sama-tiktok-record|{record_key}")),
        extracted_at_utc=EXTRACTED_AT,
        source_updated_at_utc=None,
        schema_version=SCHEMA_VERSION,
        platform=PLATFORM,
        account_id=_clean_id(row["Account ID"]),
        campaign_id=_clean_id(row["Campaign ID"]),
        campaign_name=row["Campaign name"].strip() or None,
        ad_group_id=_clean_id(row["Ad group ID"]),
        ad_group_name=row["Ad group name"].strip() or None,
        ad_id=_clean_id(row["Ad ID"]),
        ad_name=row["Ad name"].strip() or None,
        creative_id=None,
        report_date=report_date,
        reporting_timezone=SOURCE_TIMEZONE,
        currency=row["Currency"].strip().upper(),
        spend=spend,
        impressions=impressions,
        clicks=clicks,
        platform_conversions=conversions,
        platform_conversion_value=0.0,
        reach=reach,
        frequency=frequency,
        link_clicks=clicks,
        video_views=details["video_views_2s"],
        landing_page_views=None,
        details=details,
    )
    return record.to_dict()


def _order_campaign_outcomes(
    light_rows: Iterable[dict[str, Any]],
    phase_result: Any,
) -> tuple[tuple[dict[str, Any], ...], dict[str, int]]:
    by_order: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in light_rows:
        order_id = _identifier(row.get("Order Id"))
        attributes = _utm_attributes(row.get("UTM Attributes"))
        if order_id and (attributes.get("utm_source") or "").strip().lower() == "tiktok":
            by_order[order_id].add(((attributes.get("utm_id") or "").strip(),
                                    (attributes.get("utm_campaign") or "").strip()))
    conflicts = sum(len(values) > 1 for values in by_order.values())
    missing_campaign_ids = sum(
        len(values) == 1 and not next(iter(values))[0]
        for values in by_order.values()
    )
    mapping = {order_id: next(iter(values)) for order_id, values in by_order.items()
               if len(values) == 1 and next(iter(values))[0]}
    grouped: dict[tuple[str, str, date], Counter[str]] = defaultdict(Counter)
    for fact in phase_result.order_facts:
        campaign = mapping.get(fact["lightfunnel_order_id"])
        if campaign is None or fact["intent_created_at"] is None:
            continue
        report_date = fact["intent_created_at"].astimezone(ZoneInfo(SOURCE_TIMEZONE)).date()
        counter = grouped[(campaign[0], campaign[1], report_date)]
        counter["initial_orders"] += 1
        counter["matched_orders"] += fact["match_status"] == "MATCHED_HIGH_CONFIDENCE"
        counter["ambiguous_orders"] += fact["match_status"] == "AMBIGUOUS"
        counter["unmatched_orders"] += fact["match_status"] == "UNMATCHED"
        counter["confirmed_orders"] += bool(fact["confirmed"])
        counter["shipped_orders"] += bool(fact["shipped"])
        counter["delivered_orders"] += bool(fact["delivered"])
        counter["returned_orders"] += bool(fact["returned"])
        counter["out_of_stock_orders"] += bool(fact["out_of_stock"])
    rows = tuple({
        "business_id": BUSINESS_ID,
        "campaign_id": campaign_id,
        "campaign_name": campaign_name or None,
        "report_date": report_date.isoformat(),
        **{name: int(counts[name]) for name in (
            "initial_orders", "matched_orders", "ambiguous_orders", "unmatched_orders",
            "confirmed_orders", "shipped_orders", "delivered_orders", "returned_orders",
            "out_of_stock_orders",
        )},
    } for (campaign_id, campaign_name, report_date), counts in sorted(grouped.items()))
    return rows, {
        "tiktok_lightfunnels_orders": len(mapping),
        "lightfunnels_campaign_conflicts": conflicts,
        "lightfunnels_missing_campaign_ids": missing_campaign_ids,
    }


def build_tiktok_pilot(
    tiktok_rows: Iterable[tuple[dict[str, str], dict[str, str]]],
    light_rows: Iterable[dict[str, Any]],
    phase_result: Any,
) -> TikTokPilotResult:
    materialized = list(tiktok_rows)
    if not materialized:
        raise ValueError("TikTok XLSX contains no rows")
    missing_columns = REQUIRED_COLUMNS - set(materialized[0][0])
    if missing_columns:
        raise ValueError("TikTok XLSX is missing required columns: " + ", ".join(sorted(missing_columns)))

    data_rows: list[tuple[dict[str, str], dict[str, str], date | None]] = []
    summary_rows: list[dict[str, str]] = []
    for row, storage in materialized:
        report_date = _date(row.get("By Day"))
        if report_date is None and all(_clean_id(row.get(name)) in {"", "-", "--"} for name in ID_COLUMNS):
            summary_rows.append(row)
        else:
            data_rows.append((row, storage, report_date))

    missing_ids = sum(any(_is_invalid_id(_clean_id(row.get(name))) for name in ID_COLUMNS[:3])
                      for row, _, _ in data_rows)
    scientific_ids = sum(any(re.search(r"[eE][+-]?\d+$", _clean_id(row.get(name)))
                             for name in ID_COLUMNS) for row, _, _ in data_rows)
    numeric_ids = sum(any(storage.get(name) == "numeric" for name in ID_COLUMNS)
                      for _, storage, _ in data_rows)
    outside_dates = sum(report_date is None or report_date < COHORT_START or report_date > COHORT_END
                        for _, _, report_date in data_rows)
    unexpected_currency = sum(row.get("Currency", "").strip().upper() != "USD"
                              for row, _, _ in data_rows)
    negative_metrics = sum(any((_decimal(row.get(name)).is_finite()
                                and _decimal(row.get(name)) < 0) for name in ADDITIVE_COLUMNS)
                           for row, _, _ in data_rows)
    invalid_numbers = sum(any(not _decimal(row.get(name)).is_finite() for name in ADDITIVE_COLUMNS)
                          for row, _, _ in data_rows)
    click_over_impression = sum(
        _decimal(row.get("Clicks (destination)")) > _decimal(row.get("Impressions"))
        for row, _, _ in data_rows
    )

    grain_counts = Counter((
        _clean_id(row.get("Campaign ID")), _clean_id(row.get("Ad group ID")),
        _clean_id(row.get("Ad ID")), report_date,
    ) for row, _, report_date in data_rows)
    duplicate_keys = sum(count - 1 for count in grain_counts.values())
    hierarchy: dict[tuple[str, str], set[tuple[str, ...]]] = defaultdict(set)
    for row, _, _ in data_rows:
        hierarchy[("campaign", _clean_id(row.get("Campaign ID")))].add((row.get("Campaign name", ""),))
        hierarchy[("ad_group", _clean_id(row.get("Ad group ID")))].add(
            (_clean_id(row.get("Campaign ID")), row.get("Ad group name", ""))
        )
        hierarchy[("ad", _clean_id(row.get("Ad ID")))].add(
            (_clean_id(row.get("Campaign ID")), _clean_id(row.get("Ad group ID")), row.get("Ad name", ""))
        )
    hierarchy_conflicts = sum(len(values) > 1 for values in hierarchy.values())

    inactive_rows = sum(all(_decimal(row.get(name)) == 0 for name in ADDITIVE_COLUMNS)
                        for row, _, _ in data_rows)
    zero_spend_attributed = sum(
        _decimal(row.get("Spend")) == 0 and _decimal(row.get("Impressions")) == 0
        and (_decimal(row.get("Conversions")) > 0
             or _decimal(row.get("Checkouts initiated (website)")) > 0)
        for row, _, _ in data_rows
    )

    records = []
    seen: set[tuple[str, str, str, date]] = set()
    for row, _, report_date in data_rows:
        key = (_clean_id(row.get("Campaign ID")), _clean_id(row.get("Ad group ID")),
               _clean_id(row.get("Ad ID")), report_date)
        invalid = (
            key in seen or report_date is None or not (COHORT_START <= report_date <= COHORT_END)
            or any(_is_invalid_id(item) for item in key[:3])
            or row.get("Currency", "").strip().upper() != "USD"
            or any(not _decimal(row.get(name)).is_finite() or _decimal(row.get(name)) < 0
                   for name in ADDITIVE_COLUMNS)
            or _decimal(row.get("Clicks (destination)")) > _decimal(row.get("Impressions"))
        )
        if invalid:
            continue
        seen.add(key)
        records.append(_canonical_record(row, report_date))

    outcome_rows, outcome_profile = _order_campaign_outcomes(light_rows, phase_result)
    target_campaigns = {row["campaign_id"] for row in outcome_rows}
    source_campaigns = {row["campaign_id"] for row in records}
    target_missing = target_campaigns - source_campaigns
    outside_campaigns = source_campaigns - target_campaigns
    source_campaign_names: dict[str, set[str]] = defaultdict(set)
    for row in records:
        if row["campaign_name"]:
            source_campaign_names[row["campaign_id"]].add(row["campaign_name"])
    target_name_mismatches = sum(
        bool(row["campaign_name"])
        and row["campaign_name"] not in source_campaign_names.get(row["campaign_id"], set())
        for row in outcome_rows
    )
    source_pairs = {(row["campaign_id"], row["report_date"]) for row in records}
    boundary_orders = sum(
        row["initial_orders"] for row in outcome_rows
        if (row["campaign_id"], row["report_date"]) not in source_pairs
        and row["campaign_id"] in source_campaigns
    )

    target_records = [row for row in records if row["campaign_id"] in target_campaigns]
    target_conversions = sum(row["platform_conversions"] for row in target_records)
    target_initial_orders = sum(row["initial_orders"] for row in outcome_rows)
    conversion_order_gap = target_conversions - target_initial_orders

    source_totals = {name: sum((_decimal(row.get(name)) for row, _, _ in data_rows), Decimal(0))
                     for name in RECONCILIATION_COLUMNS}
    summary_totals = {name: sum((_decimal(row.get(name)) for row in summary_rows), Decimal(0))
                      for name in RECONCILIATION_COLUMNS}
    reconciliation_mismatches = sum(
        not math.isclose(float(source_totals[name]), float(summary_totals[name]), abs_tol=0.000001)
        for name in RECONCILIATION_COLUMNS
    ) if summary_rows else len(RECONCILIATION_COLUMNS)

    data_quality = (
        _dq_row("duplicate_ad_day_keys", "DATA_DEFECT", duplicate_keys),
        _dq_row("missing_campaign_ad_group_or_ad_id", "DATA_DEFECT", missing_ids),
        _dq_row("invalid_scientific_notation_ids", "DATA_DEFECT", scientific_ids),
        _dq_row("ids_stored_as_numeric_cells", "DATA_DEFECT", numeric_ids),
        _dq_row("dates_outside_pilot_period", "DATA_DEFECT", outside_dates),
        _dq_row("unexpected_currencies", "DATA_DEFECT", unexpected_currency),
        _dq_row("negative_additive_metrics", "DATA_DEFECT", negative_metrics),
        _dq_row("invalid_additive_metrics", "DATA_DEFECT", invalid_numbers),
        _dq_row("destination_clicks_exceed_impressions", "DATA_DEFECT", click_over_impression),
        _dq_row("impossible_hierarchy_conflicts", "DATA_DEFECT", hierarchy_conflicts),
        _dq_row("target_lightfunnels_campaign_missing_from_tiktok", "DATA_DEFECT", len(target_missing)),
        _dq_row("target_lightfunnels_campaign_name_mismatch", "DATA_DEFECT",
                target_name_mismatches),
        _dq_row("lightfunnels_tiktok_orders_missing_campaign_id", "DATA_DEFECT",
                outcome_profile["lightfunnels_missing_campaign_ids"]),
        _dq_row("source_aggregate_reconciliation", "DATA_DEFECT", reconciliation_mismatches),
        _dq_row("tiktok_campaigns_outside_target_cohort", "OBSERVATION", len(outside_campaigns)),
        _dq_row("ambiguous_identity_matches", "OBSERVATION",
                sum(row["ambiguous_orders"] for row in outcome_rows)),
        _dq_row("unmatched_identity_records", "OBSERVATION",
                sum(row["unmatched_orders"] for row in outcome_rows)),
        _dq_row("fully_inactive_ad_day_rows", "OBSERVATION", inactive_rows),
        _dq_row("zero_spend_rows_with_attributed_activity", "OBSERVATION", zero_spend_attributed),
        _dq_row("campaign_day_boundary_observations", "OBSERVATION", boundary_orders),
        _dq_row("platform_conversions_minus_lightfunnels_orders", "OBSERVATION",
                int(abs(conversion_order_gap)), observed=float(target_conversions),
                expected=float(target_initial_orders)),
        _dq_row("lightfunnels_campaign_mapping_conflicts", "DATA_DEFECT",
                outcome_profile["lightfunnels_campaign_conflicts"]),
    )

    profile = {
        "source_rows": len(data_rows),
        "summary_rows": len(summary_rows),
        "canonical_marketing_rows": len(records),
        "campaigns": len(source_campaigns),
        "ad_groups": len({row["ad_group_id"] for row in records}),
        "ads": len({row["ad_id"] for row in records}),
        "report_start": min((row["report_date"] for row in records), default=None),
        "report_end": max((row["report_date"] for row in records), default=None),
        "currency": tuple(sorted({row["currency"] for row in records})),
        "spend": sum(row["spend"] for row in records),
        "impressions": sum(row["impressions"] for row in records),
        "destination_clicks": sum(row["clicks"] for row in records),
        "platform_conversions": sum(row["platform_conversions"] for row in records),
        "checkouts": sum((row["details"]["checkouts_initiated"] or 0) for row in records),
        "target_campaigns": len(target_campaigns),
        "outside_target_campaigns": len(outside_campaigns),
        "target_spend": sum(row["spend"] for row in target_records),
        "target_impressions": sum(row["impressions"] for row in target_records),
        "target_destination_clicks": sum(row["clicks"] for row in target_records),
        "target_platform_conversions": target_conversions,
        "target_checkouts": sum((row["details"]["checkouts_initiated"] or 0)
                                for row in target_records),
        "tiktok_lightfunnels_orders": outcome_profile["tiktok_lightfunnels_orders"],
        "campaign_day_boundary_observations": boundary_orders,
    }
    return TikTokPilotResult(tuple(records), outcome_rows, data_quality, profile)


def load_private_tiktok(
    root: Path = PRIVATE_ROOT,
    *,
    hmac_key: str | bytes | None = None,
) -> TikTokPilotResult:
    key = hmac_key
    if key is None:
        import os
        key = os.environ.get("PULSE_PILOT_IDENTITY_HMAC_KEY")
    if not key:
        raise ValueError("Set PULSE_PILOT_IDENTITY_HMAC_KEY for privacy-safe pilot matching")
    required = (FINAL_TIKTOK_FILE, LIGHTFUNNELS_FILE, COD_LEADS_FILE, COD_ORDERS_FILE)
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        raise FileNotFoundError("Missing required pilot source file(s): " + ", ".join(missing))
    light_rows = list(_csv_rows(root / LIGHTFUNNELS_FILE))
    phase_result = build_pilot(
        light_rows,
        _xlsx_rows(root / COD_LEADS_FILE),
        _xlsx_rows(root / COD_ORDERS_FILE),
        hmac_key=key,
    )
    return build_tiktok_pilot(_xlsx_rows_exact(root / FINAL_TIKTOK_FILE), light_rows, phase_result)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "summary"))
    parser.add_argument("--root", type=Path, default=PRIVATE_ROOT)
    args = parser.parse_args(argv)
    result = load_private_tiktok(args.root)
    output = {"source_profile": result.source_profile,
              "data_quality": result.data_quality if args.command == "summary" else ()}
    print(json.dumps(_json_safe(output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
