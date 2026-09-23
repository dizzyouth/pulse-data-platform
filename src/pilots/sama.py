"""Privacy-safe Lightfunnels + COD Network pilot integration.

The adapter deliberately lives outside the generic operations/economics adapters:
Lightfunnels represents initial intent, while COD Network owns confirmation and
final commercial/lifecycle truth.  Raw PII is used transiently for HMAC matching
and is never returned by this module.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Iterable, Iterator
from urllib.parse import unquote_plus
from uuid import NAMESPACE_URL, uuid5
import xml.etree.ElementTree as ET
import zipfile
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRIVATE_ROOT = PROJECT_ROOT / "data" / "private" / "sama_pilot"
BUSINESS_ID = "sama_cod_pilot"
SOURCE_TIMEZONE = "Etc/GMT-1"  # IANA fixed UTC+1 (POSIX sign is inverted).
REPORTING_TIMEZONE = SOURCE_TIMEZONE
COHORT_START = date(2026, 3, 1)
COHORT_END = date(2026, 7, 31)
PRODUCT_FAMILY = "orca_aw_product_family"
NATIVE_SKUS = frozenset({"OORCAAW", "OORCAAWV3", "OORCAAWORG"})
PRODUCT_UNIT_COGS_USD = 2.80
RULE_VERSION = "sama_pilot_business_rules_v1"

LIGHTFUNNELS_FILE = "Lightfunnel ordes march 01 to july 31.csv"
COD_LEADS_FILE = "cod Leads - march 01 to july 31.xlsx"
COD_ORDERS_FILE = "cod Orders - march 01 to july 31.xlsx"
TIKTOK_AD_FILE = "Tiktok march 01 to july 31 - ad.xlsx"
TIKTOK_CAMPAIGN_FILE = "Tiktok march 01 to july 31 - campaign.xlsx"

LEAD_STATUS_MAP = {
    "confirmed": ("CONFIRMED", "confirmed"),
    "wrong": ("REJECTED_CONFIRMATION", "invalid_order"),
    "expired": ("UNREACHABLE", "customer_unreachable"),
    "cancelled price": ("REJECTED_CONFIRMATION", "rejected"),
    "cancelled": ("CANCELLED", "cancelled"),
    "black listed": ("REJECTED_CONFIRMATION", "invalid_order"),
}

COUNTRY_CURRENCY = {
    "sa": "SAR", "ksa": "SAR", "saudi arabia": "SAR",
    "ae": "AED", "uae": "AED", "united arab emirates": "AED",
    "kw": "KWD", "kuwait": "KWD",
}

_XLSX_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_XLSX_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PACKAGE_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


@dataclass(frozen=True, slots=True)
class PilotResult:
    order_facts: tuple[dict[str, Any], ...]
    identity_resolution: tuple[dict[str, Any], ...]
    data_quality: tuple[dict[str, Any], ...]
    commerce_orders: tuple[dict[str, Any], ...]
    order_lines: tuple[dict[str, Any], ...]
    operational_events: tuple[dict[str, Any], ...]
    shipments: tuple[dict[str, Any], ...]
    cash_collections: tuple[dict[str, Any], ...]
    product_costs: tuple[dict[str, Any], ...]
    cost_components: tuple[dict[str, Any], ...]
    source_profile: dict[str, Any]

    def counts(self) -> dict[str, int]:
        return {
            "order_facts": len(self.order_facts),
            "identity_resolution": len(self.identity_resolution),
            "data_quality": len(self.data_quality),
            "commerce_orders": len(self.commerce_orders),
            "operational_events": len(self.operational_events),
            "cost_components": len(self.cost_components),
        }


def _cell_column(reference: str) -> int:
    letters = "".join(character for character in reference if character.isalpha())
    result = 0
    for character in letters.upper():
        result = result * 26 + ord(character) - 64
    return result - 1


def _xlsx_rows(path: Path) -> Iterator[dict[str, Any]]:
    """Read the first XLSX worksheet with the standard library only."""
    ns = {"m": _XLSX_MAIN_NS, "r": _XLSX_REL_NS, "p": _PACKAGE_REL_NS}
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
        target = targets[sheet.attrib[f"{{{_XLSX_REL_NS}}}id"]]
        if target.startswith("/"):
            target = target.lstrip("/")
        elif not target.startswith("xl/"):
            target = "xl/" + target

        headers: list[str] | None = None
        with archive.open(target) as stream:
            for _, element in ET.iterparse(stream, events=("end",)):
                if element.tag != f"{{{_XLSX_MAIN_NS}}}row":
                    continue
                cells: dict[int, Any] = {}
                for cell in element.findall("m:c", ns):
                    index = _cell_column(cell.attrib.get("r", "A1"))
                    cell_type = cell.attrib.get("t")
                    value_node = cell.find("m:v", ns)
                    if cell_type == "inlineStr":
                        value: Any = "".join(node.text or "" for node in cell.findall(".//m:t", ns))
                    elif value_node is None:
                        value = ""
                    elif cell_type == "s":
                        value = shared[int(value_node.text or 0)]
                    elif cell_type == "b":
                        value = value_node.text == "1"
                    else:
                        raw = value_node.text or ""
                        try:
                            numeric = float(raw)
                            value = int(numeric) if numeric.is_integer() else numeric
                        except ValueError:
                            value = raw
                    cells[index] = value
                if cells:
                    values = [cells.get(index, "") for index in range(max(cells) + 1)]
                    if headers is None:
                        headers = [str(value).strip() for value in values]
                    else:
                        yield {header: values[index] if index < len(values) else ""
                               for index, header in enumerate(headers) if header}
                element.clear()


def _csv_rows(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        yield from csv.DictReader(stream)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _identifier(value: Any) -> str:
    text = _text(value)
    return text[:-2] if text.endswith(".0") and text[:-2].isdigit() else text


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = _text(value).replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        number = float(match.group())
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None and number.is_integer() else None


def _timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        # Excel's 1900 date system (including its historical leap-year bug).
        local = datetime(1899, 12, 30) + timedelta(days=float(value))
        return local.replace(tzinfo=ZoneInfo(SOURCE_TIMEZONE)).astimezone(timezone.utc)
    text = _text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        for pattern in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                        "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(SOURCE_TIMEZONE))
    return parsed.astimezone(timezone.utc)


def normalize_phone(value: Any) -> str | None:
    """Normalize a phone transiently; callers must not persist this value."""
    digits = re.sub(r"\D", "", _text(value))
    if digits.startswith("00"):
        digits = digits[2:]
    # The two providers inconsistently use local and international notation.
    # Match the national significant number for the three confirmed markets;
    # malformed cells containing multiple concatenated phones are rejected.
    if len(digits) > 15:
        return None
    for calling_code in ("966", "971", "965"):
        if digits.startswith(calling_code):
            digits = digits[len(calling_code):]
            break
    digits = digits.lstrip("0")
    return digits if 7 <= len(digits) <= 10 else None


def phone_linkage_key(value: Any, secret: str | bytes) -> str | None:
    normalized = normalize_phone(value)
    if not normalized:
        return None
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    if not key:
        raise ValueError("PULSE_PILOT_IDENTITY_HMAC_KEY cannot be empty")
    return hmac.new(key, normalized.encode("ascii"), hashlib.sha256).hexdigest()


def _phone_country(value: Any) -> str | None:
    digits = re.sub(r"\D", "", _text(value))
    if digits.startswith("00"):
        digits = digits[2:]
    for calling_code, country in (("966", "SA"), ("971", "AE"), ("965", "KW")):
        if digits.startswith(calling_code):
            return country
    return None


def _stable_id(kind: str, *parts: Any) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join((BUSINESS_ID, kind, *(str(part) for part in parts)))))


def _country(value: Any) -> str | None:
    result = re.sub(r"\s+", " ", _text(value)).strip()
    return result.upper() if result else None


def _currency_for_country(value: Any) -> str | None:
    return COUNTRY_CURRENCY.get(re.sub(r"\s+", " ", _text(value)).strip().lower())


def _sku_values(value: Any) -> tuple[str, ...]:
    upper = _text(value).upper()
    return tuple(sorted(sku for sku in NATIVE_SKUS if sku in upper))


def _has_unexpected_sku(value: Any) -> bool:
    text = _text(value).upper()
    if not text:
        return False
    tokens = set(re.findall(r"\b[A-Z][A-Z0-9_-]{3,}\b", text))
    ignored = {"QUANTITY", "PRODUCT", "PRODUCTS"}
    candidates = tokens - ignored
    return bool(candidates and not candidates <= NATIVE_SKUS)


def _lead_quantity(row: dict[str, Any]) -> int | None:
    value = _text(row.get("Products/Sku"))
    for pattern in (r"(?:QTY|QUANTITY)\s*[:=x-]?\s*(\d+)", r"\b(\d+)\s*[xX]\b"):
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            return int(match.group(1))
    # Owner-supplied offer rules are a safe fallback when the provider field
    # omits explicit quantity.
    price = _number(row.get("Price"))
    return 2 if price == 199 else 4 if price == 299 else None


def _safe_details(**values: Any) -> dict[str, Any]:
    """Allowlist non-PII lineage metadata."""
    return {key: value for key, value in values.items() if value is not None}


def _utm_attributes(value: Any) -> dict[str, str | None]:
    """Parse JSON, delimited, or Lightfunnels' concatenated UTM attributes."""
    text = _text(value)
    parsed: dict[str, Any] = {}
    if text:
        try:
            candidate = json.loads(text)
            if isinstance(candidate, dict):
                parsed = {str(key).lower().replace(" ", "_"): item for key, item in candidate.items()}
        except (TypeError, ValueError):
            matches = list(re.finditer(
                r"(?:utm[_ ]?)?(source|id|campaign|medium|content|term)\s*[:=]\s*[\"']?",
                text,
                re.IGNORECASE,
            ))
            for index, match in enumerate(matches):
                end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
                item = text[match.end():end].strip(" \t\r\n,;&\"'}")
                parsed[f"utm_{match.group(1).lower()}"] = unquote_plus(item)
    result = {}
    for name in ("source", "id", "medium", "campaign", "content", "term"):
        item = parsed.get(f"utm_{name}", parsed.get(name))
        cleaned = _text(item)
        result[f"utm_{name}"] = cleaned[:255] if cleaned else None
    return result


def _compare_at_value(value: Any) -> float | None:
    text = _text(value)
    match = re.search(r"compare(?:[_ -]?at)?[^\d]{0,20}(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    return float(match.group(1)) if match else None


def _lightfunnel_orders(rows: Iterable[dict[str, Any]], secret: str | bytes):
    materialized = list(rows)
    exact_counts = Counter(hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
                           for row in materialized)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing = 0
    for row in materialized:
        order_id = _identifier(row.get("Order Id"))
        if not order_id:
            missing += 1
            continue
        groups[order_id].append(row)
    output = []
    conflicting_totals = 0
    duplicate_pairs = 0
    for order_id, members in groups.items():
        totals = {_number(row.get("Order Total")) for row in members if _number(row.get("Order Total")) is not None}
        if len(totals) > 1:
            conflicting_totals += 1
        item_ids = {_identifier(row.get("Item Id")) for row in members if _identifier(row.get("Item Id"))}
        duplicate_pairs += len(members) - len({(_identifier(row.get("Order Id")), _identifier(row.get("Item Id")))
                                               for row in members})
        first = min(members, key=lambda row: _timestamp(row.get("Created at")) or datetime.max.replace(tzinfo=timezone.utc))
        skus = tuple(sorted({sku for row in members for sku in _sku_values(row.get("Sku"))}))
        country = _country(first.get("Shipping Country"))
        initial_total = min(totals) if totals else None
        item_prices = sorted({_number(row.get("Item Price")) for row in members
                              if _number(row.get("Item Price")) is not None})
        compare_values = sorted({_compare_at_value(row.get("Order Custom Data")) for row in members
                                 if _compare_at_value(row.get("Order Custom Data")) is not None})
        output.append({
            "order_id": order_id,
            "created_at": _timestamp(first.get("Created at")),
            "initial_quantity": len(item_ids) or None,
            "initial_total": initial_total,
            "initial_currency": _currency_for_country(country),
            "country": country,
            "native_skus": skus,
            "unexpected_sku": any(_has_unexpected_sku(row.get("Sku")) for row in members),
            "linkage_key": phone_linkage_key(first.get("Phone"), secret),
            "item_ids": item_ids,
            "source_item_price": item_prices[0] if len(item_prices) == 1 else None,
            "compare_at_value": compare_values[0] if len(compare_values) == 1 else None,
            **_utm_attributes(first.get("UTM Attributes")),
        })
    profile = {
        "raw_rows": len(materialized), "unique_order_ids": len(groups),
        "distinct_item_ids": len({_identifier(row.get("Item Id")) for row in materialized
                                  if _identifier(row.get("Item Id"))}),
        "exact_duplicate_rows": sum(count - 1 for count in exact_counts.values()),
        "duplicate_order_item_rows": duplicate_pairs,
        "missing_order_identifiers": missing,
        "conflicting_initial_totals": conflicting_totals,
    }
    return sorted(output, key=lambda row: row["order_id"]), profile


def _lead_rows(rows: Iterable[dict[str, Any]], secret: str | bytes):
    output = []
    for row in rows:
        lead_id = _identifier(row.get("ID"))
        if not lead_id:
            continue
        output.append({
            "lead_id": lead_id,
            "status": _text(row.get("Status")),
            "created_at": _timestamp(row.get("Created At")),
            "call_at": _timestamp(row.get("Call At")),
            "quantity": _lead_quantity(row),
            "total": _number(row.get("Price")),
            "linkage_key": phone_linkage_key(row.get("Phone"), secret),
            "country": _phone_country(row.get("Phone")),
            "native_skus": _sku_values(row.get("Products/Sku")),
        })
    return output


def _order_rows(rows: Iterable[dict[str, Any]]):
    output = []
    for row in rows:
        reference = _identifier(row.get("Reference"))
        if not reference:
            continue
        output.append({
            "reference": reference, "lead_id": _identifier(row.get("Lead ID")),
            "status": _text(row.get("Status")), "created_at": _timestamp(row.get("Created At")),
            "updated_at": _timestamp(row.get("Last Update")), "shipped_at": _timestamp(row.get("Shipped At")),
            "delivered_at": _timestamp(row.get("Delivered At")), "returned_at": _timestamp(row.get("Returned At")),
            "quantity": _integer(row.get("Quantity")), "total": _number(row.get("Total")),
            "currency": _text(row.get("Currency")).upper(), "native_skus": _sku_values(row.get("Sku")),
            "shipping_fee_display": _number(row.get("Shipping Fees")),
            "total_usd_present": _number(row.get("Total USD")) is not None,
            "country": _country(row.get("Customer Country")),
            "unexpected_sku": _has_unexpected_sku(row.get("Sku")),
        })
    return output


def resolve_identity(light_orders: Iterable[dict[str, Any]], leads: Iterable[dict[str, Any]]):
    by_phone: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for lead in leads:
        if lead["linkage_key"]:
            by_phone[lead["linkage_key"]].append(lead)
    results = []
    for order in light_orders:
        candidates = []
        for lead in by_phone.get(order["linkage_key"], []):
            if order["created_at"] and lead["created_at"]:
                delta = (lead["created_at"] - order["created_at"]).total_seconds() / 60
                if abs(delta) > 48 * 60:
                    continue
            else:
                delta = None
            score = 0.55
            method = ["HMAC_PHONE"]
            if order["initial_quantity"] is not None and lead["quantity"] == order["initial_quantity"]:
                score += 0.15; method.append("QUANTITY")
            if order["initial_total"] is not None and lead["total"] is not None and math.isclose(
                    order["initial_total"], lead["total"], abs_tol=0.01):
                score += 0.15; method.append("TOTAL")
            if delta is not None:
                score += 0.10 if 0 <= delta <= 180 else 0.05 if abs(delta) <= 1440 else 0
                method.append("TIME_PROXIMITY")
            # Country is intentionally only a bonus when both sources expose it.
            if order.get("country") and lead.get("country") and order["country"] == lead["country"]:
                score += 0.05; method.append("COUNTRY")
            candidates.append((round(score, 4), abs(delta) if delta is not None else float("inf"),
                               lead["lead_id"], lead, "+".join(method), delta))
        candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
        if not candidates or candidates[0][0] < 0.80:
            status, selected = "UNMATCHED", None
        elif len(candidates) > 1 and candidates[0][0] - candidates[1][0] < 0.05:
            status, selected = "AMBIGUOUS", None
        else:
            status, selected = "MATCHED_HIGH_CONFIDENCE", candidates[0]
        results.append({
            "business_id": BUSINESS_ID, "lightfunnel_order_id": order["order_id"],
            "cod_lead_id": selected[2] if selected else None,
            "match_status": status, "match_method": selected[4] if selected else (
                candidates[0][4] if candidates else "NO_HMAC_PHONE_CANDIDATE"),
            "confidence": selected[0] if selected else (candidates[0][0] if candidates else 0.0),
            "time_delta_minutes": round(selected[5], 2) if selected and selected[5] is not None else None,
            "linkage_key": order["linkage_key"],
        })
    # A COD lead is a single provider entity and may not be credited to more
    # than one Lightfunnels intent.  Conservatively reject every collision;
    # selecting an arbitrary winner would silently duplicate/steal lifecycle
    # and economics truth.
    by_selected_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for result in results:
        if result["match_status"] == "MATCHED_HIGH_CONFIDENCE" and result["cod_lead_id"]:
            by_selected_lead[result["cod_lead_id"]].append(result)
    for members in by_selected_lead.values():
        if len(members) > 1:
            for result in members:
                result["cod_lead_id"] = None
                result["match_status"] = "AMBIGUOUS"
                result["match_method"] = "LEAD_COLLISION+" + result["match_method"]
    return results


def _event(order_id: str, source_type: str, provider_status: str, canonical_status: str,
           event_at: datetime, *, shipment_id: str | None = None, details: dict[str, Any] | None = None):
    external = f"{source_type}:{order_id}:{provider_status}:{event_at.isoformat()}"
    return {
        "business_id": BUSINESS_ID, "source_type": source_type,
        "source_id": "lightfunnels_intent" if source_type == "commerce_orders" else "cod_network",
        "provider": "lightfunnels" if source_type == "commerce_orders" else "cod_network",
        "event_id": _stable_id("event", external), "external_event_id": external,
        "order_id": order_id, "shipment_id": shipment_id, "event_type": source_type.rstrip("s"),
        "canonical_status": canonical_status, "provider_status": provider_status,
        "event_at": event_at, "received_at_utc": event_at, "revision": 1,
        "corrects_event_id": None, "details": details or {},
    }


def _cost(order_id: str, cost_type: str, amount: float, currency: str, effective_at: datetime,
          *, shipment_id: str | None = None):
    external = f"{RULE_VERSION}:{order_id}:{cost_type}:{currency}"
    return {
        "business_id": BUSINESS_ID, "source_id": "sama_business_cost_rules",
        "provider": "business_owner_rule", "cost_component_id": _stable_id("cost", external),
        "external_record_id": external, "cost_type": cost_type, "amount": round(amount, 6),
        "currency": currency, "effective_at": effective_at, "received_at_utc": effective_at,
        "cost_basis": "ESTIMATED", "cost_scope": "SHIPMENT" if shipment_id else "ORDER",
        "precedence_key": external, "order_id": order_id, "shipment_id": shipment_id,
        "remittance_id": None, "product_id": None, "variant_id": None, "sku": None,
        "revision": 1, "corrects_record_id": None,
        "details": {"rule_basis": "BUSINESS_RULE", "rule_version": RULE_VERSION},
    }


def _validate_canonical_outputs(commerce, lines, events, shipments, collections, product_costs, costs) -> None:
    """Exercise existing provider-neutral entity invariants without changing them."""
    from src.economics.models import (CostBasis, CostComponent, CostScope, CostType,
                                      ProductCost)
    from src.operations.models import (CashCollection, CommerceOrder, OperationalEvent,
                                       OperationalStatus, OrderLine, PaymentType, Shipment)

    lines_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in lines:
        lines_by_order[row["order_id"]].append(row)
        OrderLine(**row)
    for row in commerce:
        CommerceOrder(**{**row, "payment_type": PaymentType(row["payment_type"]),
                         "lines": tuple(OrderLine(**item) for item in lines_by_order[row["order_id"]])})
    for row in events:
        OperationalEvent(**{**row, "canonical_status": OperationalStatus(row["canonical_status"])})
    for row in shipments:
        Shipment(**{key: value for key, value in row.items() if key != "source_id"})
    for row in collections:
        CashCollection(**row)
    for row in product_costs:
        ProductCost(**row)
    for row in costs:
        clean = {key: value for key, value in row.items() if key != "received_at_utc"}
        CostComponent(**{**clean, "cost_type": CostType(row["cost_type"]),
                         "cost_basis": CostBasis(row["cost_basis"]),
                         "cost_scope": CostScope(row["cost_scope"])})


def build_pilot(light_rows: Iterable[dict[str, Any]], lead_rows: Iterable[dict[str, Any]],
                order_rows: Iterable[dict[str, Any]], *, hmac_key: str | bytes) -> PilotResult:
    lights, light_profile = _lightfunnel_orders(light_rows, hmac_key)
    leads = _lead_rows(lead_rows, hmac_key)
    cod_orders = _order_rows(order_rows)
    identities = resolve_identity(lights, leads)
    lead_by_id = {row["lead_id"]: row for row in leads}
    orders_by_lead: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cod_orders:
        if row["lead_id"]:
            orders_by_lead[row["lead_id"]].append(row)

    facts, commerce, lines, events, shipments, collections, costs = [], [], [], [], [], [], []
    dq = Counter({name: 0 for name in (
        "duplicate_lightfunnels_raw_rows", "duplicate_lightfunnels_order_item_fanout",
        "conflicting_initial_totals", "conflicting_final_totals", "conflicting_currencies",
        "missing_order_identifiers", "missing_initial_currency", "missing_cod_order_lead_ids", "cod_order_lead_without_cod_lead",
        "confirmed_lead_without_cod_order", "ambiguous_identity_match", "unmatched_lightfunnel_order",
        "cod_lead_reused_by_multiple_lightfunnel_orders",
        "lifecycle_contradictions", "delivered_missing_delivered_at", "return_missing_returned_at",
        "delivered_missing_total_or_currency", "delivered_collection_total_mismatch",
        "stockout_charged_despite_never_shipped", "returned_incorrectly_retaining_cogs",
        "stockout_source_shipping_default_ignored", "source_total_usd_ignored", "unexpected_sku",
        "impossible_quantity", "final_order_changed_from_intent", "failed_confirmation_business_outcomes",
        "return_business_outcomes", "eligible_cod_records_outside_cohort",
        "returned_orders_with_zero_permanent_cogs", "stockouts_with_zero_rule_logistics",
    )})
    for identity in identities:
        order = next(item for item in lights if item["order_id"] == identity["lightfunnel_order_id"])
        if order["initial_currency"] is None: dq["missing_initial_currency"] += 1
        if identity["match_status"] == "AMBIGUOUS": dq["ambiguous_identity_match"] += 1
        if identity["match_status"] == "UNMATCHED": dq["unmatched_lightfunnel_order"] += 1
        lead = lead_by_id.get(identity["cod_lead_id"] or "")
        linked_orders = orders_by_lead.get(identity["cod_lead_id"] or "", [])
        if lead and lead["status"].strip().lower() == "confirmed" and not linked_orders:
            dq["confirmed_lead_without_cod_order"] += 1
        final = max(linked_orders, key=lambda item: (item["updated_at"] or item["created_at"] or datetime.min.replace(tzinfo=timezone.utc), item["reference"])) if linked_orders else None
        if len(linked_orders) > 1:
            totals = {(item["total"], item["currency"]) for item in linked_orders}
            if len({item[0] for item in totals}) > 1: dq["conflicting_final_totals"] += 1
            if len({item[1] for item in totals}) > 1: dq["conflicting_currencies"] += 1

        lead_status = lead["status"] if lead else None
        lead_key = (lead_status or "").strip().lower()
        confirmation = LEAD_STATUS_MAP.get(lead_key)
        final_status = (final["status"] if final else "").strip().lower()
        shipped = bool(final and final["shipped_at"])
        delivered = bool(final and final_status == "delivered")
        returned = bool(final and final_status == "return")
        out_of_stock = bool(final and final_status == "out of stock")
        cancelled = bool(final and final_status == "cancel")
        if delivered and not final["delivered_at"]: dq["delivered_missing_delivered_at"] += 1
        if returned and not final["returned_at"]: dq["return_missing_returned_at"] += 1
        if delivered and (final["total"] is None or not re.fullmatch(r"[A-Z]{3}", final["currency"] or "")):
            dq["delivered_missing_total_or_currency"] += 1
        if out_of_stock and not shipped and final["shipping_fee_display"]:
            dq["stockout_source_shipping_default_ignored"] += 1
        if final and final["quantity"] is not None and final["quantity"] <= 0: dq["impossible_quantity"] += 1
        all_skus = set(order["native_skus"]) | (set(final["native_skus"]) if final else set())
        if order["unexpected_sku"] or (final and final["unexpected_sku"]): dq["unexpected_sku"] += 1
        if final:
            chronology = [
                final["delivered_at"] and final["shipped_at"] and final["delivered_at"] < final["shipped_at"],
                final["returned_at"] and final["shipped_at"] and final["returned_at"] < final["shipped_at"],
                final["returned_at"] and final["delivered_at"] and final["returned_at"] < final["delivered_at"],
                (delivered or returned) and not shipped,
                delivered and bool(final["returned_at"]),
            ]
            if any(chronology): dq["lifecycle_contradictions"] += 1

        final_quantity = final["quantity"] if final else None
        final_total = final["total"] if final else None
        currency = final["currency"] if final and re.fullmatch(r"[A-Z]{3}", final["currency"] or "") else order["initial_currency"]
        quantity_changed = final_quantity is not None and final_quantity != order["initial_quantity"]
        value_changed = final_total is not None and order["initial_total"] is not None and not math.isclose(final_total, order["initial_total"], abs_tol=.01)
        if quantity_changed or value_changed: dq["final_order_changed_from_intent"] += 1
        if lead and lead_key != "confirmed": dq["failed_confirmation_business_outcomes"] += 1
        if returned: dq["return_business_outcomes"] += 1

        confirmed = lead_key == "confirmed"
        call_center_usd = 0.0 if lead_key == "wrong" or not lead else (3.0 if confirmed and delivered else 2.0 if confirmed else 0.5)
        logistics_usd = 4.99 if delivered else 2.99 if shipped else 0.0
        product_cogs_usd = (final_quantity or 0) * PRODUCT_UNIT_COGS_USD if delivered else 0.0
        collected = final_total if delivered and final_total is not None else 0.0
        cod_fee = collected * .05
        known_usd = call_center_usd + logistics_usd + product_cogs_usd
        economic_status = "FX_REQUIRED" if collected and currency != "USD" and known_usd else "SEPARATE_CURRENCIES"
        effective_at = (final["delivered_at"] or final["returned_at"] or final["shipped_at"] or
                        final["updated_at"] or lead["created_at"] if final and lead else
                        lead["created_at"] if lead else order["created_at"])
        effective_at = effective_at or datetime(1970, 1, 1, tzinfo=timezone.utc)
        shipment_id = _stable_id("shipment", final["reference"]) if final and shipped else None

        fact = {
            "business_id": BUSINESS_ID, "lightfunnel_order_id": order["order_id"],
            "cod_lead_id": identity["cod_lead_id"], "cod_order_reference": final["reference"] if final else None,
            "intent_created_at": order["created_at"], "match_status": identity["match_status"],
            "lead_status": lead_status, "final_order_status": final["status"] if final else None,
            "initial_quantity": order["initial_quantity"], "initial_total": order["initial_total"],
            "initial_currency": order["initial_currency"], "final_quantity": final_quantity,
            "final_total": final_total, "currency": currency, "quantity_changed": quantity_changed,
            "value_changed": value_changed, "confirmed": confirmed, "shipped": shipped,
            "delivered": delivered, "returned": returned, "out_of_stock": out_of_stock,
            "cancelled": cancelled, "cash_collected": collected, "cod_fee_native": cod_fee,
            "product_cogs_usd": product_cogs_usd, "call_center_cost_usd": call_center_usd,
            "logistics_cost_usd": logistics_usd, "known_operational_cost_usd": known_usd,
            "economic_status": economic_status, "native_sku": next(iter(sorted(all_skus)), None),
            "product_family": PRODUCT_FAMILY,
            "source_item_price": order["source_item_price"],
            "compare_at_value": order["compare_at_value"],
            "utm_source": order["utm_source"], "utm_medium": order["utm_medium"],
            "utm_campaign": order["utm_campaign"], "utm_content": order["utm_content"],
            "utm_term": order["utm_term"],
        }
        facts.append(fact)
        if delivered and not math.isclose(collected, final_total or 0.0, abs_tol=.000001):
            dq["delivered_collection_total_mismatch"] += 1
        if returned and product_cogs_usd:
            dq["returned_incorrectly_retaining_cogs"] += 1
        if out_of_stock and not shipped and logistics_usd:
            dq["stockout_charged_despite_never_shipped"] += 1

        if identity["match_status"] != "MATCHED_HIGH_CONFIDENCE":
            continue
        order_value = final_total if final_total is not None else order["initial_total"]
        order_currency = currency
        if order_value is None or order_currency is None or order["created_at"] is None:
            continue
        commerce.append({
            "business_id": BUSINESS_ID, "source_type": "commerce_orders", "source_id": "sama_pilot_adapter",
            "provider": "cod_network", "order_id": order["order_id"],
            "external_order_id": final["reference"] if final else None,
            "order_created_at": order["created_at"], "order_updated_at": final["updated_at"] if final and final["updated_at"] else order["created_at"],
            "source_timezone": SOURCE_TIMEZONE, "reporting_timezone": REPORTING_TIMEZONE,
            "currency": order_currency, "order_value": order_value, "payment_type": "cod",
            "customer_reference": None,
        })
        line_sku = next(iter(sorted(all_skus)), None)
        line_quantity = final_quantity or order["initial_quantity"]
        if line_quantity and line_sku:
            lines.append({"business_id": BUSINESS_ID, "order_id": order["order_id"],
                          "line_id": _stable_id("line", order["order_id"]), "product_id": PRODUCT_FAMILY,
                          "variant_id": None, "sku": line_sku, "quantity": line_quantity,
                          "unit_price": order_value / line_quantity, "currency": order_currency})
        events.append(_event(order["order_id"], "commerce_orders", "Lightfunnels intent", "CREATED", order["created_at"],
                             details=_safe_details(initial_quantity=order["initial_quantity"], initial_total=order["initial_total"])))
        if lead and lead["created_at"] and confirmation:
            events.append(_event(order["order_id"], "confirmation_events", lead["status"], confirmation[0], lead["created_at"],
                                 details={"outcome": confirmation[1], "provider_reason_code": lead["status"]}))
        if final and shipped:
            events.append(_event(order["order_id"], "fulfillment_events", "Shipped", "SHIPPED", final["shipped_at"], shipment_id=shipment_id))
            shipments.append({"business_id": BUSINESS_ID, "source_id": "cod_network", "provider": "cod_network",
                              "order_id": order["order_id"], "fulfillment_id": _stable_id("fulfillment", final["reference"]),
                              "shipment_id": shipment_id, "courier": None, "tracking_reference": None,
                              "shipment_created_at": final["shipped_at"], "shipped_at": final["shipped_at"],
                              "delivered_at": final["delivered_at"], "return_at": final["returned_at"],
                              "line_quantities": {lines[-1]["line_id"]: line_quantity} if line_quantity and lines else {}})
        if final and delivered and final["delivered_at"]:
            events.append(_event(order["order_id"], "delivery_events", final["status"], "DELIVERED", final["delivered_at"], shipment_id=shipment_id))
            collection_id = _stable_id("collection", final["reference"])
            collections.append({"business_id": BUSINESS_ID, "source_id": "cod_network", "provider": "cod_network",
                                "collection_id": collection_id, "order_id": order["order_id"], "shipment_id": shipment_id,
                                "cash_expected": collected, "cash_collected": collected, "collection_currency": currency,
                                "collected_at": final["delivered_at"], "remittance_id": None})
            events.append(_event(order["order_id"], "cod_collections", "Delivered/full COD", "CASH_COLLECTED",
                                 final["delivered_at"], shipment_id=shipment_id,
                                 details={"collection_id": collection_id}))
        if final and returned and final["returned_at"]:
            events.append(_event(order["order_id"], "delivery_events", final["status"], "RETURNED_TO_ORIGIN", final["returned_at"], shipment_id=shipment_id))

        if call_center_usd:
            costs.append(_cost(order["order_id"], "OTHER_VARIABLE_COST", call_center_usd, "USD", effective_at))
        if logistics_usd:
            costs.append(_cost(order["order_id"], "SHIPPING", logistics_usd, "USD", effective_at, shipment_id=shipment_id))
        if cod_fee and currency:
            costs.append(_cost(order["order_id"], "COD_FEE", cod_fee, currency, effective_at, shipment_id=shipment_id))

    linked_leads = {row["cod_lead_id"] for row in identities if row["cod_lead_id"]}
    dq["cod_order_lead_without_cod_lead"] = sum(1 for row in cod_orders if row["lead_id"] not in lead_by_id)
    dq["eligible_cod_records_outside_cohort"] = sum(1 for row in cod_orders if row["lead_id"] not in linked_leads)
    dq["duplicate_lightfunnels_raw_rows"] = light_profile["exact_duplicate_rows"]
    dq["duplicate_lightfunnels_order_item_fanout"] = light_profile["duplicate_order_item_rows"]
    dq["conflicting_initial_totals"] = light_profile["conflicting_initial_totals"]
    dq["missing_order_identifiers"] = light_profile["missing_order_identifiers"]
    dq["missing_cod_order_lead_ids"] = sum(1 for row in cod_orders if not row["lead_id"])
    dq["source_total_usd_ignored"] = sum(1 for row in cod_orders if row["total_usd_present"])
    dq["cod_lead_reused_by_multiple_lightfunnel_orders"] = sum(
        1 for row in identities if row["match_method"].startswith("LEAD_COLLISION+"))
    dq["returned_orders_with_zero_permanent_cogs"] = sum(1 for row in facts if row["returned"] and row["product_cogs_usd"] == 0)
    dq["stockouts_with_zero_rule_logistics"] = sum(1 for row in facts if row["out_of_stock"] and not row["shipped"] and row["logistics_cost_usd"] == 0)
    dq_rows = tuple({"business_id": BUSINESS_ID, "check_name": name,
                     "category": "BUSINESS_OUTCOME" if name in {"final_order_changed_from_intent",
                                                                  "failed_confirmation_business_outcomes",
                                                                  "return_business_outcomes"} else
                                 "OBSERVATION" if name in {"source_total_usd_ignored", "eligible_cod_records_outside_cohort",
                                                           "returned_orders_with_zero_permanent_cogs", "stockouts_with_zero_rule_logistics",
                                                           "stockout_source_shipping_default_ignored"} else "DATA_DEFECT",
                     "issue_count": int(count)} for name, count in sorted(dq.items()))
    product_costs = tuple({
        "business_id": BUSINESS_ID, "source_id": "sama_business_cost_rules", "provider": "business_owner_rule",
        "cost_record_id": _stable_id("product-cost", sku), "external_record_id": f"{RULE_VERSION}:{sku}",
        "product_id": PRODUCT_FAMILY, "variant_id": None, "sku": sku, "unit_cogs": PRODUCT_UNIT_COGS_USD,
        "currency": "USD", "valid_from": date(2026, 3, 1), "valid_to": date(2026, 7, 31), "revision": 1,
        "details": {"rule_basis": "BUSINESS_RULE", "rule_version": RULE_VERSION,
                    "recognition": "delivered_only; returned inventory recoverable"},
    } for sku in sorted(NATIVE_SKUS))
    profile = {**light_profile, "cod_lead_rows": len(leads), "unique_cod_lead_ids": len(lead_by_id),
               "cod_order_rows": len(cod_orders), "unique_cod_order_references": len({row["reference"] for row in cod_orders}),
               "cod_lead_statuses": tuple(sorted({row["status"] for row in leads})),
               "cod_order_statuses": tuple(sorted({row["status"] for row in cod_orders})),
               "cod_order_lead_ids_present": sum(1 for row in cod_orders if row["lead_id"] in lead_by_id)}
    _validate_canonical_outputs(commerce, lines, events, shipments, collections, product_costs, costs)
    return PilotResult(tuple(facts), tuple(identities), dq_rows, tuple(commerce), tuple(lines),
                       tuple(events), tuple(shipments), tuple(collections), product_costs, tuple(costs), profile)


def load_private_pilot(root: Path = PRIVATE_ROOT, *, hmac_key: str | bytes | None = None) -> PilotResult:
    key = hmac_key or os.environ.get("PULSE_PILOT_IDENTITY_HMAC_KEY")
    if not key:
        raise ValueError("Set PULSE_PILOT_IDENTITY_HMAC_KEY for privacy-safe pilot matching")
    required = (LIGHTFUNNELS_FILE, COD_LEADS_FILE, COD_ORDERS_FILE)
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        raise FileNotFoundError("Missing required pilot source file(s): " + ", ".join(missing))
    return build_pilot(_csv_rows(root / LIGHTFUNNELS_FILE), _xlsx_rows(root / COD_LEADS_FILE),
                       _xlsx_rows(root / COD_ORDERS_FILE), hmac_key=key)


def profile_tiktok_exports(root: Path = PRIVATE_ROOT) -> dict[str, Any]:
    """Return only non-sensitive schema facts; TikTok remains outside canonical marketing."""
    output = {}
    for filename in (TIKTOK_AD_FILE, TIKTOK_CAMPAIGN_FILE):
        path = root / filename
        rows = list(_xlsx_rows(path)) if path.is_file() else []
        columns = tuple(rows[0]) if rows else ()
        output[filename] = {
            "rows": len(rows), "columns": columns,
            "observed_grain": "campaign_period_aggregate" if "Campaign name" in columns else "ad_period_aggregate",
            "daily_compatible": False,
        }
    return output


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "summary", "tiktok-profile"))
    parser.add_argument("--root", type=Path, default=PRIVATE_ROOT)
    args = parser.parse_args(argv)
    if args.command == "tiktok-profile":
        output = profile_tiktok_exports(args.root)
    else:
        result = load_private_pilot(args.root)
        output = {"source_profile": result.source_profile, "counts": result.counts()}
        if args.command == "summary":
            output["funnel"] = {
                "initial_orders": len(result.order_facts),
                "matched": sum(row["match_status"] == "MATCHED_HIGH_CONFIDENCE" for row in result.order_facts),
                "ambiguous": sum(row["match_status"] == "AMBIGUOUS" for row in result.order_facts),
                "unmatched": sum(row["match_status"] == "UNMATCHED" for row in result.order_facts),
                "confirmed": sum(row["confirmed"] for row in result.order_facts),
                "shipped": sum(row["shipped"] for row in result.order_facts),
                "delivered": sum(row["delivered"] for row in result.order_facts),
                "returned": sum(row["returned"] for row in result.order_facts),
            }
    print(json.dumps(_json_safe(output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
