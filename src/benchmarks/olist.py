"""Olist public-data benchmark using Pulse ingestion and canonical models.

The input CSV files are read-only.  Generated benchmark artifacts are written
under the ignored ``data/benchmarks`` tree as newline-delimited JSON so the
full benchmark remains offline and does not require pandas.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from math import fsum
from pathlib import Path
from statistics import mean, median
import time
from typing import Any, Iterable, Mapping, Sequence
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from src.economics.pipeline import build_gold_rows as build_economics_gold
from src.onboarding.adapters import AdapterHealth
from src.onboarding.contracts import contract_for
from src.onboarding.models import IngestionEnvelope, SourceConfig
from src.operations.models import (CommerceOrder, OperationalEvent,
                                   OperationalStatus, OrderLine, PaymentType)
from src.operations.pipeline import build_gold_rows as build_operations_gold
from src.quality.operations import check_operations
from src.quality.anomaly import MetricSeries, evaluate
from src.quality.anomaly_runner import policy_for


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "config" / "benchmarks" / "olist.json"
OLIST_BUSINESS_ID = "public_olist_benchmark"
OLIST_SOURCE_ID = "olist_public_files"
PROVIDER = "olist_public"
RECONCILIATION_TOLERANCE = Decimal("0.01")

EXPECTED_HEADERS = {
    "customers": ("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"),
    "orders": ("order_id", "customer_id", "order_status", "order_purchase_timestamp",
               "order_approved_at", "order_delivered_carrier_date",
               "order_delivered_customer_date", "order_estimated_delivery_date"),
    "order_items": ("order_id", "order_item_id", "product_id", "seller_id",
                    "shipping_limit_date", "price", "freight_value"),
    "payments": ("order_id", "payment_sequential", "payment_type",
                 "payment_installments", "payment_value"),
    "products": ("product_id", "product_category_name", "product_name_lenght",
                 "product_description_lenght", "product_photos_qty", "product_weight_g",
                 "product_length_cm", "product_height_cm", "product_width_cm"),
    "sellers": ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
    "category_translation": ("product_category_name", "product_category_name_english"),
}

# These are current-state mappings only.  The event stream below emits actual
# observed milestones, not synthetic events for snapshot-only statuses.
SAFE_STATUS_MAP = {
    "created": OperationalStatus.CREATED.value,
    "approved": OperationalStatus.CONFIRMED.value,
    "shipped": OperationalStatus.SHIPPED.value,
    "delivered": OperationalStatus.DELIVERED.value,
    "canceled": OperationalStatus.CANCELLED.value,
    "invoiced": None,
    "processing": None,
    "unavailable": None,
}


@dataclass(frozen=True, slots=True)
class BenchmarkIssue:
    code: str
    severity: str
    classification: str
    count: int
    sample_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OlistManifest:
    raw: Mapping[str, Any]

    @classmethod
    def load(cls, path: Path = MANIFEST_PATH) -> "OlistManifest":
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("Olist benchmark manifest must contain one object")
        return cls(value)

    @property
    def required_files(self) -> Mapping[str, Mapping[str, Any]]:
        return self.raw["required_files"]

    def root(self, *, fixture: bool) -> Path:
        key = "fixture_path" if fixture else "expected_local_path"
        return (PROJECT_ROOT / self.raw[key]).resolve()

    def validate(self, root: Path, *, check_headers: bool = True) -> tuple[str, ...]:
        errors: list[str] = []
        if not root.is_dir():
            return (f"Olist dataset directory is missing: {root}",)
        for table, spec in self.required_files.items():
            path = root / spec["filename"]
            if not path.is_file():
                errors.append(f"Missing required Olist file: {spec['filename']}")
                continue
            if check_headers:
                with path.open("r", encoding="utf-8-sig", newline="") as handle:
                    header = tuple(next(csv.reader(handle), ()))
                if header != EXPECTED_HEADERS[table]:
                    errors.append(
                        f"Unexpected schema for {spec['filename']}: expected "
                        f"{','.join(EXPECTED_HEADERS[table])}"
                    )
        return tuple(errors)


class OlistSourceAdapter:
    """Read-only file adapter implementing the existing SourceAdapter shape."""

    def __init__(self, root: Path, manifest: OlistManifest | None = None,
                 config: SourceConfig | None = None):
        self.root = Path(root).resolve()
        self.manifest = manifest or OlistManifest.load()
        self.config = config or SourceConfig(
            source_type="commerce_dataset", source_id=OLIST_SOURCE_ID, enabled=True,
            business_id=OLIST_BUSINESS_ID, ingestion_mode="file", schedule="0 0 * * *",
            schema_version="commerce_dataset_v1", credential_ref="OLIST_LOCAL_DATASET",
            metadata={"adapter": "olist_public", "root": str(self.root)},
        )

    def validate_config(self) -> tuple[str, ...]:
        return self.manifest.validate(self.root)

    def healthcheck(self) -> AdapterHealth:
        errors = self.validate_config()
        return AdapterHealth(healthy=not errors,
                             message="ready; local files only" if not errors else "; ".join(errors))

    def rows(self, table: str) -> Iterable[dict[str, str]]:
        spec = self.manifest.required_files[table]
        with (self.root / spec["filename"]).open("r", encoding="utf-8-sig", newline="") as handle:
            yield from csv.DictReader(handle)

    @staticmethod
    def _native_key(table: str, row: Mapping[str, str]) -> str:
        fields = {
            "orders": ("order_id",), "order_items": ("order_id", "order_item_id"),
            "payments": ("order_id", "payment_sequential"), "products": ("product_id",),
            "sellers": ("seller_id",), "customers": ("customer_id",),
            "category_translation": ("product_category_name",),
        }[table]
        return "|".join(row.get(name, "") for name in fields)

    def envelope(self, table: str, row: Mapping[str, str], extracted_at: datetime) -> IngestionEnvelope:
        key = self._native_key(table, row)
        identity = f"{self.config.business_id}|{self.config.source_id}|{table}|{key}"
        payload = {"source_file": self.manifest.required_files[table]["filename"],
                   "table": table, "record_key": key,
                   "source_observed_at_utc": extracted_at.isoformat().replace("+00:00", "Z"),
                   "native_record": dict(row)}
        contract_for(self.config.source_type, self.config.schema_version).validate(payload)
        return IngestionEnvelope(
            business_id=self.config.business_id, source_type=self.config.source_type,
            source_id=self.config.source_id,
            ingestion_id=str(uuid5(NAMESPACE_URL, f"olist-ingestion|{extracted_at.isoformat()}")),
            record_id=str(uuid5(NAMESPACE_URL, identity)), extracted_at_utc=extracted_at,
            source_updated_at_utc=None, schema_version=self.config.schema_version,
            payload=payload,
        )

    def extract(self) -> tuple[IngestionEnvelope, ...]:
        errors = self.validate_config()
        if errors:
            raise ValueError("; ".join(errors))
        extracted_at = datetime.now(timezone.utc)
        return tuple(self.envelope(table, row, extracted_at)
                     for table in self.manifest.required_files for row in self.rows(table))

    def normalize(self, record: IngestionEnvelope) -> dict[str, Any]:
        if (record.business_id, record.source_id) != (self.config.business_id, self.config.source_id):
            raise ValueError("Envelope identity does not match the Olist adapter")
        return dict(record.payload)


def _decimal(value: str, field: str, record_id: str) -> Decimal:
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} is not numeric for {record_id}") from None
    if not number.is_finite():
        raise ValueError(f"{field} is not finite for {record_id}")
    return number


def _timestamp(value: str, zone: ZoneInfo, field: str, record_id: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{field} is not a timestamp for {record_id}") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
    return parsed.astimezone(timezone.utc)


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value).__name__)


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=_json_default, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _event(order_id: str, source_type: str, status: OperationalStatus,
           event_at: datetime, native_status: str, details: Mapping[str, Any]) -> dict[str, Any]:
    external = f"{order_id}|{status.value.lower()}|{event_at.isoformat()}"
    event_id = str(uuid5(NAMESPACE_URL, f"{OLIST_BUSINESS_ID}|{OLIST_SOURCE_ID}|{external}"))
    model = OperationalEvent(
        business_id=OLIST_BUSINESS_ID, source_type=source_type, source_id=OLIST_SOURCE_ID,
        provider=PROVIDER, event_id=event_id, external_event_id=external, order_id=order_id,
        event_type=status.value.lower(), canonical_status=status, provider_status=native_status,
        event_at=event_at, received_at_utc=event_at, details=dict(details),
    )
    row = {name: getattr(model, name) for name in model.__dataclass_fields__}
    row.update({"canonical_status": status.value, "confirmation_outcome": None,
                "provider_reason_code": None, "courier": None, "tracking_reference": None,
                "attempt_number": None, "attempt_outcome": None,
                "details_json": json.dumps(row["details"], sort_keys=True, separators=(",", ":"))})
    return row


def _issue(code: str, severity: str, classification: str,
           identifiers: Sequence[str]) -> BenchmarkIssue:
    return BenchmarkIssue(code, severity, classification, len(identifiers), tuple(identifiers[:10]))


def _order_events(order: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
    details = {"native_status": order["native_status"],
               "snapshot_only_status": order["canonical_status"] is None}
    yield _event(order["order_id"], "commerce_orders", OperationalStatus.CREATED,
                 order["order_created_at"], order["native_status"], details)
    if order["order_approved_at"]:
        yield _event(order["order_id"], "confirmation_events", OperationalStatus.CONFIRMED,
                     order["order_approved_at"], order["native_status"], details)
    if order["carrier_handoff_at"]:
        yield _event(order["order_id"], "fulfillment_events", OperationalStatus.SHIPPED,
                     order["carrier_handoff_at"], order["native_status"], details)
    if order["delivered_at"]:
        yield _event(order["order_id"], "delivery_events", OperationalStatus.DELIVERED,
                     order["delivered_at"], order["native_status"], details)


def run_benchmark(*, fixture: bool = False, root: Path | None = None,
                  output_root: Path | None = None, write_outputs: bool = True,
                  enforce_acceptance: bool = False,
                  core_compatibility: bool | None = None) -> dict[str, Any]:
    """Run the deterministic offline Olist benchmark and return its report."""
    manifest = OlistManifest.load()
    selected_root = Path(root).resolve() if root else manifest.root(fixture=fixture)
    adapter = OlistSourceAdapter(selected_root, manifest)
    errors = adapter.validate_config()
    if errors:
        suffix = " Use --fixture for the committed CI dataset." if not fixture else ""
        raise ValueError("; ".join(errors) + suffix)
    output = (Path(output_root).resolve() if output_root else
              (PROJECT_ROOT / "data" / "benchmarks" / "olist" /
               ("fixture" if fixture else "full")).resolve())
    # The committed fixture exercises the Python canonical builders exhaustively.
    # A full in-memory event/economics projection duplicates several hundred
    # thousand dictionaries; it remains opt-in while the full default path
    # performs bounded benchmark aggregations over every source row.
    exercise_core = fixture if core_compatibility is None else core_compatibility
    started = time.perf_counter()
    extracted_at = datetime.now(timezone.utc)
    counts: dict[str, int] = {}
    stage_seconds: dict[str, float] = {}

    stage = time.perf_counter()
    tables: dict[str, list[dict[str, str]]] = {}
    for table in manifest.required_files:
        rows = list(adapter.rows(table))
        tables[table] = rows
        counts[table] = len(rows)
        if write_outputs:
            _write_jsonl(output / "bronze" / f"{table}.jsonl",
                         (adapter.envelope(table, row, extracted_at).to_dict() for row in rows))
    stage_seconds["bronze"] = time.perf_counter() - stage

    stage = time.perf_counter()
    translations = {r["product_category_name"]: r["product_category_name_english"]
                    for r in tables["category_translation"]}
    products = {r["product_id"]: r for r in tables["products"]}
    sellers = {r["seller_id"] for r in tables["sellers"]}
    customers = {r["customer_id"]: r["customer_unique_id"] for r in tables["customers"]}
    item_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    payment_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in tables["order_items"]:
        item_groups[row["order_id"]].append(row)
    for row in tables["payments"]:
        payment_groups[row["order_id"]].append(row)

    duplicate_ids: list[str] = []
    for table, grain in (("orders", ("order_id",)), ("order_items", ("order_id", "order_item_id")),
                         ("payments", ("order_id", "payment_sequential")),
                         ("products", ("product_id",)), ("sellers", ("seller_id",)),
                         ("customers", ("customer_id",))):
        keys = [tuple(row[name] for name in grain) for row in tables[table]]
        duplicate_ids.extend(f"{table}:{'|'.join(key)}" for key, number in Counter(keys).items() if number > 1)

    known_orders = {row["order_id"] for row in tables["orders"]}
    known_products, known_customers = set(products), set(customers)
    fk_missing = {
        "items_order": sorted({row["order_id"] for row in tables["order_items"] if row["order_id"] not in known_orders}),
        "payments_order": sorted({row["order_id"] for row in tables["payments"] if row["order_id"] not in known_orders}),
        "orders_customer": sorted({row["order_id"] for row in tables["orders"] if row["customer_id"] not in known_customers}),
        "items_product": sorted({row["order_id"] for row in tables["order_items"] if row["product_id"] not in known_products}),
        "items_seller": sorted({row["order_id"] for row in tables["order_items"] if row["seller_id"] not in sellers}),
    }

    zone = ZoneInfo(str(manifest.raw["source_timezone"])); currency = str(manifest.raw["currency"])
    order_rows: list[dict[str, Any]] = []
    line_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    product_rows: list[dict[str, Any]] = []
    customer_rows: list[dict[str, Any]] = []
    seller_rows = [{"business_id": OLIST_BUSINESS_ID, "source_id": OLIST_SOURCE_ID,
                    "seller_id": seller_id} for seller_id in sorted(sellers)]
    events: list[dict[str, Any]] = []; event_count = 0
    rejects: list[dict[str, str]] = []
    status_counts: Counter[str] = Counter()
    missing_lines_by_status: Counter[str] = Counter()
    payment_mismatch_by_status: Counter[str] = Counter()
    payment_combinations: Counter[str] = Counter()
    missing_timestamps_by_status: dict[str, Counter[str]] = defaultdict(Counter)
    missing_lines: list[str] = []; missing_payments: list[str] = []
    delivered_missing_actual: list[str] = []; carrier_before_approval: list[str] = []
    delivery_before_carrier: list[str] = []; delivery_before_purchase: list[str] = []
    late_deliveries: list[str] = []; negative_money: list[str] = []
    mismatch_001: list[str] = []; mismatch_100: list[str] = []
    mismatch_net_001 = Decimal(0); mismatch_absolute_001 = Decimal(0)
    both_items_payments = 0; delivery_days: list[float] = []

    for row in tables["products"]:
        native = row["product_category_name"] or None
        product_rows.append({"business_id": OLIST_BUSINESS_ID, "source_id": OLIST_SOURCE_ID,
                             "product_id": row["product_id"], "category_native": native,
                             "category_english": translations.get(native) if native else None})
    for row in tables["customers"]:
        customer_rows.append({"business_id": OLIST_BUSINESS_ID, "source_id": OLIST_SOURCE_ID,
                              "customer_id": row["customer_id"],
                              "customer_unique_reference": row["customer_unique_id"]})

    for raw in tables["orders"]:
        order_id = raw["order_id"]
        if not order_id:
            rejects.append({"table": "orders", "record_id": "", "reason": "missing order identity"})
            continue
        try:
            purchased = _timestamp(raw["order_purchase_timestamp"], zone, "order_purchase_timestamp", order_id)
            approved = _timestamp(raw["order_approved_at"], zone, "order_approved_at", order_id)
            carrier = _timestamp(raw["order_delivered_carrier_date"], zone, "order_delivered_carrier_date", order_id)
            delivered = _timestamp(raw["order_delivered_customer_date"], zone, "order_delivered_customer_date", order_id)
            estimated = _timestamp(raw["order_estimated_delivery_date"], zone, "order_estimated_delivery_date", order_id)
            if purchased is None or estimated is None:
                raise ValueError(f"required order timestamp is missing for {order_id}")
            item_rows = item_groups.get(order_id, [])
            pay_rows = payment_groups.get(order_id, [])
            merchandise = sum((_decimal(item["price"], "price", order_id) for item in item_rows), Decimal(0))
            freight = sum((_decimal(item["freight_value"], "freight_value", order_id) for item in item_rows), Decimal(0))
            payment_total = sum((_decimal(item["payment_value"], "payment_value", order_id) for item in pay_rows), Decimal(0))
        except ValueError as error:
            rejects.append({"table": "orders", "record_id": order_id, "reason": str(error)})
            continue
        if merchandise < 0 or freight < 0 or payment_total < 0:
            negative_money.append(order_id)
        calculated = merchandise + freight
        delta = payment_total - calculated if pay_rows and item_rows else None
        mismatch_over_001 = False
        if item_rows and pay_rows:
            both_items_payments += 1
            # ``fsum`` mirrors a stable double-precision warehouse aggregate;
            # the exact Decimal delta remains the value exposed in Silver.
            aggregate_delta = fsum(float(item["payment_value"]) for item in pay_rows) - fsum(
                float(item["price"]) + float(item["freight_value"]) for item in item_rows
            )
            if abs(aggregate_delta) > float(RECONCILIATION_TOLERANCE):
                mismatch_001.append(order_id); mismatch_over_001 = True
                mismatch_net_001 += delta
                mismatch_absolute_001 += abs(delta)
            if abs(delta) > Decimal("1.00"): mismatch_100.append(order_id)
        if not item_rows: missing_lines.append(order_id)
        if not pay_rows: missing_payments.append(order_id)
        native_status = raw["order_status"].strip().lower(); status_counts[native_status] += 1
        if not item_rows: missing_lines_by_status[native_status] += 1
        if mismatch_over_001: payment_mismatch_by_status[native_status] += 1
        for field, value in (("order_approved_at", approved),
                             ("order_delivered_carrier_date", carrier),
                             ("order_delivered_customer_date", delivered)):
            if value is None: missing_timestamps_by_status[native_status][field] += 1
        if native_status == "delivered" and delivered is None: delivered_missing_actual.append(order_id)
        if approved and carrier and carrier < approved: carrier_before_approval.append(order_id)
        if carrier and delivered and delivered < carrier: delivery_before_carrier.append(order_id)
        if delivered and delivered < purchased: delivery_before_purchase.append(order_id)
        if delivered and delivered > estimated: late_deliveries.append(order_id)
        if delivered:
            delivery_days.append((delivered - purchased).total_seconds() / 86400)
        payment_types = sorted({p["payment_type"] for p in pay_rows})
        if payment_types: payment_combinations[" + ".join(payment_types)] += 1
        payment_type = PaymentType.PREPAID if any(p != "not_defined" for p in payment_types) else PaymentType.OTHER
        updated = max((value for value in (purchased, approved, carrier, delivered) if value), default=purchased)
        raw_lines = []
        for item in item_rows:
            product = products.get(item["product_id"], {})
            line = {"business_id": OLIST_BUSINESS_ID, "order_id": order_id,
                    "line_id": item["order_item_id"], "product_id": item["product_id"] or None,
                    "variant_id": None, "sku": None, "quantity": 1,
                    "unit_price": float(_decimal(item["price"], "price", order_id)), "currency": currency,
                    "source_id": OLIST_SOURCE_ID, "seller_id": item["seller_id"],
                    "customer_freight_charge": float(_decimal(item["freight_value"], "freight_value", order_id)),
                    "category_native": product.get("product_category_name") or None,
                    "category_english": translations.get(product.get("product_category_name", ""))}
            OrderLine(**{name: line[name] for name in OrderLine.__dataclass_fields__})
            line_rows.append(line)
            raw_lines.append({name: line[name] for name in ("line_id", "product_id", "variant_id", "sku",
                                                             "quantity", "unit_price")})
        order = {"business_id": OLIST_BUSINESS_ID, "source_type": "commerce_orders",
                 "source_id": OLIST_SOURCE_ID, "provider": PROVIDER, "order_id": order_id,
                 "external_order_id": order_id, "order_created_at": purchased,
                 "order_updated_at": updated, "source_timezone": str(zone),
                 "reporting_timezone": str(zone), "currency": currency,
                 "order_value": float(calculated), "payment_type": payment_type.value,
                 "customer_reference": raw["customer_id"], "customer_unique_reference": customers.get(raw["customer_id"]),
                 "native_status": native_status, "canonical_status": SAFE_STATUS_MAP.get(native_status),
                 "order_approved_at": approved, "carrier_handoff_at": carrier,
                 "delivered_at": delivered, "estimated_delivery_at": estimated,
                 "merchandise_value": float(merchandise), "customer_freight_charge": float(freight),
                 "commerce_calculated_total": float(calculated),
                 "payment_total": float(payment_total) if pay_rows else None,
                 "payment_reconciliation_delta": float(delta) if delta is not None else None,
                 "item_count": len(item_rows), "seller_count": len({x["seller_id"] for x in item_rows}),
                 "product_count": len({x["product_id"] for x in item_rows}),
                 "payment_row_count": len(pay_rows), "payment_methods": payment_types}
        CommerceOrder(**{name: order[name] for name in CommerceOrder.__dataclass_fields__ if name != "lines"},
                      lines=tuple(OrderLine(**{name: line[name] for name in OrderLine.__dataclass_fields__})
                                  for line in line_rows[-len(item_rows):]) if item_rows else ())
        order_rows.append(order)
        milestone_count = 1 + sum(value is not None for value in (approved, carrier, delivered))
        event_count += milestone_count
        if exercise_core:
            events.extend(_order_events(order))
        for payment in pay_rows:
            payment_rows.append({"business_id": OLIST_BUSINESS_ID, "source_id": OLIST_SOURCE_ID,
                                 "order_id": order_id, "payment_sequential": int(payment["payment_sequential"]),
                                 "payment_method": payment["payment_type"],
                                 "installments": int(payment["payment_installments"]),
                                 "payment_amount": float(_decimal(payment["payment_value"], "payment_value", order_id)),
                                 "currency": currency})

    operations_silver = {
        "commerce_orders": tuple({name: row[name] for name in (
            "business_id", "source_type", "source_id", "provider", "order_id", "external_order_id",
            "order_created_at", "order_updated_at", "source_timezone", "reporting_timezone", "currency",
            "order_value", "payment_type", "customer_reference")} for row in order_rows),
        "order_lines": tuple({name: row[name] for name in OrderLine.__dataclass_fields__} for row in line_rows),
        "operational_events": tuple(events), "shipments": (), "cash_collections": (), "remittances": (),
    }
    if exercise_core:
        operations_gold = build_operations_gold(operations_silver)
        economics_gold = build_economics_gold(
            operations_silver, operations_gold,
            {"product_costs": (), "cost_components": (), "attribution_links": ()}, {},
            as_of_date=date.today(),
        )
        economic_statuses = dict(Counter(
            row["economic_status"] for row in economics_gold["order_economics"]
        ))
    else:
        operations_gold = {}
        economics_gold = {}
        economic_statuses = {"INCOMPLETE_COSTS": len(order_rows)}
    stage_seconds["silver"] = time.perf_counter() - stage

    stage = time.perf_counter()
    payment_method_groups: dict[str, dict[str, float | int]] = defaultdict(lambda: {"payment_rows": 0, "payment_amount": 0.0})
    for payment in payment_rows:
        group = payment_method_groups[payment["payment_method"]]
        group["payment_rows"] += 1; group["payment_amount"] += payment["payment_amount"]
    commerce_daily: dict[str, dict[str, float | int]] = defaultdict(lambda: defaultdict(float))
    for order in order_rows:
        key = order["order_created_at"].astimezone(zone).date().isoformat()
        daily = commerce_daily[key]
        daily["orders"] += 1; daily["merchandise_value"] += order["merchandise_value"]
        daily["customer_freight_charge"] += order["customer_freight_charge"]
        daily["commerce_calculated_total"] += order["commerce_calculated_total"]
        daily["payment_total"] += order["payment_total"] or 0.0
    gold = {
        "orders_by_status": [{"business_id": OLIST_BUSINESS_ID, "native_status": key, "order_count": value}
                             for key, value in sorted(status_counts.items())],
        "commerce_daily": [{"business_id": OLIST_BUSINESS_ID, "event_date": key, "currency": currency, **value}
                           for key, value in sorted(commerce_daily.items())],
        "payment_methods": [{"business_id": OLIST_BUSINESS_ID, "payment_method": key, "currency": currency, **value}
                            for key, value in sorted(payment_method_groups.items())],
    }
    issues = [
        _issue("duplicate_identity", "CRITICAL", "DATA_DEFECT", duplicate_ids),
        *(_issue(f"referential_integrity_{key}", "CRITICAL", "DATA_DEFECT", value)
          for key, value in fk_missing.items()),
        _issue("negative_monetary_amount", "CRITICAL", "DATA_DEFECT", negative_money),
        _issue("delivery_before_purchase", "CRITICAL", "DATA_DEFECT", delivery_before_purchase),
        _issue("delivery_before_carrier", "WARNING", "DATA_DEFECT", delivery_before_carrier),
        _issue("carrier_before_approval", "WARNING", "DATA_DEFECT", carrier_before_approval),
        _issue("late_delivery", "INFO", "BUSINESS_OUTCOME", late_deliveries),
        _issue("delivered_missing_actual", "WARNING", "COMPLETENESS", delivered_missing_actual),
        _issue("product_missing_category", "INFO", "COMPLETENESS",
               [r["product_id"] for r in product_rows if r["category_native"] is None]),
        _issue("order_missing_lines", "INFO", "COMPLETENESS", missing_lines),
        _issue("order_missing_payment", "INFO", "COMPLETENESS", missing_payments),
        _issue("payment_reconciliation_over_0_01", "WARNING", "RECONCILIATION", mismatch_001),
        _issue("payment_reconciliation_over_1_00", "WARNING", "RECONCILIATION", mismatch_100),
    ]
    operation_issues = check_operations(operations_silver) if exercise_core else ()
    if operation_issues:
        by_code: dict[tuple[str, str], list[str]] = defaultdict(list)
        for item in operation_issues:
            by_code[(item.code, item.severity)].append(item.order_id or "")
        issues.extend(_issue(code, severity, "DATA_DEFECT", ids)
                      for (code, severity), ids in sorted(by_code.items()))

    metrics: dict[str, Any] = {
        "status_counts": dict(sorted(status_counts.items())),
        "orders_with_multiple_items": sum(len(v) > 1 for v in item_groups.values()),
        "orders_with_multiple_sellers": sum(len({x["seller_id"] for x in v}) > 1 for v in item_groups.values()),
        "orders_with_multiple_products": sum(len({x["product_id"] for x in v}) > 1 for v in item_groups.values()),
        "orders_with_multiple_payments": sum(len(v) > 1 for v in payment_groups.values()),
        "max_items_per_order": max(map(len, item_groups.values()), default=0),
        "max_payments_per_order": max(map(len, payment_groups.values()), default=0),
        "distinct_customer_unique_references": len(set(customers.values())),
        "orders_with_items": len(item_groups), "orders_with_payments": len(payment_groups),
        "orders_without_items_by_status": dict(sorted(missing_lines_by_status.items())),
        "payment_combinations": dict(payment_combinations.most_common()),
        "payment_mismatch_over_0_01_by_status": dict(sorted(payment_mismatch_by_status.items())),
        "missing_timestamps_by_status": {
            status: dict(sorted(values.items()))
            for status, values in sorted(missing_timestamps_by_status.items())
        },
        "orders_with_items_and_payments": both_items_payments,
        "late_deliveries": len(late_deliveries), "carrier_before_approval": len(carrier_before_approval),
        "delivery_before_carrier": len(delivery_before_carrier),
        "products_missing_category": sum(r["category_native"] is None for r in product_rows),
        "payment_mismatch_over_0_01": len(mismatch_001), "payment_mismatch_over_1_00": len(mismatch_100),
        "payment_mismatch_over_0_01_net_delta": float(mismatch_net_001),
        "payment_mismatch_over_0_01_absolute_delta": float(mismatch_absolute_001),
        "merchandise_value_total": sum(row["merchandise_value"] for row in order_rows),
        "customer_freight_charge_total": sum(row["customer_freight_charge"] for row in order_rows),
        "commerce_calculated_total": sum(row["commerce_calculated_total"] for row in order_rows),
        "payment_total": sum(row["payment_total"] or 0.0 for row in order_rows),
        "average_delivery_days": mean(delivery_days) if delivery_days else None,
        "median_delivery_days": median(delivery_days) if delivery_days else None,
        "p95_delivery_days": _percentile(delivery_days, .95),
    }
    expected = manifest.raw["acceptance"]
    acceptance: dict[str, dict[str, Any]] = {}
    if not fixture:
        for table, spec in manifest.required_files.items():
            acceptance[f"rows.{table}"] = {"expected": spec["expected_rows"], "actual": counts[table],
                                            "passed": counts[table] == spec["expected_rows"]}
        for name, expected_value in expected.items():
            actual = metrics[name]
            if name == "status_counts":
                passed = all(actual.get(status) == count for status, count in expected_value.items())
            else:
                passed = actual == expected_value
            acceptance[name] = {"expected": expected_value, "actual": actual, "passed": passed}
    if enforce_acceptance and any(not value["passed"] for value in acceptance.values()):
        failures = [name for name, value in acceptance.items() if not value["passed"]]
        raise ValueError("Olist full-data acceptance failed: " + ", ".join(failures))

    if write_outputs:
        for name, rows in (("commerce_orders", order_rows), ("order_lines", line_rows),
                           ("payments", payment_rows), ("products", product_rows),
                           ("customers", customer_rows), ("sellers", seller_rows)):
            _write_jsonl(output / "silver" / f"{name}.jsonl", rows)
        _write_jsonl(output / "silver" / "operational_events.jsonl",
                     events if exercise_core else
                     (event for order in order_rows for event in _order_events(order)))
        gold["economic_completeness"] = [{
            "business_id": OLIST_BUSINESS_ID, "currency": currency,
            "order_count": len(order_rows), "economic_status": "INCOMPLETE_COSTS",
            "cogs_available": False, "attribution_available": False,
            "profit_calculated": False,
        }]
        for name, rows in {**gold, **operations_gold, **economics_gold}.items():
            _write_jsonl(output / "gold" / f"{name}.jsonl", rows)
        _write_jsonl(output / "gold" / "data_quality.jsonl", (asdict(item) for item in issues))
    stage_seconds["gold_and_quality"] = time.perf_counter() - stage
    stage_seconds["total"] = time.perf_counter() - started
    daily_points = sorted((date.fromisoformat(day), int(values["orders"]))
                          for day, values in commerce_daily.items())
    if daily_points:
        current_day, current_value = daily_points[-1]
        history_points = daily_points[:-1]
        series = MetricSeries(
            metric_name="completed_order_volume", dataset_name="olist_commerce_daily",
            layer="gold", current_value=current_value,
            history=tuple(value for _, value in history_points),
            observed_at_utc=datetime.combine(current_day, datetime.min.time(), tzinfo=timezone.utc),
            history_observed_at_utc=tuple(
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
                for day, _ in history_points
            ),
            dimensions={"business_id": OLIST_BUSINESS_ID, "currency": currency},
        )
        anomaly_result = evaluate(
            series, policy_for(series.metric_name, 7),
            uuid5(NAMESPACE_URL, f"olist-anomaly|{current_day.isoformat()}|completed_order_volume"),
        )
        anomaly = {"status": anomaly_result.status.value,
                   "severity": anomaly_result.severity.value,
                   "baseline_strategy": anomaly_result.baseline_strategy,
                   "history_count": anomaly_result.history_count,
                   "current_value": anomaly_result.current_value,
                   "baseline_value": anomaly_result.baseline_value,
                   "thresholds_unchanged": True}
    else:
        anomaly = None
    report = {
        "benchmark_id": manifest.raw["benchmark_id"], "business_id": OLIST_BUSINESS_ID,
        "mode": "fixture" if fixture else "full",
        "source_root": str(manifest.raw["fixture_path" if fixture else "expected_local_path"]),
        "network_access": False, "currency": currency,
        "input_rows": counts, "bronze_rows": sum(counts.values()),
        "silver_rows": {"commerce_orders": len(order_rows), "order_lines": len(line_rows),
                        "payments": len(payment_rows), "products": len(product_rows),
                        "customers": len(customer_rows), "sellers": len(seller_rows),
                        "operational_events": event_count},
        "rejected_rows": len(rejects), "metrics": metrics,
        "quality": [asdict(item) for item in issues],
        "economics": {"order_rows": len(order_rows),
                      "statuses": economic_statuses,
                      "profit_calculated": False, "cogs_available": False,
                      "attribution_available": False, "cod_fabricated": False,
                      "remittance_fabricated": False},
        "operations": {"core_compatibility_exercised": exercise_core,
                       **{name: len(rows) for name, rows in operations_gold.items()}},
        "anomaly": anomaly,
        "acceptance": acceptance, "timings_seconds": {k: round(v, 6) for k, v in stage_seconds.items()},
        "output_root": (str(output.relative_to(PROJECT_ROOT))
                        if write_outputs and output.is_relative_to(PROJECT_ROOT)
                        else str(output) if write_outputs else None),
    }
    if write_outputs:
        _write_jsonl(output / "report.jsonl", (report,))
        if rejects: _write_jsonl(output / "silver" / "rejected.jsonl", rejects)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the offline Olist public-data benchmark")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("validate", "profile", "dry-run", "benchmark"):
        command = sub.add_parser(name)
        command.add_argument("--fixture", action="store_true")
        command.add_argument("--root", type=Path)
    sub.choices["benchmark"].add_argument("--enforce-acceptance", action="store_true")
    sub.choices["benchmark"].add_argument("--core-compatibility", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = OlistManifest.load()
    root = args.root.resolve() if args.root else manifest.root(fixture=args.fixture)
    try:
        if args.command == "validate":
            errors = manifest.validate(root)
            payload = {"valid": not errors, "mode": "fixture" if args.fixture else "full",
                       "required_files": [spec["filename"] for spec in manifest.required_files.values()],
                       "deferred_files": [spec["filename"] for spec in manifest.raw["deferred_files"].values()],
                       "errors": errors}
            success = not errors
        elif args.command == "profile":
            adapter = OlistSourceAdapter(root, manifest)
            errors = adapter.validate_config()
            if errors: raise ValueError("; ".join(errors))
            payload = {"mode": "fixture" if args.fixture else "full",
                       "input_rows": {table: sum(1 for _ in adapter.rows(table))
                                      for table in manifest.required_files},
                       "manifest": manifest.raw}
            success = True
        else:
            payload = run_benchmark(fixture=args.fixture, root=root,
                                    write_outputs=args.command == "benchmark",
                                    enforce_acceptance=getattr(args, "enforce_acceptance", False),
                                    core_compatibility=(True if getattr(args, "core_compatibility", False) else None))
            success = True
    except (OSError, ValueError) as error:
        payload, success = {"error": str(error), "network_access": False}, False
    print(json.dumps(payload, default=_json_default, sort_keys=True, indent=2))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
