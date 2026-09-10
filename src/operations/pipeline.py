"""Deterministic Bronze/Silver/Gold commerce-operations pipeline."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterable
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                               StructField, StructType, TimestampType)

from src.onboarding.models import IngestionEnvelope
from src.onboarding.registry import BusinessRegistry, validate_business
from src.operations.adapters import (OPERATIONS_SOURCE_TYPES, STATUS_MAPPINGS,
                                     operations_adapter_for)
from src.operations.models import (CashCollection, CommerceOrder, OperationalEvent,
                                   OperationalStatus, OrderLine, PaymentType, Remittance,
                                   SettlementStatus, Shipment, event_latest_revisions,
                                   operational_kpis)
from src.streaming.windows_spark import (configure_windows_spark_builder,
                                         configure_windows_spark_environment)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_TABLES = ("commerce_orders", "order_lines", "operational_events", "shipments",
                 "cash_collections", "remittances")
OPERATIONS_GOLD_TABLES = ("order_operations_current", "order_operations_daily",
                          "confirmation_performance", "delivery_performance",
                          "cod_collection_performance", "remittance_performance")


@dataclass(frozen=True, slots=True)
class OperationsPaths:
    bronze: Path
    commerce_orders: Path
    order_lines: Path
    operational_events: Path
    shipments: Path
    cash_collections: Path
    remittances: Path
    order_operations_current: Path
    order_operations_daily: Path
    confirmation_performance: Path
    delivery_performance: Path
    cod_collection_performance: Path
    remittance_performance: Path


def _path(value: str, root: Path) -> Path:
    candidate = Path(value)
    return candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()


def load_operations_paths(environ=None, *, project_root: Path = PROJECT_ROOT) -> OperationsPaths:
    env = os.environ if environ is None else environ
    names = {
        "bronze": ("BRONZE_OPERATIONS_PATH", "data/bronze/commerce_operations"),
        **{name: (f"SILVER_{name.upper()}_PATH", f"data/silver/{name}") for name in SILVER_TABLES},
        **{name: (f"GOLD_{name.upper()}_PATH", f"data/gold/{name}") for name in OPERATIONS_GOLD_TABLES},
    }
    return OperationsPaths(**{name: _path(env.get(variable, default), project_root)
                              for name, (variable, default) in names.items()})


def build_operations_spark_session(*, app_name="pulse-operations", master="local[*]") -> SparkSession:
    configure_windows_spark_environment()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName(app_name).master(master)
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", os.getenv("OPERATIONS_SHUFFLE_PARTITIONS", "4")))
    return configure_windows_spark_builder(builder).getOrCreate()


BRONZE_SCHEMA = StructType([
    StructField("business_id", StringType(), False), StructField("source_type", StringType(), False),
    StructField("source_id", StringType(), False), StructField("ingestion_id", StringType(), False),
    StructField("record_id", StringType(), False), StructField("extracted_at_utc", TimestampType(), False),
    StructField("source_updated_at_utc", TimestampType(), True), StructField("schema_version", StringType(), False),
    StructField("payload", StringType(), False),
])


def _schema(*columns):
    types = {"str": StringType(), "int": LongType(), "float": DoubleType(),
             "date": DateType(), "time": TimestampType()}
    return StructType([StructField(name, types[kind], nullable) for name, kind, nullable in columns])


SILVER_SCHEMAS = {
    "commerce_orders": _schema(
        ("business_id","str",False),("source_type","str",False),("source_id","str",False),
        ("provider","str",False),("order_id","str",False),("external_order_id","str",True),
        ("order_created_at","time",False),("order_updated_at","time",False),
        ("source_timezone","str",False),("reporting_timezone","str",False),("currency","str",False),
        ("order_value","float",False),("payment_type","str",False),("customer_reference","str",True)),
    "order_lines": _schema(
        ("business_id","str",False),("order_id","str",False),("line_id","str",False),
        ("product_id","str",True),("variant_id","str",True),("sku","str",True),
        ("quantity","int",False),("unit_price","float",False),("currency","str",False)),
    "operational_events": _schema(
        ("business_id","str",False),("source_type","str",False),("source_id","str",False),
        ("provider","str",False),("event_id","str",False),("external_event_id","str",False),
        ("order_id","str",False),("shipment_id","str",True),("event_type","str",False),
        ("canonical_status","str",False),("provider_status","str",False),
        ("event_at","time",False),("received_at_utc","time",False),("revision","int",False),
        ("corrects_event_id","str",True),("confirmation_outcome","str",True),
        ("provider_reason_code","str",True),("courier","str",True),("tracking_reference","str",True),
        ("attempt_number","int",True),("attempt_outcome","str",True),("details_json","str",False)),
    "shipments": _schema(
        ("business_id","str",False),("source_id","str",False),("provider","str",False),
        ("order_id","str",False),("fulfillment_id","str",False),("shipment_id","str",False),
        ("courier","str",True),("tracking_reference","str",True),("shipment_created_at","time",True),
        ("shipped_at","time",True),("delivered_at","time",True),("return_at","time",True),
        ("line_quantities_json","str",False)),
    "cash_collections": _schema(
        ("business_id","str",False),("source_id","str",False),("provider","str",False),
        ("collection_id","str",False),("order_id","str",False),("shipment_id","str",True),
        ("cash_expected","float",False),("cash_collected","float",False),
        ("collection_currency","str",False),("collected_at","time",True),("remittance_id","str",True)),
    "remittances": _schema(
        ("business_id","str",False),("source_id","str",False),("provider","str",False),
        ("remittance_id","str",False),("currency","str",False),("period_start","date",False),
        ("period_end","date",False),("gross_collected","float",False),("provider_fees","float",False),
        ("shipping_fees","float",False),("cod_fees","float",False),("adjustments","float",False),
        ("net_remitted","float",False),("remitted_at","time",True),("settlement_status","str",False),
        ("order_links_json","str",False)),
}

GOLD_SCHEMAS = {
    "order_operations_current": _schema(
        ("business_id","str",False),("order_id","str",False),("order_source_id","str",False),
        ("order_provider","str",False),("currency","str",False),("payment_type","str",False),
        ("order_value","float",False),("order_created_at","time",False),("reporting_timezone","str",False),
        ("current_operational_status","str",False),("current_provider","str",False),
        ("last_event_at","time",False),("confirmation_status","str",True),
        ("fulfillment_status","str",True),("delivery_status","str",True),
        ("collection_status","str",True),("remittance_status","str",True),
        ("confirmed_at","time",True),("shipped_at","time",True),("delivered_at","time",True),
        ("delivery_attempts","int",False),("shipment_count","int",False),
        ("cash_expected","float",False),("cash_collected","float",False)),
    "order_operations_daily": _schema(
        ("business_id","str",False),("event_date","date",False),("reporting_timezone","str",False),
        ("currency","str",False),("payment_type","str",False),("orders_created","int",False),
        ("confirmed_orders","int",False),("shipped_orders","int",False),("delivered_orders","int",False),
        ("refused_orders","int",False),("returned_orders","int",False),("unreachable_orders","int",False),
        ("delivery_attempts","int",False),("cash_collected","float",False)),
    "confirmation_performance": _schema(
        ("business_id","str",False),("cohort_date","date",False),("provider","str",False),
        ("currency","str",False),("payment_type","str",False),("eligible_orders","int",False),
        ("confirmed_orders","int",False),("rejected_orders","int",False),("unreachable_orders","int",False),
        ("confirmation_rate","float",True),("unreachable_rate","float",True),("average_time_to_confirm_hours","float",True)),
    "delivery_performance": _schema(
        ("business_id","str",False),("cohort_date","date",False),("courier","str",False),
        ("currency","str",False),("payment_type","str",False),("shipped_orders","int",False),
        ("delivered_orders","int",False),("refused_orders","int",False),("returned_orders","int",False),
        ("delivery_attempts","int",False),("delivery_rate","float",True),("refusal_rate","float",True),
        ("return_rate","float",True),("average_delivery_attempts","float",True),
        ("average_time_to_ship_hours","float",True),("average_time_to_deliver_hours","float",True)),
    "cod_collection_performance": _schema(
        ("business_id","str",False),("collection_date","date",False),("provider","str",False),
        ("currency","str",False),("collection_count","int",False),("cash_expected","float",False),
        ("cash_collected","float",False),("cash_collection_rate","float",True)),
    "remittance_performance": _schema(
        ("business_id","str",False),("period_end","date",False),("provider","str",False),
        ("currency","str",False),("settlement_status","str",False),("remittance_count","int",False),
        ("gross_collected","float",False),("provider_fees","float",False),("shipping_fees","float",False),
        ("cod_fees","float",False),("adjustments","float",False),("net_remitted","float",False),
        ("remittance_pending_amount","float",False)),
}


def extract_registered_operations(registry: BusinessRegistry | None = None, *,
                                  business_id: str | None = None, source_id: str | None = None):
    selected = registry or BusinessRegistry()
    owners = [business_id] if business_id else [b.business_id for b in selected.list_businesses()
                                                 if b.status == "ACTIVE"]
    output = []
    for owner in owners:
        report = validate_business(selected, owner)
        if not report.valid:
            raise ValueError(f"Invalid onboarding for {owner}")
        enabled = set(selected.get_business(owner).enabled_sources)
        for config in selected.sources_for(owner):
            if config.enabled and config.source_id in enabled and config.source_type in OPERATIONS_SOURCE_TYPES:
                if source_id is not None and config.source_id != source_id:
                    continue
                adapter = operations_adapter_for(config)
                output.append((adapter, adapter.extract()))
    if source_id is not None and not output:
        raise ValueError(f"Enabled operations source {source_id!r} was not found")
    return tuple(output)


def _event_object(row: dict[str, Any]) -> OperationalEvent:
    return OperationalEvent(**{**row, "canonical_status": OperationalStatus(row["canonical_status"])})


def normalize_operations(extractions, registry: BusinessRegistry | None = None):
    """Canonicalize, preserve revisions, and deduplicate exact provider retries."""
    selected = registry or BusinessRegistry()
    reporting = {business.business_id: business.reporting_timezone for business in selected.list_businesses()}
    orders, lines, events, shipments, collections, remittances = {}, {}, {}, {}, {}, {}
    for adapter, envelopes in extractions:
        for envelope in envelopes:
            item = adapter.normalize(envelope)
            event = item.get("event")
            event_extras = {"confirmation_outcome": None, "provider_reason_code": None,
                            "courier": None, "tracking_reference": None,
                            "attempt_number": None, "attempt_outcome": None}
            if item["entity_type"] == "commerce_order":
                order = dict(item["order"])
                raw_lines = order.pop("lines")
                order["reporting_timezone"] = reporting[order["business_id"]]
                validated_lines = tuple(OrderLine(**{
                    "business_id": order["business_id"], "order_id": order["order_id"],
                    "product_id": None, "variant_id": None, "sku": None,
                    "currency": order["currency"], **line})
                    for line in raw_lines)
                CommerceOrder(**{**order, "payment_type": PaymentType(order["payment_type"]),
                                 "lines": validated_lines})
                key = (order["business_id"], order["source_id"], order["provider"], order["order_id"])
                if key not in orders or order["order_updated_at"] >= orders[key]["order_updated_at"]:
                    orders[key] = order
                    for line in raw_lines:
                        line_row = {"business_id": order["business_id"], "order_id": order["order_id"],
                                    "product_id": None, "variant_id": None, "sku": None, **line,
                                    "currency": order["currency"]}
                        lines[(order["business_id"], order["order_id"], line["line_id"])] = line_row
            elif item["entity_type"] == "confirmation":
                event_extras.update(item["confirmation"])
                event_extras["confirmation_outcome"] = event_extras.pop("outcome")
            elif item["entity_type"] == "shipment":
                shipment = dict(item["shipment"])
                event_extras.update(courier=shipment.get("courier"),
                                    tracking_reference=shipment.get("tracking_reference"))
                if shipment["shipment_id"]:
                    Shipment(**shipment)
                    shipment["source_id"] = adapter.config.source_id
                    key = (shipment["business_id"], shipment["source_id"], shipment["provider"],
                           shipment["fulfillment_id"], shipment["shipment_id"])
                    shipments[key] = shipment
            elif item["entity_type"] == "delivery":
                event_extras.update(courier=item["courier"], tracking_reference=item["tracking_reference"])
                if item["delivery_attempt"]:
                    event_extras.update(item["delivery_attempt"])
            elif item["entity_type"] == "cash_collection":
                collection = dict(item["collection"])
                CashCollection(**collection)
                key = (collection["business_id"], collection["source_id"], collection["provider"],
                       collection["collection_id"])
                collections[key] = collection
            elif item["entity_type"] == "remittance":
                remittance = dict(item["remittance"])
                Remittance(**{name: remittance[name] for name in (
                    "business_id", "source_id", "provider", "remittance_id", "currency",
                    "gross_collected", "provider_fees", "shipping_fees", "cod_fees", "adjustments",
                    "net_remitted", "remitted_at")} ,
                    period_start=date.fromisoformat(remittance["period_start"]),
                    period_end=date.fromisoformat(remittance["period_end"]),
                    settlement_status=SettlementStatus(remittance["settlement_status"]),
                    order_links=remittance["order_links"])
                key = (remittance["business_id"], remittance["source_id"], remittance["provider"],
                       remittance["remittance_id"])
                existing = remittances.get(key)
                if existing is None or remittance["revision"] >= existing["revision"]:
                    remittances[key] = remittance
                status = STATUS_MAPPINGS["remittances"][remittance["provider_status"]]
                for link in remittance["order_links"]:
                    external = remittance["external_event_id"] + "|" + link["order_id"]
                    event_id = str(uuid5(NAMESPACE_URL, "|".join(("operations-event", remittance["business_id"],
                        remittance["source_id"], remittance["provider"], external, str(remittance["revision"])))))
                    generated = OperationalEvent(
                        business_id=remittance["business_id"], source_type="remittances",
                        source_id=remittance["source_id"], provider=remittance["provider"], event_id=event_id,
                        external_event_id=external, order_id=link["order_id"], shipment_id=link.get("shipment_id"),
                        event_type="remittance", canonical_status=status,
                        provider_status=remittance["provider_status"], event_at=remittance["event_at"],
                        received_at_utc=remittance["received_at_utc"], revision=remittance["revision"],
                        details={"remittance_id": remittance["remittance_id"],
                                 "collection_id": link.get("collection_id")})
                    events[generated.event_id] = {**generated.__dict__} if hasattr(generated, "__dict__") else {
                        name: getattr(generated, name) for name in generated.__dataclass_fields__}
            if event:
                canonical = _event_object(event)
                events[canonical.event_id] = {name: getattr(canonical, name)
                    for name in canonical.__dataclass_fields__} | event_extras

    event_rows = []
    for row in events.values():
        event_rows.append({**row, "canonical_status": OperationalStatus(row["canonical_status"]).value,
                           "details_json": json.dumps(row.pop("details"), sort_keys=True, separators=(",", ":"))})
    remittance_rows = []
    for row in remittances.values():
        cleaned = {k: v for k, v in row.items() if k not in {
            "event_at", "received_at_utc", "external_event_id", "provider_status", "revision"}}
        cleaned["period_start"] = date.fromisoformat(cleaned["period_start"])
        cleaned["period_end"] = date.fromisoformat(cleaned["period_end"])
        cleaned["order_links_json"] = json.dumps(cleaned.pop("order_links"), sort_keys=True, separators=(",", ":"))
        remittance_rows.append(cleaned)
    shipment_rows = [{**row, "line_quantities_json": json.dumps(row.pop("line_quantities"), sort_keys=True)}
                     for row in shipments.values()]
    return {
        "commerce_orders": tuple(sorted(orders.values(), key=lambda x: (x["business_id"], x["order_id"]))),
        "order_lines": tuple(sorted(lines.values(), key=lambda x: (x["business_id"], x["order_id"], x["line_id"]))),
        "operational_events": tuple(sorted(event_rows, key=lambda x: (x["business_id"], x["event_at"], x["event_id"]))),
        "shipments": tuple(sorted(shipment_rows, key=lambda x: (x["business_id"], x["shipment_id"]))),
        "cash_collections": tuple(sorted(collections.values(), key=lambda x: (x["business_id"], x["collection_id"]))),
        "remittances": tuple(sorted(remittance_rows, key=lambda x: (x["business_id"], x["remittance_id"]))),
    }


def _hours(start, end):
    return None if start is None or end is None else (end - start).total_seconds() / 3600.0


def _average(values):
    values = [value for value in values if value is not None]
    return None if not values else sum(values) / len(values)


def build_gold_rows(silver):
    orders = silver["commerce_orders"]
    raw_events = silver["operational_events"]
    objects = [_event_object({name: row[name] for name in OperationalEvent.__dataclass_fields__})
               for row in raw_events]
    effective = event_latest_revisions(objects)
    by_order = defaultdict(list)
    for event in effective:
        by_order[(event.business_id, event.order_id)].append(event)
    shipments = defaultdict(list)
    for row in silver["shipments"]:
        shipments[(row["business_id"], row["order_id"])].append(row)
    collections = defaultdict(list)
    for row in silver["cash_collections"]:
        collections[(row["business_id"], row["order_id"])].append(row)
    current = []
    for order in orders:
        key = (order["business_id"], order["order_id"])
        history = sorted(by_order[key], key=lambda event: event.ordering_key)
        latest = history[-1]
        def last_for(source_type):
            values = [event for event in history if event.source_type == source_type]
            return values[-1] if values else None
        confirmation, fulfillment, delivery = (last_for(name) for name in (
            "confirmation_events", "fulfillment_events", "delivery_events"))
        collection, remittance = last_for("cod_collections"), last_for("remittances")
        confirmed_at = next((e.event_at for e in history if e.canonical_status == OperationalStatus.CONFIRMED), None)
        shipped_at = next((e.event_at for e in history if e.canonical_status == OperationalStatus.SHIPPED), None)
        delivered_at = next((e.event_at for e in history if e.canonical_status == OperationalStatus.DELIVERED), None)
        current.append({
            "business_id": order["business_id"], "order_id": order["order_id"],
            "order_source_id": order["source_id"], "order_provider": order["provider"],
            "currency": order["currency"], "payment_type": order["payment_type"],
            "order_value": order["order_value"], "order_created_at": order["order_created_at"],
            "reporting_timezone": order["reporting_timezone"],
            "current_operational_status": latest.canonical_status.value, "current_provider": latest.provider,
            "last_event_at": latest.event_at,
            "confirmation_status": confirmation.canonical_status.value if confirmation else None,
            "fulfillment_status": fulfillment.canonical_status.value if fulfillment else None,
            "delivery_status": delivery.canonical_status.value if delivery else None,
            "collection_status": collection.canonical_status.value if collection else None,
            "remittance_status": remittance.canonical_status.value if remittance else None,
            "confirmed_at": confirmed_at, "shipped_at": shipped_at, "delivered_at": delivered_at,
            "delivery_attempts": sum(e.source_type == "delivery_events" and
                                     any(r["event_id"] == e.event_id and r.get("attempt_number") is not None
                                         for r in raw_events) for e in history),
            "shipment_count": len({row["shipment_id"] for row in shipments[key]}),
            "cash_expected": sum(row["cash_expected"] for row in collections[key]),
            "cash_collected": sum(row["cash_collected"] for row in collections[key]),
        })

    daily_groups = defaultdict(list)
    order_lookup = {(row["business_id"], row["order_id"]): row for row in orders}
    for event in effective:
        order = order_lookup.get((event.business_id, event.order_id))
        if order:
            business_day = event.event_at.astimezone(ZoneInfo(order["reporting_timezone"])).date()
            daily_groups[(event.business_id, business_day, order["reporting_timezone"],
                          order["currency"], order["payment_type"])].append(event)
    daily = []
    for key, history in sorted(daily_groups.items()):
        statuses = defaultdict(set)
        attempts = 0
        for event in history:
            statuses[event.canonical_status].add(event.order_id)
            attempts += any(row["event_id"] == event.event_id and row.get("attempt_number") is not None for row in raw_events)
        cash = sum(row["cash_collected"] for row in silver["cash_collections"]
                   if row["business_id"] == key[0] and row["collected_at"] and row["collected_at"].date() == key[1]
                   and row["collection_currency"] == key[3])
        daily.append(dict(zip(("business_id","event_date","reporting_timezone","currency","payment_type"), key)) | {
            "orders_created": len(statuses[OperationalStatus.CREATED]),
            "confirmed_orders": len(statuses[OperationalStatus.CONFIRMED]),
            "shipped_orders": len(statuses[OperationalStatus.SHIPPED]),
            "delivered_orders": len(statuses[OperationalStatus.DELIVERED]),
            "refused_orders": len(statuses[OperationalStatus.REFUSED]),
            "returned_orders": len(statuses[OperationalStatus.RETURNED_TO_ORIGIN]),
            "unreachable_orders": len(statuses[OperationalStatus.UNREACHABLE]),
            "delivery_attempts": attempts, "cash_collected": cash,
        })

    confirmation_groups, delivery_groups = defaultdict(list), defaultdict(list)
    for order in orders:
        cohort_date = order["order_created_at"].astimezone(ZoneInfo(order["reporting_timezone"])).date()
        key = (order["business_id"], cohort_date, order["currency"], order["payment_type"])
        confirmation_groups[key].append((order, by_order[(order["business_id"], order["order_id"])]))
        courier_names = sorted({row["courier"] for row in shipments[(order["business_id"], order["order_id"])] if row["courier"]})
        event_couriers = {row["event_id"]: row.get("courier") for row in raw_events}
        for courier in courier_names:
            courier_history = [event for event in by_order[(order["business_id"], order["order_id"])]
                               if event.source_type not in {"fulfillment_events", "delivery_events"}
                               or event_couriers.get(event.event_id) == courier]
            delivery_groups[(order["business_id"], cohort_date, courier,
                             order["currency"], order["payment_type"])].append((order, courier_history))
    confirmation_rows = []
    for key, members in sorted(confirmation_groups.items()):
        eligible = [m for m in members if m[0]["payment_type"] != "prepaid"]
        providers = [e.provider for _, ev in eligible for e in ev if e.source_type == "confirmation_events"]
        provider = providers[0] if providers else "not_applicable"
        confirmed = [m for m in eligible if any(e.canonical_status == OperationalStatus.CONFIRMED for e in m[1])]
        rejected = [m for m in eligible if any(e.canonical_status == OperationalStatus.REJECTED_CONFIRMATION for e in m[1])]
        unreachable = [m for m in eligible if any(e.canonical_status == OperationalStatus.UNREACHABLE and e.source_type == "confirmation_events" for e in m[1])]
        times = [_hours(order["order_created_at"], min(e.event_at for e in ev if e.canonical_status == OperationalStatus.CONFIRMED))
                 for order, ev in confirmed]
        rates = operational_kpis(eligible_orders=len(eligible), confirmed_orders=len(confirmed), shipped_orders=0,
            delivered_orders=0, refused_orders=0, returned_orders=0, unreachable_orders=len(unreachable),
            delivery_attempts=0, cash_expected=0, cash_collected=0)
        confirmation_rows.append({"business_id": key[0], "cohort_date": key[1], "provider": provider,
            "currency": key[2], "payment_type": key[3], "eligible_orders": len(eligible),
            "confirmed_orders": len(confirmed), "rejected_orders": len(rejected),
            "unreachable_orders": len(unreachable), "confirmation_rate": rates["confirmation_rate"],
            "unreachable_rate": rates["unreachable_rate"], "average_time_to_confirm_hours": _average(times)})
    delivery_rows = []
    for key, members in sorted(delivery_groups.items()):
        def has(status, ev): return any(e.canonical_status == status for e in ev)
        shipped = [m for m in members if has(OperationalStatus.SHIPPED, m[1])]
        delivered = [m for m in shipped if has(OperationalStatus.DELIVERED, m[1])]
        refused = [m for m in shipped if has(OperationalStatus.REFUSED, m[1])]
        returned = [m for m in delivered if has(OperationalStatus.RETURNED_TO_ORIGIN, m[1])]
        attempts = sum(sum(any(row["event_id"] == event.event_id and row.get("attempt_number") is not None
                               for row in raw_events) for event in events)
                       for _, events in shipped)
        rates = operational_kpis(eligible_orders=0, confirmed_orders=0, shipped_orders=len(shipped),
            delivered_orders=len(delivered), refused_orders=len(refused), returned_orders=len(returned),
            unreachable_orders=0, delivery_attempts=attempts, cash_expected=0, cash_collected=0)
        ship_times, delivery_times = [], []
        for order, ev in shipped:
            ship_at = min(e.event_at for e in ev if e.canonical_status == OperationalStatus.SHIPPED)
            ship_times.append(_hours(order["order_created_at"], ship_at))
            delivered_values = [e.event_at for e in ev if e.canonical_status == OperationalStatus.DELIVERED]
            if delivered_values: delivery_times.append(_hours(ship_at, min(delivered_values)))
        delivery_rows.append(dict(zip(("business_id","cohort_date","courier","currency","payment_type"), key)) | {
            "shipped_orders": len(shipped), "delivered_orders": len(delivered), "refused_orders": len(refused),
            "returned_orders": len(returned), "delivery_attempts": attempts, "delivery_rate": rates["delivery_rate"],
            "refusal_rate": rates["refusal_rate"], "return_rate": rates["return_rate"],
            "average_delivery_attempts": rates["average_delivery_attempts"],
            "average_time_to_ship_hours": _average(ship_times),
            "average_time_to_deliver_hours": _average(delivery_times)})

    cod_groups = defaultdict(list)
    for row in silver["cash_collections"]:
        order = order_lookup[(row["business_id"], row["order_id"])]
        day = (row["collected_at"].astimezone(ZoneInfo(order["reporting_timezone"])).date()
               if row["collected_at"] else date(1970, 1, 1))
        cod_groups[(row["business_id"], day, row["provider"], row["collection_currency"])].append(row)
    cod_rows = []
    for key, rows in sorted(cod_groups.items()):
        expected, collected = sum(r["cash_expected"] for r in rows), sum(r["cash_collected"] for r in rows)
        cod_rows.append(dict(zip(("business_id","collection_date","provider","currency"), key)) | {
            "collection_count": len(rows), "cash_expected": expected, "cash_collected": collected,
            "cash_collection_rate": operational_kpis(eligible_orders=0, confirmed_orders=0, shipped_orders=0,
                delivered_orders=0, refused_orders=0, returned_orders=0, unreachable_orders=0,
                delivery_attempts=0, cash_expected=expected, cash_collected=collected)["cash_collection_rate"]})
    remittance_groups = defaultdict(list)
    for row in silver["remittances"]:
        remittance_groups[(row["business_id"], row["period_end"], row["provider"], row["currency"],
                           row["settlement_status"])].append(row)
    remittance_rows = []
    for key, rows in sorted(remittance_groups.items()):
        sums = {name: sum(r[name] for r in rows) for name in
                ("gross_collected","provider_fees","shipping_fees","cod_fees","adjustments","net_remitted")}
        remittance_rows.append(dict(zip(("business_id","period_end","provider","currency","settlement_status"), key)) | {
            "remittance_count": len(rows), **sums,
            "remittance_pending_amount": sums["gross_collected"] if key[-1] != "remitted" else 0.0})
    return {"order_operations_current": tuple(current), "order_operations_daily": tuple(daily),
            "confirmation_performance": tuple(confirmation_rows), "delivery_performance": tuple(delivery_rows),
            "cod_collection_performance": tuple(cod_rows), "remittance_performance": tuple(remittance_rows)}


def _spark_values(rows, schema):
    def convert(value, field):
        if value is None: return None
        if isinstance(field.dataType, DoubleType): return float(value)
        if isinstance(field.dataType, LongType): return int(value)
        return value
    return [tuple(convert(row.get(field.name), field) for field in schema.fields) for row in rows]


def run_pipeline(registry: BusinessRegistry | None = None, *, business_id: str | None = None,
                 source_id: str | None = None, paths: OperationsPaths | None = None,
                 spark: SparkSession | None = None):
    selected = registry or BusinessRegistry()
    output_paths = paths or load_operations_paths()
    extractions = extract_registered_operations(selected, business_id=business_id, source_id=source_id)
    envelopes = tuple(envelope for _, batch in extractions for envelope in batch)
    silver = normalize_operations(extractions, selected)
    gold = build_gold_rows(silver)
    own_spark = spark is None
    session = spark or build_operations_spark_session(master=os.getenv("SPARK_MASTER", "local[*]"))
    try:
        bronze = [(e.business_id,e.source_type,e.source_id,e.ingestion_id,e.record_id,e.extracted_at_utc,
                   e.source_updated_at_utc,e.schema_version,json.dumps(e.payload,sort_keys=True,separators=(",",":")))
                  for e in envelopes]
        session.createDataFrame(bronze, BRONZE_SCHEMA).write.mode("overwrite").parquet(str(output_paths.bronze))
        for name, rows in {**silver, **gold}.items():
            schema = SILVER_SCHEMAS.get(name) or GOLD_SCHEMAS[name]
            session.createDataFrame(_spark_values(rows, schema), schema).write.mode("overwrite").parquet(
                str(getattr(output_paths, name)))
    finally:
        if own_spark: session.stop()
    return {"bronze": len(envelopes), **{name: len(rows) for name, rows in silver.items()},
            **{name: len(rows) for name, rows in gold.items()}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "demo")); parser.add_argument("--business-id")
    args = parser.parse_args(argv)
    if args.command == "build": result = run_pipeline(business_id=args.business_id)
    else:
        registry = BusinessRegistry(); extractions = extract_registered_operations(registry, business_id=args.business_id)
        silver = normalize_operations(extractions, registry); gold = build_gold_rows(silver)
        result = {"bronze": sum(len(batch) for _, batch in extractions),
                  **{name: len(rows) for name, rows in silver.items()}, **{name: len(rows) for name, rows in gold.items()}}
    print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
