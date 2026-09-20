"""Deterministic unified-economics enrichment over existing canonical facts."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql.types import (BooleanType, DateType, DoubleType, LongType, StringType,
                               StructField, StructType, TimestampType)

from src.economics.adapters import ECONOMICS_SOURCE_TYPES, economics_adapter_for
from src.economics.models import (AttributionLink, CostBasis, CostComponent, CostScope,
                                  CostType, EconomicStatus, ProductCost, safe_divide)
from src.marketing.pipeline import (GOLD_SCHEMAS as MARKETING_SCHEMAS,
                                    build_gold_rows as build_marketing_gold,
                                    extract_registered_marketing, normalize_latest)
from src.onboarding.registry import BusinessRegistry, validate_business
from src.operations.models import OperationalStatus, event_latest_revisions
from src.operations.pipeline import (GOLD_SCHEMAS as OPERATIONS_GOLD_SCHEMAS,
                                     SILVER_SCHEMAS as OPERATIONS_SILVER_SCHEMAS,
                                     _event_object, build_gold_rows as build_operations_gold,
                                     extract_registered_operations, normalize_operations)
from src.streaming.windows_spark import (configure_windows_spark_builder,
                                         configure_windows_spark_environment)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ECONOMICS_SILVER_TABLES = ("product_costs", "cost_components", "attribution_links")
ECONOMICS_GOLD_TABLES = ("order_economics", "business_economics_daily",
                         "business_economics_cohort", "attributed_campaign_economics")


@dataclass(frozen=True, slots=True)
class EconomicsPaths:
    bronze: Path
    product_costs: Path
    cost_components: Path
    attribution_links: Path
    order_economics: Path
    business_economics_daily: Path
    business_economics_cohort: Path
    attributed_campaign_economics: Path


def _path(value: str, root: Path) -> Path:
    candidate = Path(value)
    return candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()


def _as_date(value):
    return value if isinstance(value, date) else date.fromisoformat(value)


def load_economics_paths(environ=None, *, project_root: Path = PROJECT_ROOT) -> EconomicsPaths:
    env = os.environ if environ is None else environ
    names = {
        "bronze": ("BRONZE_ECONOMICS_PATH", "data/bronze/commerce_economics"),
        **{name: (f"SILVER_{name.upper()}_PATH", f"data/silver/{name}")
           for name in ECONOMICS_SILVER_TABLES},
        **{name: (f"GOLD_{name.upper()}_PATH", f"data/gold/{name}")
           for name in ECONOMICS_GOLD_TABLES},
    }
    return EconomicsPaths(**{name: _path(env.get(variable, default), project_root)
                             for name, (variable, default) in names.items()})


def build_economics_spark_session(*, app_name="pulse-economics", master="local[*]"):
    configure_windows_spark_environment()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName(app_name).master(master)
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", os.getenv("ECONOMICS_SHUFFLE_PARTITIONS", "4")))
    return configure_windows_spark_builder(builder).getOrCreate()


def _schema(*columns):
    types = {"str": StringType(), "int": LongType(), "float": DoubleType(),
             "date": DateType(), "time": TimestampType(), "bool": BooleanType()}
    return StructType([StructField(name, types[kind], nullable) for name, kind, nullable in columns])


BRONZE_SCHEMA = _schema(
    ("business_id","str",False),("source_type","str",False),("source_id","str",False),
    ("ingestion_id","str",False),("record_id","str",False),("extracted_at_utc","time",False),
    ("source_updated_at_utc","time",True),("schema_version","str",False),("payload","str",False))

SILVER_SCHEMAS = {
    "product_costs": _schema(
        ("business_id","str",False),("source_id","str",False),("provider","str",False),
        ("cost_record_id","str",False),("external_record_id","str",False),
        ("product_id","str",True),("variant_id","str",True),("sku","str",True),
        ("unit_cogs","float",False),("currency","str",False),("valid_from","date",False),
        ("valid_to","date",True),("revision","int",False),("details_json","str",False)),
    "cost_components": _schema(
        ("business_id","str",False),("source_id","str",False),("provider","str",False),
        ("cost_component_id","str",False),("external_record_id","str",False),
        ("cost_type","str",False),("amount","float",False),("currency","str",False),
        ("effective_at","time",False),("cost_basis","str",False),("cost_scope","str",False),
        ("precedence_key","str",True),("order_id","str",True),("shipment_id","str",True),
        ("remittance_id","str",True),("product_id","str",True),("variant_id","str",True),
        ("sku","str",True),("revision","int",False),("corrects_record_id","str",True),
        ("details_json","str",False)),
    "attribution_links": _schema(
        ("business_id","str",False),("source_id","str",False),("provider","str",False),
        ("attribution_link_id","str",False),("external_record_id","str",False),
        ("order_id","str",False),("marketing_platform","str",False),
        ("marketing_source_id","str",False),("marketing_date","date",False),
        ("attribution_method","str",False),("attribution_weight","float",False),
        ("linked_at","time",False),("campaign_id","str",True),("ad_group_id","str",True),
        ("ad_id","str",True),("revision","int",False),("details_json","str",False)),
}

ORDER_COST_FIELDS = ("product_cogs", "shipping_cost", "cod_fee", "fulfillment_fee",
                     "return_fee", "payment_processing_fee", "provider_fee",
                     "other_variable_cost")

GOLD_SCHEMAS = {
    "order_economics": _schema(
        ("business_id","str",False),("order_source_id","str",False),("order_id","str",False),
        ("order_created_date","date",False),("reporting_timezone","str",False),
        ("currency","str",False),("payment_type","str",False),
        ("order_value","float",False),("confirmed_order_value","float",False),
        ("shipped_order_value","float",False),("delivered_order_value","float",False),
        ("cash_expected","float",False),("cash_collected","float",False),
        ("gross_collected","float",False),("net_remitted","float",False),
        *( (name,"float",False) for name in ORDER_COST_FIELDS ),
        ("variable_operational_cost","float",False),("recognized_economic_value","float",False),
        ("allocated_marketing_spend","float",True),("marketing_currency","str",True),
        ("contribution_order_value","float",True),("contribution_delivered_value","float",True),
        ("contribution_cash_collected","float",True),("contribution_net_remitted","float",True),
        ("contribution_before_marketing","float",True),("contribution_after_marketing","float",True),
        ("confirmed","bool",False),("shipped","bool",False),("delivered","bool",False),
        ("refused","bool",False),("returned","bool",False),("delivery_attempts","int",False),
        ("shipment_count","int",False),("missing_cogs_lines","int",False),("cogs_complete","bool",False),
        ("has_operational_costs","bool",False),("has_collection_data","bool",False),
        ("has_remittance_data","bool",False),("has_marketing_attribution","bool",False),
        ("currency_compatible","bool",False),("cohort_mature","bool",False),
        ("uses_estimated_costs","bool",False),("economic_status","str",False)),
    "business_economics_daily": _schema(
        ("business_id","str",False),("event_date","date",False),("reporting_timezone","str",False),
        ("currency","str",False),("marketing_spend","float",False),("orders_created","int",False),
        ("order_value","float",False),("confirmed_orders","int",False),("confirmed_order_value","float",False),
        ("shipped_orders","int",False),("shipped_order_value","float",False),
        ("delivered_orders","int",False),("delivered_order_value","float",False),
        ("cash_expected","float",False),("cash_collected","float",False),
        ("gross_collected","float",False),("net_remitted","float",False),
        ("variable_cost","float",False),("return_cost","float",False)),
    "business_economics_cohort": _schema(
        ("business_id","str",False),("cohort_date","date",False),("reporting_timezone","str",False),
        ("currency","str",False),("marketing_spend","float",False),("order_count","int",False),
        ("confirmed_orders","int",False),("shipped_orders","int",False),("delivered_orders","int",False),
        ("refused_orders","int",False),("returned_orders","int",False),("order_value","float",False),
        ("delivered_order_value","float",False),("cash_collected","float",False),
        ("net_remitted","float",False),("product_cogs","float",False),
        ("variable_operational_cost","float",False),("contribution_before_marketing","float",True),
        ("contribution_after_marketing","float",True),("marketing_cost_per_order","float",True),
        ("marketing_cost_per_confirmed_order","float",True),("marketing_cost_per_shipped_order","float",True),
        ("marketing_cost_per_delivered_order","float",True),("cogs_per_delivered_order","float",True),
        ("operations_cost_per_delivered_order","float",True),("contribution_per_delivered_order","float",True),
        ("contribution_margin_pct","float",True),("cash_collection_per_delivered_order","float",True),
        ("pending_remittance_amount","float",False),("delivered_but_unremitted_count","int",False),
        ("delivered_but_unremitted_value","float",False),("complete_orders","int",False),
        ("incomplete_orders","int",False),("cohort_mature","bool",False)),
    "attributed_campaign_economics": _schema(
        ("business_id","str",False),("marketing_date","date",False),("platform","str",False),
        ("marketing_source_id","str",False),("campaign_id","str",False),
        ("marketing_currency","str",False),("commerce_currency","str",True),
        ("campaign_spend","float",False),("platform_conversion_value","float",False),
        ("platform_roas","float",True),("attributed_orders","float",False),
        ("attributed_confirmed_orders","float",False),("attributed_delivered_orders","float",False),
        ("attributed_delivered_value","float",False),("attributed_cash_collected","float",False),
        ("attributed_cogs","float",False),("attributed_operational_cost","float",False),
        ("attributed_contribution_before_marketing","float",True),
        ("delivered_value_roas","float",True),("cash_roas","float",True),
        ("contribution_roas","float",True),("currency_compatible","bool",False)),
}


def extract_registered_economics(registry: BusinessRegistry | None = None, *,
                                 business_id: str | None = None, source_id: str | None = None):
    selected = registry or BusinessRegistry()
    owners = [business_id] if business_id else [b.business_id for b in selected.list_businesses()
                                                if b.status == "ACTIVE"]
    output = []
    for owner in owners:
        if not validate_business(selected, owner).valid:
            raise ValueError(f"Invalid onboarding for {owner}")
        enabled = set(selected.get_business(owner).enabled_sources)
        for config in selected.sources_for(owner):
            if config.enabled and config.source_id in enabled and config.source_type in ECONOMICS_SOURCE_TYPES:
                if source_id is None or source_id == config.source_id:
                    adapter = economics_adapter_for(config)
                    output.append((adapter, adapter.extract()))
    if source_id is not None and not output:
        raise ValueError(f"Enabled economics source {source_id!r} was not found")
    return tuple(output)


def normalize_economics(extractions) -> dict[str, tuple[dict[str, Any], ...]]:
    groups = {name: {} for name in ECONOMICS_SILVER_TABLES}
    type_to_table = {"product_costs": "product_costs", "variable_cost_events": "cost_components",
                     "attribution_links": "attribution_links"}
    for adapter, envelopes in extractions:
        table = type_to_table[adapter.config.source_type]
        for envelope in envelopes:
            row = adapter.normalize(envelope)
            row["details_json"] = json.dumps(row.pop("details"), sort_keys=True, separators=(",", ":"))
            groups[table][row[next(name for name in (
                "cost_record_id", "cost_component_id", "attribution_link_id") if name in row)]] = row
    return {name: tuple(sorted(values.values(), key=lambda row: (
        row["business_id"], row["external_record_id"], row["revision"])))
        for name, values in groups.items()}


def effective_product_cost(costs: Iterable[dict[str, Any]], line: dict[str, Any],
                           order_date: date) -> dict[str, Any] | None:
    matches = []
    for cost in costs:
        if cost["business_id"] != line["business_id"] or cost["currency"] != line["currency"]:
            continue
        if cost["valid_from"] > order_date or (cost["valid_to"] and cost["valid_to"] < order_date):
            continue
        specificity = (3 if line.get("sku") and cost.get("sku") == line.get("sku") else
                       2 if line.get("variant_id") and cost.get("variant_id") == line.get("variant_id") else
                       1 if line.get("product_id") and cost.get("product_id") == line.get("product_id") else 0)
        if specificity:
            matches.append((specificity, cost["valid_from"], cost["revision"], cost["cost_record_id"], cost))
    return max(matches)[-1] if matches else None


def resolve_cost_components(rows: Iterable[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    latest = {}
    for row in rows:
        key = (row["business_id"], row["source_id"], row["provider"], row["external_record_id"])
        if key not in latest or (row["revision"], row["cost_component_id"]) > (
                latest[key]["revision"], latest[key]["cost_component_id"]):
            latest[key] = row
    selected, precedence = [], {}
    basis = {"ACTUAL": 3, "CONFIGURED": 2, "ESTIMATED": 1}
    scope = {"ORDER": 4, "SHIPMENT": 3, "REMITTANCE": 2, "SCHEDULE": 1}
    for row in latest.values():
        if not row.get("precedence_key"):
            selected.append(row)
            continue
        key = (row["business_id"], row["precedence_key"])
        rank = (basis[row["cost_basis"]], scope[row["cost_scope"]], row["revision"], row["cost_component_id"])
        if key not in precedence or rank > precedence[key][0]:
            precedence[key] = (rank, row)
    selected.extend(value[1] for value in precedence.values())
    return tuple(sorted(selected, key=lambda row: (row["business_id"], row["cost_component_id"])))


def _latest_links(rows):
    latest = {}
    for row in rows:
        key = (row["business_id"], row["source_id"], row["provider"], row["external_record_id"])
        if key not in latest or (row["revision"], row["attribution_link_id"]) > (
                latest[key]["revision"], latest[key]["attribution_link_id"]):
            latest[key] = row
    return tuple(latest.values())


def _marketing_index(marketing):
    index = {}
    for row in marketing.get("campaign_performance", ()):
        index[(row["business_id"], row["source_id"], row["platform"], row["campaign_id"],
               _as_date(row["report_date"]))] = row
    return index


def _remittance_allocations(operations_silver):
    collection_amount = {(r["business_id"], r["collection_id"]): r["cash_collected"]
                         for r in operations_silver["cash_collections"]}
    output = defaultdict(lambda: {"gross_collected": 0.0, "net_remitted": 0.0})
    for remittance in operations_silver["remittances"]:
        links = json.loads(remittance["order_links_json"])
        weights = [collection_amount.get((remittance["business_id"], link.get("collection_id")), 0.0)
                   for link in links]
        total = sum(weights)
        if total <= 0 and links:
            weights = [1.0] * len(links); total = float(len(links))
        for link, weight in zip(links, weights):
            share = weight / total if total else 0.0
            target = output[(remittance["business_id"], link["order_id"])]
            target["gross_collected"] += remittance["gross_collected"] * share
            target["net_remitted"] += remittance["net_remitted"] * share
    return output


def _operation_event(row):
    names = ("business_id","source_type","source_id","provider","event_id","external_event_id",
             "order_id","event_type","canonical_status","provider_status","event_at","received_at_utc",
             "revision","corrects_event_id","shipment_id")
    details = row.get("details")
    if details is None:
        details = json.loads(row.get("details_json") or "{}")
    values = {name: row[name] for name in names}
    for name in ("event_at", "received_at_utc"):
        if values[name].tzinfo is None:
            values[name] = values[name].replace(tzinfo=timezone.utc)
    return _event_object({**values, "details": details})


def _aware_utc(value):
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def build_order_economics(operations_silver, operations_gold, economics_silver,
                          marketing_gold, *, as_of_date=date(2026, 2, 15)):
    current = {(r["business_id"], r["order_id"]): r
               for r in operations_gold["order_operations_current"]}
    lines = defaultdict(list)
    for row in operations_silver["order_lines"]:
        lines[(row["business_id"], row["order_id"])].append(row)
    events = defaultdict(list)
    event_objects = [_operation_event(row) for row in operations_silver["operational_events"]]
    for event in event_latest_revisions(event_objects):
        events[(event.business_id, event.order_id)].append(event)
    costs = defaultdict(list)
    for row in resolve_cost_components(economics_silver["cost_components"]):
        if row.get("order_id"):
            costs[(row["business_id"], row["order_id"])].append(row)
    links = defaultdict(list)
    for row in _latest_links(economics_silver["attribution_links"]):
        links[(row["business_id"], row["order_id"])].append(row)
    remitted = _remittance_allocations(operations_silver)
    marketing_index = _marketing_index(marketing_gold)
    link_groups = defaultdict(list)
    for order_links in links.values():
        for link in order_links:
            link_groups[(link["business_id"], link["marketing_source_id"], link["marketing_platform"],
                         link.get("campaign_id"), link["marketing_date"])].append(link)
    output = []
    for order in operations_silver["commerce_orders"]:
        key = (order["business_id"], order["order_id"])
        state, history = current[key], events[key]
        statuses = {event.canonical_status for event in history}
        confirmed = OperationalStatus.CONFIRMED in statuses or order["payment_type"] == "prepaid"
        shipped = OperationalStatus.SHIPPED in statuses
        delivered_once = OperationalStatus.DELIVERED in statuses
        returned = OperationalStatus.RETURNED_TO_ORIGIN in statuses
        refused = OperationalStatus.REFUSED in statuses
        delivered = delivered_once and not returned
        order_day = _aware_utc(order["order_created_at"]).astimezone(
            ZoneInfo(order["reporting_timezone"])).date()
        product_cogs, missing = 0.0, 0
        for line in lines[key]:
            match = effective_product_cost(economics_silver["product_costs"], line, order_day)
            if match is None:
                missing += 1
            else:
                product_cogs += match["unit_cogs"] * line["quantity"]
        field_map = {"SHIPPING":"shipping_cost", "COD_FEE":"cod_fee",
                     "FULFILLMENT":"fulfillment_fee", "RETURN_FEE":"return_fee",
                     "PAYMENT_PROCESSING":"payment_processing_fee", "PROVIDER_FEE":"provider_fee",
                     "OTHER_VARIABLE_COST":"other_variable_cost"}
        cost_values = {name: 0.0 for name in ORDER_COST_FIELDS}
        cost_values["product_cogs"] = product_cogs
        compatible_costs = []
        for component in costs[key]:
            if component["currency"] == order["currency"]:
                compatible_costs.append(component)
                cost_values[field_map[component["cost_type"]]] += component["amount"]
        variable_cost = sum(cost_values.values())
        order_links = links[key]
        allocated_spend, marketing_currencies = 0.0, set()
        for link in order_links:
            campaign_key = (link["business_id"], link["marketing_source_id"],
                            link["marketing_platform"], link.get("campaign_id"), link["marketing_date"])
            marketing = marketing_index.get(campaign_key)
            if marketing:
                marketing_currencies.add(marketing["currency"])
                denominator = sum(item["attribution_weight"] for item in link_groups[campaign_key])
                allocated_spend += marketing["spend"] * link["attribution_weight"] / denominator
        marketing_currency = next(iter(marketing_currencies)) if len(marketing_currencies) == 1 else None
        currency_compatible = bool(order_links) and marketing_currency == order["currency"]
        # No lines means there is no evidence that product costs are complete.
        # This matters for valid order facts whose source cannot supply line
        # detail (for example cancelled or unavailable external orders).
        cogs_complete = bool(lines[key]) and missing == 0
        recognized = (state["cash_collected"] if order["payment_type"] == "cod" and
                      state["cash_collected"] > 0 else order["order_value"] if delivered else 0.0)
        contribution = recognized - variable_cost if cogs_complete else None
        maturity = (as_of_date - order_day).days >= 14
        if not cogs_complete:
            economic_status = EconomicStatus.INCOMPLETE_COSTS
        elif order_links and not currency_compatible:
            economic_status = EconomicStatus.CURRENCY_MISMATCH
        elif not maturity:
            economic_status = EconomicStatus.IMMATURE_COHORT
        elif not order_links:
            economic_status = EconomicStatus.INCOMPLETE_ATTRIBUTION
        else:
            economic_status = EconomicStatus.COMPLETE
        settlement = remitted[key]
        output.append({
            "business_id": order["business_id"], "order_source_id": order["source_id"],
            "order_id": order["order_id"], "order_created_date": order_day,
            "reporting_timezone": order["reporting_timezone"], "currency": order["currency"],
            "payment_type": order["payment_type"], "order_value": order["order_value"],
            "confirmed_order_value": order["order_value"] if confirmed else 0.0,
            "shipped_order_value": order["order_value"] if shipped else 0.0,
            "delivered_order_value": order["order_value"] if delivered else 0.0,
            "cash_expected": state["cash_expected"], "cash_collected": state["cash_collected"],
            **settlement, **cost_values, "variable_operational_cost": variable_cost,
            "recognized_economic_value": recognized,
            "allocated_marketing_spend": allocated_spend if order_links else None,
            "marketing_currency": marketing_currency,
            "contribution_order_value": order["order_value"] - variable_cost if cogs_complete else None,
            "contribution_delivered_value": ((order["order_value"] if delivered else 0.0) - variable_cost
                                              if cogs_complete else None),
            "contribution_cash_collected": state["cash_collected"] - variable_cost if cogs_complete else None,
            "contribution_net_remitted": settlement["net_remitted"] - variable_cost if cogs_complete else None,
            "contribution_before_marketing": contribution,
            "contribution_after_marketing": (contribution - allocated_spend
                                              if contribution is not None and currency_compatible else None),
            "confirmed": confirmed, "shipped": shipped, "delivered": delivered,
            "refused": refused, "returned": returned,
            "delivery_attempts": state["delivery_attempts"], "shipment_count": state["shipment_count"],
            "missing_cogs_lines": missing, "cogs_complete": cogs_complete,
            "has_operational_costs": bool(compatible_costs),
            "has_collection_data": state["cash_expected"] > 0 or state["cash_collected"] > 0,
            "has_remittance_data": settlement["gross_collected"] > 0 or settlement["net_remitted"] > 0,
            "has_marketing_attribution": bool(order_links), "currency_compatible": currency_compatible,
            "cohort_mature": maturity,
            "uses_estimated_costs": any(c["cost_basis"] == "ESTIMATED" for c in compatible_costs),
            "economic_status": economic_status.value,
        })
    return tuple(sorted(output, key=lambda row: (row["business_id"], row["order_id"])))


def _daily_rows(order_rows, operations_silver, economics_silver, marketing_gold):
    groups = defaultdict(lambda: defaultdict(float))
    timezones = {row["business_id"]: row["reporting_timezone"] for row in order_rows}
    currencies = {(row["business_id"], row["order_id"]): row["currency"] for row in order_rows}
    order_values = {(row["business_id"], row["order_id"]): row["order_value"] for row in order_rows}
    for row in order_rows:
        key = (row["business_id"], row["order_created_date"], row["currency"])
        groups[key]["orders_created"] += 1; groups[key]["order_value"] += row["order_value"]
    for event in event_latest_revisions(_operation_event(raw)
                                        for raw in operations_silver["operational_events"]):
        currency = currencies.get((event.business_id, event.order_id))
        if not currency: continue
        day = event.event_at.astimezone(ZoneInfo(timezones[event.business_id])).date()
        target = groups[(event.business_id, day, currency)]
        value = order_values[(event.business_id, event.order_id)]
        mapping = {OperationalStatus.CONFIRMED:("confirmed_orders","confirmed_order_value"),
                   OperationalStatus.SHIPPED:("shipped_orders","shipped_order_value"),
                   OperationalStatus.DELIVERED:("delivered_orders","delivered_order_value")}
        if event.canonical_status in mapping:
            count, amount = mapping[event.canonical_status]; target[count] += 1; target[amount] += value
    for row in operations_silver["cash_collections"]:
        if row["collected_at"]:
            day = _aware_utc(row["collected_at"]).astimezone(
                ZoneInfo(timezones[row["business_id"]])).date()
            target = groups[(row["business_id"], day, row["collection_currency"])]
            target["cash_expected"] += row["cash_expected"]; target["cash_collected"] += row["cash_collected"]
    for row in operations_silver["remittances"]:
        target = groups[(row["business_id"], row["period_end"], row["currency"])]
        target["gross_collected"] += row["gross_collected"]; target["net_remitted"] += row["net_remitted"]
    for row in resolve_cost_components(economics_silver["cost_components"]):
        day = _aware_utc(row["effective_at"]).astimezone(
            ZoneInfo(timezones[row["business_id"]])).date()
        target = groups[(row["business_id"], day, row["currency"])]
        target["variable_cost"] += row["amount"]
        if row["cost_type"] == "RETURN_FEE": target["return_cost"] += row["amount"]
    for row in marketing_gold.get("marketing_daily", ()):
        if row["business_id"] in timezones:
            groups[(row["business_id"], _as_date(row["report_date"]), row["currency"])]["marketing_spend"] += row["spend"]
    fields = [field.name for field in GOLD_SCHEMAS["business_economics_daily"].fields]
    output = []
    for (business, day, currency), values in sorted(groups.items()):
        row = {name: 0.0 for name in fields}
        row.update(values); row.update(business_id=business, event_date=day,
                                       reporting_timezone=timezones[business], currency=currency)
        for name in ("orders_created","confirmed_orders","shipped_orders","delivered_orders"):
            row[name] = int(row[name])
        output.append(row)
    return tuple(output)


def _cohort_rows(order_rows, marketing_gold):
    groups = defaultdict(list)
    for row in order_rows:
        groups[(row["business_id"], row["order_created_date"], row["reporting_timezone"],
                row["currency"])].append(row)
    marketing = defaultdict(float)
    for row in marketing_gold.get("marketing_daily", ()):
        marketing[(row["business_id"], _as_date(row["report_date"]), row["reporting_timezone"], row["currency"])] += row["spend"]
    output = []
    for key, rows in sorted(groups.items()):
        spend = marketing[key]
        complete = all(row["cogs_complete"] for row in rows)
        delivered_value = sum(row["delivered_order_value"] for row in rows)
        variable_cost = sum(row["variable_operational_cost"] for row in rows)
        before = sum(row["contribution_before_marketing"] for row in rows) if complete else None
        after = before - spend if before is not None else None
        delivered = sum(row["delivered"] for row in rows)
        pending = sum(max(row["gross_collected"] - row["net_remitted"], 0.0) for row in rows)
        unremitted = [r for r in rows if r["delivered"] and r["net_remitted"] == 0]
        recognized = sum(row["recognized_economic_value"] for row in rows)
        output.append(dict(zip(("business_id","cohort_date","reporting_timezone","currency"), key)) | {
            "marketing_spend": spend, "order_count": len(rows),
            "confirmed_orders": sum(r["confirmed"] for r in rows), "shipped_orders": sum(r["shipped"] for r in rows),
            "delivered_orders": delivered, "refused_orders": sum(r["refused"] for r in rows),
            "returned_orders": sum(r["returned"] for r in rows), "order_value": sum(r["order_value"] for r in rows),
            "delivered_order_value": delivered_value, "cash_collected": sum(r["cash_collected"] for r in rows),
            "net_remitted": sum(r["net_remitted"] for r in rows), "product_cogs": sum(r["product_cogs"] for r in rows),
            "variable_operational_cost": variable_cost, "contribution_before_marketing": before,
            "contribution_after_marketing": after, "marketing_cost_per_order": safe_divide(spend, len(rows)),
            "marketing_cost_per_confirmed_order": safe_divide(spend, sum(r["confirmed"] for r in rows)),
            "marketing_cost_per_shipped_order": safe_divide(spend, sum(r["shipped"] for r in rows)),
            "marketing_cost_per_delivered_order": safe_divide(spend, delivered),
            "cogs_per_delivered_order": safe_divide(sum(r["product_cogs"] for r in rows), delivered),
            "operations_cost_per_delivered_order": safe_divide(variable_cost - sum(r["product_cogs"] for r in rows), delivered),
            "contribution_per_delivered_order": safe_divide(after, delivered) if after is not None else None,
            "contribution_margin_pct": safe_divide(after, recognized) if after is not None and recognized > 0 else None,
            "cash_collection_per_delivered_order": safe_divide(sum(r["cash_collected"] for r in rows), delivered),
            "pending_remittance_amount": pending, "delivered_but_unremitted_count": len(unremitted),
            "delivered_but_unremitted_value": sum(r["delivered_order_value"] for r in unremitted),
            "complete_orders": sum(r["economic_status"] == "COMPLETE" for r in rows),
            "incomplete_orders": sum(r["economic_status"] != "COMPLETE" for r in rows),
            "cohort_mature": all(r["cohort_mature"] for r in rows),
        })
    return tuple(output)


def _campaign_rows(order_rows, economics_silver, marketing_gold):
    orders = {(r["business_id"], r["order_id"]): r for r in order_rows}
    economic_businesses = {key[0] for key in orders}
    links_by_campaign = defaultdict(list)
    for link in _latest_links(economics_silver["attribution_links"]):
        key = (link["business_id"], link["marketing_date"], link["marketing_platform"],
               link["marketing_source_id"], link.get("campaign_id"))
        links_by_campaign[key].append(link)
    output = []
    for marketing in marketing_gold.get("campaign_performance", ()):
        if marketing["business_id"] not in economic_businesses:
            continue
        marketing_date = _as_date(marketing["report_date"])
        key = (marketing["business_id"], marketing_date, marketing["platform"],
               marketing["source_id"], marketing["campaign_id"])
        links = links_by_campaign.get(key, [])
        currency_groups = defaultdict(list)
        for link in links:
            order = orders.get((link["business_id"], link["order_id"]))
            if order: currency_groups[order["currency"]].append((link, order))
        if not currency_groups:
            currency_groups[None] = []
        total_weight = sum(link["attribution_weight"] for link in links)
        for commerce_currency, members in sorted(currency_groups.items(), key=lambda item: item[0] or ""):
            group_weight = sum(link["attribution_weight"] for link, _ in members)
            spend = marketing["spend"] * group_weight / total_weight if total_weight else marketing["spend"]
            compatible = commerce_currency == marketing["currency"] and commerce_currency is not None
            def weighted(field): return sum(order[field] * link["attribution_weight"] for link, order in members)
            before = (weighted("contribution_before_marketing")
                      if members and all(order["contribution_before_marketing"] is not None for _, order in members)
                      else None)
            delivered_value, cash = weighted("delivered_order_value"), weighted("cash_collected")
            output.append({
                "business_id": marketing["business_id"], "marketing_date": marketing_date,
                "platform": marketing["platform"], "marketing_source_id": marketing["source_id"],
                "campaign_id": marketing["campaign_id"], "marketing_currency": marketing["currency"],
                "commerce_currency": commerce_currency, "campaign_spend": spend,
                "platform_conversion_value": marketing["platform_conversion_value"],
                "platform_roas": safe_divide(marketing["platform_conversion_value"], marketing["spend"]),
                "attributed_orders": group_weight,
                "attributed_confirmed_orders": sum(link["attribution_weight"] for link, order in members if order["confirmed"]),
                "attributed_delivered_orders": sum(link["attribution_weight"] for link, order in members if order["delivered"]),
                "attributed_delivered_value": delivered_value, "attributed_cash_collected": cash,
                "attributed_cogs": weighted("product_cogs"),
                "attributed_operational_cost": weighted("variable_operational_cost") - weighted("product_cogs"),
                "attributed_contribution_before_marketing": before,
                "delivered_value_roas": safe_divide(delivered_value, spend) if compatible else None,
                "cash_roas": safe_divide(cash, spend) if compatible else None,
                "contribution_roas": safe_divide(before, spend) if compatible and before is not None else None,
                "currency_compatible": compatible,
            })
    return tuple(output)


def build_gold_rows(operations_silver, operations_gold, economics_silver, marketing_gold,
                    *, as_of_date=date(2026, 2, 15)):
    orders = build_order_economics(operations_silver, operations_gold, economics_silver,
                                   marketing_gold, as_of_date=as_of_date)
    return {"order_economics": orders,
            "business_economics_daily": _daily_rows(orders, operations_silver, economics_silver, marketing_gold),
            "business_economics_cohort": _cohort_rows(orders, marketing_gold),
            "attributed_campaign_economics": _campaign_rows(orders, economics_silver, marketing_gold)}


def _rows_from_frame(frame):
    return tuple(row.asDict(recursive=True) for row in frame.collect())


def _spark_values(rows, schema):
    values = []
    for row in rows:
        converted = []
        for field in schema.fields:
            value = row.get(field.name)
            if value is not None and isinstance(field.dataType, DoubleType): value = float(value)
            if value is not None and isinstance(field.dataType, LongType): value = int(value)
            converted.append(value)
        values.append(tuple(converted))
    return values


def demo_rows(registry=None, *, business_id=None):
    selected = registry or BusinessRegistry()
    ops_ext = extract_registered_operations(selected, business_id=business_id)
    operations_silver = normalize_operations(ops_ext, selected)
    operations_gold = build_operations_gold(operations_silver)
    marketing_silver = normalize_latest(extract_registered_marketing(selected, business_id=business_id))
    marketing_gold = build_marketing_gold(marketing_silver)
    economics_ext = extract_registered_economics(selected, business_id=business_id)
    economics_silver = normalize_economics(economics_ext)
    return economics_ext, economics_silver, build_gold_rows(
        operations_silver, operations_gold, economics_silver, marketing_gold)


def run_pipeline(registry=None, *, paths=None, spark=None):
    from src.marketing.pipeline import load_marketing_paths
    from src.operations.pipeline import load_operations_paths
    from src.utils.parquet import read_parquet_data_files

    selected, output = registry or BusinessRegistry(), paths or load_economics_paths()
    operations_paths, marketing_paths = load_operations_paths(), load_marketing_paths()
    own_spark = spark is None
    session = spark or build_economics_spark_session(master=os.getenv("SPARK_MASTER", "local[*]"))
    try:
        operations_silver = {name: _rows_from_frame(read_parquet_data_files(
            session, Path(getattr(operations_paths, name))).select(*OPERATIONS_SILVER_SCHEMAS[name].fieldNames()))
            for name in OPERATIONS_SILVER_SCHEMAS}
        operations_gold = {name: _rows_from_frame(read_parquet_data_files(
            session, Path(getattr(operations_paths, name))).select(*OPERATIONS_GOLD_SCHEMAS[name].fieldNames()))
            for name in OPERATIONS_GOLD_SCHEMAS}
        marketing_gold = {name: _rows_from_frame(read_parquet_data_files(
            session, Path(getattr(marketing_paths, name))).select(*MARKETING_SCHEMAS[name].fieldNames()))
            for name in MARKETING_SCHEMAS}
        extractions = extract_registered_economics(selected)
        economics_silver = normalize_economics(extractions)
        gold = build_gold_rows(operations_silver, operations_gold, economics_silver, marketing_gold)
        envelopes = tuple(envelope for _, batch in extractions for envelope in batch)
        bronze = [(e.business_id,e.source_type,e.source_id,e.ingestion_id,e.record_id,e.extracted_at_utc,
                   e.source_updated_at_utc,e.schema_version,json.dumps(e.payload,sort_keys=True,separators=(",",":")))
                  for e in envelopes]
        session.createDataFrame(bronze, BRONZE_SCHEMA).write.mode("overwrite").parquet(str(output.bronze))
        for name, rows in {**economics_silver, **gold}.items():
            schema = SILVER_SCHEMAS.get(name) or GOLD_SCHEMAS[name]
            session.createDataFrame(_spark_values(rows, schema), schema).write.mode("overwrite").parquet(
                str(getattr(output, name)))
        return {"bronze": len(envelopes), **{name: len(rows) for name, rows in economics_silver.items()},
                **{name: len(rows) for name, rows in gold.items()}}
    finally:
        if own_spark: session.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "demo")); parser.add_argument("--business-id")
    args = parser.parse_args(argv)
    if args.command == "build": result = run_pipeline()
    else:
        extractions, silver, gold = demo_rows(business_id=args.business_id)
        result = {"bronze": sum(len(batch) for _, batch in extractions),
                  **{name: len(rows) for name, rows in silver.items()},
                  **{name: len(rows) for name, rows in gold.items()}}
    print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
