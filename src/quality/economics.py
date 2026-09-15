"""Economics-specific completeness and reconciliation checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from src.quality.models import Severity


@dataclass(frozen=True, slots=True)
class EconomicsQualityIssue:
    code: str
    severity: Severity
    message: str
    business_id: str | None = None
    order_id: str | None = None


def check_economics(silver: Mapping[str, Sequence[Mapping[str, Any]]],
                    gold: Mapping[str, Sequence[Mapping[str, Any]]] | None = None
                    ) -> tuple[EconomicsQualityIssue, ...]:
    issues = []
    def add(code, severity, message, row=None):
        row = row or {}
        issues.append(EconomicsQualityIssue(code, severity, message,
            row.get("business_id"), row.get("order_id")))

    for table, identity in (("product_costs", "cost_record_id"),
                            ("cost_components", "cost_component_id"),
                            ("attribution_links", "attribution_link_id")):
        seen = set()
        for row in silver.get(table, ()):
            if not row.get("business_id") or not row.get(identity):
                add("economic_identity_incomplete", Severity.CRITICAL,
                    f"{table} identity is incomplete", row)
            if row.get(identity) in seen:
                add("duplicate_economic_record", Severity.CRITICAL,
                    f"{table} canonical identity is duplicated", row)
            seen.add(row.get(identity))
    costs = silver.get("product_costs", ())
    for row in costs:
        if row["unit_cogs"] < 0:
            add("negative_cogs", Severity.CRITICAL, "Product COGS cannot be negative", row)
        if row.get("valid_to") and row["valid_to"] < row["valid_from"]:
            add("invalid_cost_effective_range", Severity.CRITICAL,
                "Product cost valid_to precedes valid_from", row)
    for index, left in enumerate(costs):
        for right in costs[index + 1:]:
            same_subject = all(left.get(name) == right.get(name) for name in
                               ("business_id", "source_id", "product_id", "variant_id", "sku", "currency"))
            left_end, right_end = left.get("valid_to"), right.get("valid_to")
            overlaps = ((left_end is None or right["valid_from"] <= left_end)
                        and (right_end is None or left["valid_from"] <= right_end))
            if same_subject and overlaps and left["external_record_id"] != right["external_record_id"]:
                add("overlapping_product_costs", Severity.WARNING,
                    "Separate product cost records have overlapping effective ranges", right)
    components = silver.get("cost_components", ())
    for row in components:
        if row["amount"] < 0:
            add("negative_variable_cost", Severity.CRITICAL, "Variable cost cannot be negative", row)
        if not any(row.get(name) for name in ("order_id","shipment_id","remittance_id",
                                              "product_id","variant_id","sku")):
            add("cost_subject_missing", Severity.CRITICAL, "Cost has no economic subject", row)
    latest_links = {}
    for row in silver.get("attribution_links", ()):
        revision_key = (row["business_id"], row["source_id"], row["provider"],
                        row["external_record_id"])
        if revision_key not in latest_links or row["revision"] > latest_links[revision_key]["revision"]:
            latest_links[revision_key] = row
    weight_by_order = {}
    for row in latest_links.values():
        if not 0 <= row["attribution_weight"] <= 1:
            add("invalid_attribution_weight", Severity.CRITICAL,
                "Attribution weight must be between zero and one", row)
        key = (row["business_id"], row["order_id"])
        weight_by_order[key] = weight_by_order.get(key, 0.0) + row["attribution_weight"]
    for (business_id, order_id), weight in weight_by_order.items():
        if weight > 1.000000001:
            add("attribution_weight_above_one", Severity.CRITICAL,
                "Attribution weights exceed one", {"business_id": business_id, "order_id": order_id})
    if gold:
        order_rows = gold.get("order_economics", ())
        for row in order_rows:
            if not row["cogs_complete"]:
                add("missing_cogs", Severity.WARNING,
                    "Contribution is incomplete because one or more lines lack COGS", row)
            if row["has_marketing_attribution"] and not row["currency_compatible"]:
                add("marketing_commerce_currency_mismatch", Severity.WARNING,
                    "Attributed marketing and commerce currencies differ", row)
            if row["net_remitted"] > row["gross_collected"] + 1e-8:
                add("net_remitted_above_gross", Severity.CRITICAL,
                    "Net remitted exceeds attributable gross collected", row)
            if row["cash_expected"] > 0 and row["cash_collected"] > row["cash_expected"] + 1e-8:
                add("cash_collected_above_expected", Severity.WARNING,
                    "Cash collected exceeds expected COD cash", row)
            expected = row["recognized_economic_value"] - row["variable_operational_cost"]
            actual = row["contribution_before_marketing"]
            if row["cogs_complete"] and (actual is None or abs(actual - expected) > 1e-8):
                add("contribution_formula_mismatch", Severity.CRITICAL,
                    "Contribution before marketing does not reconcile", row)
        cohort_order_value = sum(row["delivered_order_value"] for row in order_rows)
        cohort_value = sum(row["delivered_order_value"]
                           for row in gold.get("business_economics_cohort", ()))
        if abs(cohort_order_value - cohort_value) > 1e-8:
            add("fanout_reconciliation_failed", Severity.CRITICAL,
                "Order and cohort delivered values do not reconcile")
    return tuple(issues)
