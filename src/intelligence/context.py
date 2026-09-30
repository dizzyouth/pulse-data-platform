"""Build a bounded, aggregate-only evidence context from fixed warehouse reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any, Callable, Mapping, Protocol, Sequence

import psycopg
from psycopg.rows import dict_row

from src.intelligence.models import DecisionSignal
from src.warehouse.load_gold import connection_kwargs


PILOT_ANOMALY_METRICS = (
    "daily_target_spend",
    "lightfunnels_order_volume",
    "confirmed_order_volume",
    "delivered_order_volume",
    "returned_order_volume",
)

CURATED_RELATIONS = frozenset({
    "marts.sama_pilot_intelligence_signals",
    "marts.sama_pilot_business_leakage",
    "marts.sama_pilot_campaign_diagnostics",
    "marts.sama_pilot_unified_overview",
    "marts.sama_pilot_unified_native_economics",
    "monitoring_views.anomaly_baseline_history",
})

SIGNAL_SQL = """
SELECT signal_order, signal_id, business_id, as_of_date, scope_type, scope_id,
       scope_name, signal_type, signal_category, priority, confidence, metric_name,
       observed_value, baseline_value, absolute_gap, relative_gap, sample_size,
       impact_order_count, evidence_summary, why_it_matters,
       recommended_next_step, limitation, causal_claim
FROM marts.sama_pilot_intelligence_signals
WHERE business_id = %s
ORDER BY signal_order
"""

LEAKAGE_SQL = """
SELECT business_id, as_of_date, leakage_stage, category, observed_count,
       denominator_count, observed_rate, interpretation, is_measurement_gap,
       is_operational_gap, stage_order
FROM marts.sama_pilot_business_leakage
WHERE business_id = %s
ORDER BY stage_order
"""

CAMPAIGN_SQL = """
SELECT business_id, campaign_name, spend_usd, lightfunnels_orders, matched_orders,
       confirmed_orders, shipped_orders, delivered_orders, returned_orders,
       confirmation_rate, peer_confirmation_rate, delivery_rate, peer_delivery_rate,
       return_rate, peer_return_rate, cost_per_lightfunnels_order_usd,
       peer_cost_per_lightfunnels_order_usd, cost_per_delivered_order_usd,
       peer_cost_per_delivered_order_usd, delivery_benchmark_gap_orders,
       excess_returns_vs_peer, confirmation_sample_band, fulfillment_sample_band,
       acquisition_sample_band, cost_delivered_sample_band, sample_band,
       benchmark_interpretation
FROM marts.sama_pilot_campaign_diagnostics
WHERE business_id = %s
ORDER BY campaign_name
"""

ANOMALY_SQL = """
SELECT DISTINCT ON (metric_name)
       metric_name, status, severity, observed_at_utc, observed_value, expected_value,
       lower_bound, upper_bound, baseline_strategy, confidence, explanation
FROM monitoring_views.anomaly_baseline_history
WHERE business_id = %s AND metric_name = ANY(%s)
ORDER BY metric_name, observed_at_utc DESC, evaluated_at_utc DESC, evaluation_id DESC
"""

ECONOMICS_SQL = """
SELECT business_id, economic_status, currency
FROM marts.sama_pilot_unified_native_economics
WHERE business_id = %s
ORDER BY currency
"""

AS_OF_SQL = """
SELECT business_id, period_end AS as_of_date
FROM marts.sama_pilot_unified_overview
WHERE business_id = %s
"""

_FORBIDDEN_CONTEXT_KEYS = frozenset({
    "customer", "customer_id", "phone", "phone_hash", "email", "address",
    "tracking_number", "order_id", "order_identifier", "source_filename",
    "source_file", "file_path", "raw_record",
})


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", value.strip().lower()).strip("-_")
    if not slug:
        raise ValueError("Evidence identity cannot be empty")
    return slug


def _json_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError(f"Unsupported evidence value type: {type(value).__name__}")


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceItem:
    evidence_id: str
    evidence_type: str
    business_id: str
    scope_type: str
    scope_name: str
    title: str
    facts: Mapping[str, Any]
    limitation: str
    source_relation: str

    def __post_init__(self) -> None:
        for field in (
            "evidence_id", "evidence_type", "business_id", "scope_type", "scope_name",
            "title", "limitation", "source_relation",
        ):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be non-empty text")
        if self.source_relation not in CURATED_RELATIONS:
            raise ValueError("Evidence source must be an approved aggregate relation")
        if self.scope_type not in {"BUSINESS", "CAMPAIGN", "METRIC"}:
            raise ValueError("Unsupported evidence scope")
        normalized: dict[str, Any] = {}
        for key, value in self.facts.items():
            if not isinstance(key, str) or key.lower() in _FORBIDDEN_CONTEXT_KEYS:
                raise ValueError(f"Forbidden or invalid evidence field: {key}")
            normalized[key] = _json_scalar(value)
        object.__setattr__(self, "facts", normalized)

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence_id": self.evidence_id,
            "evidence_type": self.evidence_type,
            "business_id": self.business_id,
            "scope_type": self.scope_type,
            "scope_name": self.scope_name,
            "title": self.title,
            "facts": dict(self.facts),
            "limitation": self.limitation,
            "source_relation": self.source_relation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class IntelligenceContext:
    business_id: str
    as_of_date: date
    signals: tuple[DecisionSignal, ...]
    leakage: tuple[EvidenceItem, ...]
    campaign_diagnostics: tuple[EvidenceItem, ...]
    time_anomalies: tuple[EvidenceItem, ...]
    economic_status: EvidenceItem
    evidence_items: tuple[EvidenceItem, ...]

    def __post_init__(self) -> None:
        if not self.business_id.strip():
            raise ValueError("business_id cannot be empty")
        expected = (*self.leakage, *self.campaign_diagnostics, *self.time_anomalies,
                    self.economic_status)
        signal_count = len(self.signals)
        if len(self.evidence_items) != signal_count + len(expected):
            raise ValueError("evidence_items must contain every context item exactly once")
        ids = [item.evidence_id for item in self.evidence_items]
        if len(ids) != len(set(ids)):
            raise ValueError("Evidence IDs must be unique")
        if any(item.business_id != self.business_id for item in self.evidence_items):
            raise ValueError("Evidence cannot cross business boundaries")

    @property
    def evidence_by_id(self) -> dict[str, EvidenceItem]:
        return {item.evidence_id: item for item in self.evidence_items}

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "evidence_items": [item.to_dict() for item in self.evidence_items],
        }


class EvidenceRepository(Protocol):
    def fetch_as_of(self, business_id: str) -> Mapping[str, Any] | None: ...
    def fetch_signals(self, business_id: str) -> Sequence[Mapping[str, Any]]: ...
    def fetch_leakage(self, business_id: str) -> Sequence[Mapping[str, Any]]: ...
    def fetch_campaigns(self, business_id: str) -> Sequence[Mapping[str, Any]]: ...
    def fetch_anomalies(self, business_id: str) -> Sequence[Mapping[str, Any]]: ...
    def fetch_economics(self, business_id: str) -> Sequence[Mapping[str, Any]]: ...


class PostgresEvidenceRepository:
    """Read-only adapter. All SQL is fixed here; callers can only bind a business ID."""

    def __init__(self, connect: Callable[..., Any] = psycopg.connect) -> None:
        self._connect = connect

    def _one(self, query: str, params: tuple[Any, ...]) -> Mapping[str, Any] | None:
        rows = self._many(query, params)
        return rows[0] if rows else None

    def _many(self, query: str, params: tuple[Any, ...]) -> list[Mapping[str, Any]]:
        with self._connect(**connection_kwargs(), row_factory=dict_row) as connection:
            connection.read_only = True
            return list(connection.execute(query, params).fetchall())

    def fetch_as_of(self, business_id: str) -> Mapping[str, Any] | None:
        return self._one(AS_OF_SQL, (business_id,))

    def fetch_signals(self, business_id: str) -> Sequence[Mapping[str, Any]]:
        return self._many(SIGNAL_SQL, (business_id,))

    def fetch_leakage(self, business_id: str) -> Sequence[Mapping[str, Any]]:
        return self._many(LEAKAGE_SQL, (business_id,))

    def fetch_campaigns(self, business_id: str) -> Sequence[Mapping[str, Any]]:
        return self._many(CAMPAIGN_SQL, (business_id,))

    def fetch_anomalies(self, business_id: str) -> Sequence[Mapping[str, Any]]:
        return self._many(ANOMALY_SQL, (business_id, list(PILOT_ANOMALY_METRICS)))

    def fetch_economics(self, business_id: str) -> Sequence[Mapping[str, Any]]:
        return self._many(ECONOMICS_SQL, (business_id,))


def _signal_item(signal: DecisionSignal, order: int) -> EvidenceItem:
    suffix = _slug(signal.signal_type)
    if signal.scope_type == "CAMPAIGN":
        suffix = f"{suffix}:{_slug(signal.scope_name)}"
    return EvidenceItem(
        evidence_id=f"signal:{suffix}",
        evidence_type="SIGNAL",
        business_id=signal.business_id,
        scope_type=signal.scope_type,
        scope_name=signal.scope_name,
        title=signal.signal_type.replace("_", " ").title(),
        facts={
            "signal_order": order,
            "signal_type": signal.signal_type,
            "signal_category": signal.signal_category,
            "priority": signal.priority,
            "confidence": signal.confidence,
            "metric_name": signal.metric_name,
            "observed_value": signal.observed_value,
            "baseline_value": signal.baseline_value,
            "absolute_gap": signal.absolute_gap,
            "relative_gap": signal.relative_gap,
            "sample_size": signal.sample_size,
            "impact_order_count": signal.impact_order_count,
            "evidence_summary": signal.evidence_summary,
            "why_it_matters": signal.why_it_matters,
            "recommended_next_step": signal.recommended_next_step,
            "causal_claim": signal.causal_claim,
        },
        limitation=signal.limitation,
        source_relation="marts.sama_pilot_intelligence_signals",
    )


def build_context(
    business_id: str, repository: EvidenceRepository | None = None
) -> IntelligenceContext:
    """Build the complete bounded context without exposing arbitrary query access."""
    if not isinstance(business_id, str) or not business_id.strip():
        raise ValueError("business_id is required")
    repo = repository or PostgresEvidenceRepository()
    as_of_row = repo.fetch_as_of(business_id)
    if not as_of_row:
        raise LookupError(f"No curated intelligence context for business {business_id!r}")
    as_of_date = as_of_row["as_of_date"]
    if isinstance(as_of_date, datetime):
        as_of_date = as_of_date.date()
    if not isinstance(as_of_date, date):
        raise ValueError("Warehouse as_of_date is invalid")

    signal_rows = list(repo.fetch_signals(business_id))
    signals = tuple(DecisionSignal.from_row(row) for row in signal_rows)
    signal_items = tuple(
        _signal_item(signal, int(row["signal_order"]))
        for signal, row in zip(signals, signal_rows, strict=True)
    )

    leakage = tuple(EvidenceItem(
        evidence_id=f"leakage:{_slug(row['leakage_stage'])}",
        evidence_type="LEAKAGE",
        business_id=row["business_id"], scope_type="BUSINESS", scope_name=business_id,
        title=str(row["leakage_stage"]).replace("_", " ").title(),
        facts={key: row[key] for key in (
            "leakage_stage", "category", "observed_count", "denominator_count",
            "observed_rate", "interpretation", "is_measurement_gap",
            "is_operational_gap", "stage_order",
        )},
        limitation=(
            "This is an observed stage difference. It is not proof of cause, lost revenue, "
            "fraud, or a guaranteed recoverable opportunity."
        ),
        source_relation="marts.sama_pilot_business_leakage",
    ) for row in repo.fetch_leakage(business_id))

    campaigns = tuple(EvidenceItem(
        evidence_id=f"campaign:{_slug(row['campaign_name'])}",
        evidence_type="CAMPAIGN_DIAGNOSTIC",
        business_id=row["business_id"], scope_type="CAMPAIGN",
        scope_name=row["campaign_name"], title=f"Campaign {row['campaign_name']}",
        facts={key: row[key] for key in (
            "campaign_name", "spend_usd", "lightfunnels_orders", "matched_orders",
            "confirmed_orders", "shipped_orders", "delivered_orders", "returned_orders",
            "confirmation_rate", "peer_confirmation_rate", "delivery_rate",
            "peer_delivery_rate", "return_rate", "peer_return_rate",
            "cost_per_lightfunnels_order_usd", "peer_cost_per_lightfunnels_order_usd",
            "cost_per_delivered_order_usd", "peer_cost_per_delivered_order_usd",
            "delivery_benchmark_gap_orders", "excess_returns_vs_peer",
            "confirmation_sample_band", "fulfillment_sample_band",
            "acquisition_sample_band", "cost_delivered_sample_band", "sample_band",
            "benchmark_interpretation",
        )},
        limitation=(
            "Peer differences are observational and cannot establish a causal reason, "
            "forecast lift, or authorize a budget action."
        ),
        source_relation="marts.sama_pilot_campaign_diagnostics",
    ) for row in repo.fetch_campaigns(business_id))

    anomaly_by_metric = {row["metric_name"]: row for row in repo.fetch_anomalies(business_id)}
    anomalies = tuple(EvidenceItem(
        evidence_id=f"anomaly:{metric}", evidence_type="TIME_ANOMALY",
        business_id=business_id, scope_type="METRIC", scope_name=metric,
        title=f"Latest anomaly state: {metric}",
        facts=(
            {key: anomaly_by_metric[metric][key] for key in (
                "metric_name", "status", "severity", "observed_at_utc", "observed_value",
                "expected_value", "lower_bound", "upper_bound", "baseline_strategy",
                "confidence", "explanation",
            )}
            if metric in anomaly_by_metric else
            {"metric_name": metric, "status": "NOT_EVALUATED"}
        ),
        limitation=(
            "This is the latest persisted time-series evaluation; it does not deactivate "
            "structural business signals."
        ),
        source_relation="monitoring_views.anomaly_baseline_history",
    ) for metric in PILOT_ANOMALY_METRICS)

    economics_rows = list(repo.fetch_economics(business_id))
    statuses = sorted({str(row["economic_status"]) for row in economics_rows})
    currencies = sorted({str(row["currency"]) for row in economics_rows})
    status = "FX_REQUIRED" if "FX_REQUIRED" in statuses else (
        statuses[0] if len(statuses) == 1 else "SEPARATE_CURRENCIES"
    )
    economics = EvidenceItem(
        evidence_id=f"economics:{_slug(status)}", evidence_type="ECONOMIC_STATUS",
        business_id=business_id, scope_type="BUSINESS", scope_name=business_id,
        title=f"Economic safety: {status}",
        facts={"economic_status": status, "native_currencies": ", ".join(currencies)},
        limitation=(
            "Native COD revenue cannot be combined with USD costs into trusted business "
            "profit, contribution, margin, MER, or ROAS without trusted FX."
        ),
        source_relation="marts.sama_pilot_unified_native_economics",
    )

    items = (*signal_items, *leakage, *campaigns, *anomalies, economics)
    return IntelligenceContext(
        business_id=business_id, as_of_date=as_of_date, signals=signals,
        leakage=leakage, campaign_diagnostics=campaigns, time_anomalies=anomalies,
        economic_status=economics, evidence_items=items,
    )
