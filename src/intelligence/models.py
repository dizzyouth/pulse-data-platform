"""Provider-neutral decision-signal contract for deterministic diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionSignal:
    signal_id: str
    business_id: str
    as_of_date: date
    scope_type: str
    scope_id: str
    scope_name: str
    signal_type: str
    signal_category: str
    priority: str
    confidence: str
    metric_name: str
    observed_value: float
    baseline_value: float
    absolute_gap: float
    relative_gap: float | None
    sample_size: int
    impact_order_count: float | None
    evidence_summary: str
    why_it_matters: str
    recommended_next_step: str
    limitation: str
    causal_claim: bool = False

    def __post_init__(self) -> None:
        if self.scope_type not in {"BUSINESS", "CAMPAIGN"}:
            raise ValueError("scope_type must be BUSINESS or CAMPAIGN")
        if self.priority not in {"HIGH", "MEDIUM", "LOW"}:
            raise ValueError("priority must be HIGH, MEDIUM, or LOW")
        if self.confidence not in {"HIGH", "MEDIUM", "LOW"}:
            raise ValueError("confidence must be HIGH, MEDIUM, or LOW")
        if self.causal_claim:
            raise ValueError("Phase 6.6A signals cannot make causal claims")
        required = (
            self.signal_id, self.business_id, self.scope_id, self.scope_name,
            self.signal_type, self.signal_category, self.metric_name,
            self.evidence_summary, self.why_it_matters,
            self.recommended_next_step, self.limitation,
        )
        if any(not value.strip() for value in required):
            raise ValueError("Decision signal text fields cannot be blank")
        if self.sample_size < 0 or self.impact_order_count is not None and self.impact_order_count < 0:
            raise ValueError("Decision signal counts cannot be negative")
        if self.confidence == "LOW" and self.priority == "HIGH":
            raise ValueError("LOW sample evidence cannot produce HIGH priority")

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "DecisionSignal":
        return cls(**{field: row[field] for field in cls.__dataclass_fields__})
