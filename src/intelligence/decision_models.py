"""Immutable contracts for deterministic decision-readiness assessment."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
import re
from typing import Any


class DecisionValidationError(ValueError):
    """A decision-readiness object failed its local deterministic contract."""


class DecisionReadiness(StrEnum):
    READY_FOR_HUMAN_REVIEW = "READY_FOR_HUMAN_REVIEW"
    NEEDS_MORE_EVIDENCE = "NEEDS_MORE_EVIDENCE"
    BLOCKED_BY_BOUNDARY = "BLOCKED_BY_BOUNDARY"


class DecisionClass(StrEnum):
    INVESTIGATION_DIRECTION = "INVESTIGATION_DIRECTION"
    BUSINESS_CHANGE_CONSIDERATION = "BUSINESS_CHANGE_CONSIDERATION"
    MEASUREMENT_GOVERNANCE = "MEASUREMENT_GOVERNANCE"
    FINANCIAL_DECISION = "FINANCIAL_DECISION"


class DecisionType(StrEnum):
    CONFIRMATION_INVESTIGATION_REVIEW = "CONFIRMATION_INVESTIGATION_REVIEW"
    CONFIRMATION_OPERATIONAL_CHANGE_CONSIDERATION = (
        "CONFIRMATION_OPERATIONAL_CHANGE_CONSIDERATION"
    )
    ACQUISITION_FULFILLMENT_INVESTIGATION_DIRECTION = (
        "ACQUISITION_FULFILLMENT_INVESTIGATION_DIRECTION"
    )
    CAMPAIGN_CHANGE_CONSIDERATION = "CAMPAIGN_CHANGE_CONSIDERATION"
    MEASUREMENT_RECONCILIATION_REVIEW = "MEASUREMENT_RECONCILIATION_REVIEW"
    MEASUREMENT_USE_READINESS = "MEASUREMENT_USE_READINESS"
    ACQUISITION_EFFICIENCY_INVESTIGATION_REVIEW = (
        "ACQUISITION_EFFICIENCY_INVESTIGATION_REVIEW"
    )
    ACQUISITION_EFFICIENCY_CAMPAIGN_CHANGE_CONSIDERATION = (
        "ACQUISITION_EFFICIENCY_CAMPAIGN_CHANGE_CONSIDERATION"
    )
    TRUSTED_ECONOMICS_REVIEW = "TRUSTED_ECONOMICS_REVIEW"


class ReadinessReasonCode(StrEnum):
    SUFFICIENT_CURRENT_EVIDENCE = "SUFFICIENT_CURRENT_EVIDENCE"
    MISSING_REQUIRED_EVIDENCE = "MISSING_REQUIRED_EVIDENCE"
    OPEN_CRITICAL_INVESTIGATION = "OPEN_CRITICAL_INVESTIGATION"
    COUNTER_EVIDENCE_PRESENT = "COUNTER_EVIDENCE_PRESENT"
    DATA_QUALITY_LIMITATION = "DATA_QUALITY_LIMITATION"
    CAUSAL_UNCERTAINTY = "CAUSAL_UNCERTAINTY"
    MEASUREMENT_UNRESOLVED = "MEASUREMENT_UNRESOLVED"
    ECONOMIC_BOUNDARY = "ECONOMIC_BOUNDARY"
    HUMAN_REVIEW_REQUIRED = "HUMAN_REVIEW_REQUIRED"


_DECISION_ID = re.compile(
    r"^decision-readiness:[a-z0-9][a-z0-9_-]*:"
    r"[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*$"
)


def valid_decision_id_shape(value: str) -> bool:
    """Return whether a value has the bounded stable decision-ID shape."""
    return isinstance(value, str) and bool(_DECISION_ID.fullmatch(value))


def _text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DecisionValidationError(f"{field} must be non-empty text")
    return value.strip()


def _text_tuple(
    value: tuple[str, ...], field: str, *, required: bool = False
) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise DecisionValidationError(f"{field} must be a tuple")
    normalized = tuple(_text(item, field) for item in value)
    if required and not normalized:
        raise DecisionValidationError(f"{field} cannot be empty")
    if len(normalized) != len(set(normalized)):
        raise DecisionValidationError(f"{field} cannot contain duplicates")
    return normalized


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", value.strip().lower()).strip("-_")
    if not slug:
        raise DecisionValidationError("Stable decision identity cannot be empty")
    return slug


def stable_decision_id(
    opportunity_type: str, opportunity_scope: str, frame_id: str
) -> str:
    """Return a stable ID for one opportunity identity and decision frame."""
    return (
        f"decision-readiness:{_slug(opportunity_type)}:"
        f"{_slug(opportunity_scope)}:{_slug(frame_id)}"
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionReadinessAssessment:
    decision_order: int
    decision_id: str
    business_id: str
    as_of_date: date
    originating_opportunity_id: str
    investigation_plan_id: str
    decision_type: DecisionType
    decision_class: DecisionClass
    decision_question: str
    readiness: DecisionReadiness
    readiness_reason_codes: tuple[ReadinessReasonCode, ...]
    rationale_summary: str
    supporting_evidence_refs: tuple[str, ...]
    counter_evidence_refs: tuple[str, ...]
    blocking_evidence_refs: tuple[str, ...]
    required_requirement_ids: tuple[str, ...]
    unresolved_requirement_ids: tuple[str, ...]
    relevant_investigation_task_ids: tuple[str, ...]
    next_evidence_task_ids: tuple[str, ...]
    decision_boundary: str
    human_review_required: bool = True
    autonomous_action_allowed: bool = False
    limitation: str = (
        "Readiness supports bounded human review only; Pulse does not choose or execute "
        "a decision."
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.decision_order, bool)
            or not isinstance(self.decision_order, int)
            or self.decision_order < 1
        ):
            raise DecisionValidationError("decision_order must be a positive integer")
        if isinstance(self.as_of_date, datetime) or not isinstance(self.as_of_date, date):
            raise DecisionValidationError("as_of_date must be a date")
        for field in (
            "decision_id", "business_id", "originating_opportunity_id",
            "investigation_plan_id", "decision_question", "rationale_summary",
            "decision_boundary", "limitation",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not _DECISION_ID.fullmatch(self.decision_id):
            raise DecisionValidationError("decision_id has an invalid stable shape")
        try:
            object.__setattr__(self, "decision_type", DecisionType(self.decision_type))
            object.__setattr__(self, "decision_class", DecisionClass(self.decision_class))
            object.__setattr__(self, "readiness", DecisionReadiness(self.readiness))
            object.__setattr__(
                self,
                "readiness_reason_codes",
                tuple(ReadinessReasonCode(item) for item in self.readiness_reason_codes),
            )
        except ValueError as exc:
            raise DecisionValidationError(str(exc)) from None
        if not self.readiness_reason_codes:
            raise DecisionValidationError("readiness_reason_codes cannot be empty")
        if len(self.readiness_reason_codes) != len(set(self.readiness_reason_codes)):
            raise DecisionValidationError("readiness_reason_codes cannot contain duplicates")
        for field, required in (
            ("supporting_evidence_refs", True),
            ("counter_evidence_refs", False),
            ("blocking_evidence_refs", False),
            ("required_requirement_ids", True),
            ("unresolved_requirement_ids", False),
            ("relevant_investigation_task_ids", True),
            ("next_evidence_task_ids", False),
        ):
            object.__setattr__(
                self, field, _text_tuple(getattr(self, field), field, required=required)
            )
        evidence_buckets = (
            set(self.supporting_evidence_refs),
            set(self.counter_evidence_refs),
            set(self.blocking_evidence_refs),
        )
        if any(
            evidence_buckets[left] & evidence_buckets[right]
            for left, right in ((0, 1), (0, 2), (1, 2))
        ):
            raise DecisionValidationError("Evidence buckets must be disjoint")
        if set(self.unresolved_requirement_ids) - set(self.required_requirement_ids):
            raise DecisionValidationError("Unresolved requirements must be required")
        if set(self.next_evidence_task_ids) - set(self.relevant_investigation_task_ids):
            raise DecisionValidationError("Next evidence tasks must be relevant tasks")
        if self.human_review_required is not True:
            raise DecisionValidationError("human_review_required must always be true")
        if self.autonomous_action_allowed is not False:
            raise DecisionValidationError("autonomous_action_allowed must always be false")
        if ReadinessReasonCode.HUMAN_REVIEW_REQUIRED not in self.readiness_reason_codes:
            raise DecisionValidationError("Every assessment must preserve human review")

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_order": self.decision_order,
            "decision_id": self.decision_id,
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "originating_opportunity_id": self.originating_opportunity_id,
            "investigation_plan_id": self.investigation_plan_id,
            "decision_type": self.decision_type.value,
            "decision_class": self.decision_class.value,
            "decision_question": self.decision_question,
            "readiness": self.readiness.value,
            "readiness_reason_codes": [item.value for item in self.readiness_reason_codes],
            "rationale_summary": self.rationale_summary,
            "supporting_evidence_refs": list(self.supporting_evidence_refs),
            "counter_evidence_refs": list(self.counter_evidence_refs),
            "blocking_evidence_refs": list(self.blocking_evidence_refs),
            "required_requirement_ids": list(self.required_requirement_ids),
            "unresolved_requirement_ids": list(self.unresolved_requirement_ids),
            "relevant_investigation_task_ids": list(self.relevant_investigation_task_ids),
            "next_evidence_task_ids": list(self.next_evidence_task_ids),
            "decision_boundary": self.decision_boundary,
            "human_review_required": self.human_review_required,
            "autonomous_action_allowed": self.autonomous_action_allowed,
            "limitation": self.limitation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionReadinessPortfolio:
    business_id: str
    as_of_date: date
    assessments: tuple[DecisionReadinessAssessment, ...]
    ready_for_human_review_count: int
    needs_more_evidence_count: int
    blocked_by_boundary_count: int
    first_reviewable_decision_id: str | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "business_id", _text(self.business_id, "business_id"))
        if isinstance(self.as_of_date, datetime) or not isinstance(self.as_of_date, date):
            raise DecisionValidationError("as_of_date must be a date")
        if not isinstance(self.assessments, tuple) or not all(
            isinstance(item, DecisionReadinessAssessment) for item in self.assessments
        ):
            raise DecisionValidationError("assessments must be a typed tuple")
        for field in (
            "ready_for_human_review_count", "needs_more_evidence_count",
            "blocked_by_boundary_count",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise DecisionValidationError(f"{field} must be a non-negative integer")
        if self.first_reviewable_decision_id is not None:
            object.__setattr__(
                self,
                "first_reviewable_decision_id",
                _text(self.first_reviewable_decision_id, "first_reviewable_decision_id"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "assessments": [item.to_dict() for item in self.assessments],
            "ready_for_human_review_count": self.ready_for_human_review_count,
            "needs_more_evidence_count": self.needs_more_evidence_count,
            "blocked_by_boundary_count": self.blocked_by_boundary_count,
            "first_reviewable_decision_id": self.first_reviewable_decision_id,
        }
