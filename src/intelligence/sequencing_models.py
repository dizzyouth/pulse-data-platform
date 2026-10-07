"""Immutable contracts for evidence leverage and investigation sequencing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
import re
from typing import Any

from src.intelligence.investigation_models import (
    EvidenceRequirementStatus,
    InvestigationReadiness,
    InvestigationTaskKind,
)
from src.intelligence.opportunity_models import OpportunityPriority


class SequencingValidationError(ValueError):
    """An evidence-leverage or sequencing object failed closed validation."""


class SequencingPortfolioState(StrEnum):
    READY_TASK_AVAILABLE = "READY_TASK_AVAILABLE"
    EVIDENCE_GAP_FIRST = "EVIDENCE_GAP_FIRST"
    BOUNDARY_ONLY = "BOUNDARY_ONLY"
    NO_OPEN_READINESS_GAPS = "NO_OPEN_READINESS_GAPS"


_REQUIREMENT_ID = re.compile(r"^requirement:[a-z0-9][a-z0-9_:-]*$")
_DECISION_ID = re.compile(
    r"^decision-readiness:[a-z0-9][a-z0-9_-]*:"
    r"[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*$"
)
_OPPORTUNITY_ID = re.compile(r"^opportunity:[a-z0-9_-]+:[a-z0-9_-]+$")
_TASK_ID = re.compile(
    r"^investigation-task:[a-z0-9][a-z0-9_-]*:"
    r"[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*$"
)
_PLAN_ID = re.compile(
    r"^investigation-plan:[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*$"
)
_GAP_ID = re.compile(r"^evidence-gap:[a-z0-9][a-z0-9_-]*$")


def _text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SequencingValidationError(f"{field} must be non-empty text")
    return value.strip()


def _typed_tuple(value: tuple[Any, ...], item_type: type, field: str) -> tuple[Any, ...]:
    if not isinstance(value, tuple) or not all(isinstance(item, item_type) for item in value):
        raise SequencingValidationError(f"{field} must be a typed tuple")
    return value


def _id_tuple(
    value: tuple[str, ...],
    field: str,
    pattern: re.Pattern[str],
    *,
    required: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise SequencingValidationError(f"{field} must be a tuple")
    normalized = tuple(_text(item, field) for item in value)
    if required and not normalized:
        raise SequencingValidationError(f"{field} cannot be empty")
    if len(normalized) != len(set(normalized)):
        raise SequencingValidationError(f"{field} cannot contain duplicates")
    if any(not pattern.fullmatch(item) for item in normalized):
        raise SequencingValidationError(f"{field} contains an invalid stable ID")
    return normalized


def _count(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SequencingValidationError(f"{field} must be a non-negative integer")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceLeverageItem:
    requirement_id: str
    requirement_name: str
    description: str
    requirement_status: EvidenceRequirementStatus
    affected_decision_ids: tuple[str, ...]
    affected_opportunity_ids: tuple[str, ...]
    related_task_ids: tuple[str, ...]
    affected_decision_count: int
    affected_opportunity_count: int
    highest_opportunity_priority: OpportunityPriority
    shared_across_decisions: bool
    shared_across_opportunities: bool
    existing_gap_id: str | None
    limitation: str

    def __post_init__(self) -> None:
        for field in ("requirement_id", "requirement_name", "description", "limitation"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not _REQUIREMENT_ID.fullmatch(self.requirement_id):
            raise SequencingValidationError("requirement_id has an invalid stable shape")
        try:
            object.__setattr__(
                self, "requirement_status", EvidenceRequirementStatus(self.requirement_status)
            )
            object.__setattr__(
                self,
                "highest_opportunity_priority",
                OpportunityPriority(self.highest_opportunity_priority),
            )
        except ValueError as exc:
            raise SequencingValidationError(str(exc)) from None
        object.__setattr__(
            self,
            "affected_decision_ids",
            _id_tuple(
                self.affected_decision_ids,
                "affected_decision_ids",
                _DECISION_ID,
                required=True,
            ),
        )
        object.__setattr__(
            self,
            "affected_opportunity_ids",
            _id_tuple(
                self.affected_opportunity_ids,
                "affected_opportunity_ids",
                _OPPORTUNITY_ID,
                required=True,
            ),
        )
        object.__setattr__(
            self,
            "related_task_ids",
            _id_tuple(self.related_task_ids, "related_task_ids", _TASK_ID),
        )
        object.__setattr__(
            self,
            "affected_decision_count",
            _count(self.affected_decision_count, "affected_decision_count"),
        )
        object.__setattr__(
            self,
            "affected_opportunity_count",
            _count(self.affected_opportunity_count, "affected_opportunity_count"),
        )
        if self.affected_decision_count != len(self.affected_decision_ids):
            raise SequencingValidationError("affected_decision_count is inconsistent")
        if self.affected_opportunity_count != len(self.affected_opportunity_ids):
            raise SequencingValidationError("affected_opportunity_count is inconsistent")
        if self.shared_across_decisions is not (self.affected_decision_count > 1):
            raise SequencingValidationError("shared_across_decisions is inconsistent")
        if self.shared_across_opportunities is not (self.affected_opportunity_count > 1):
            raise SequencingValidationError("shared_across_opportunities is inconsistent")
        if self.existing_gap_id is not None:
            object.__setattr__(
                self, "existing_gap_id", _text(self.existing_gap_id, "existing_gap_id")
            )
            if not _GAP_ID.fullmatch(self.existing_gap_id):
                raise SequencingValidationError("existing_gap_id has an invalid stable shape")

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_id": self.requirement_id,
            "requirement_name": self.requirement_name,
            "description": self.description,
            "requirement_status": self.requirement_status.value,
            "affected_decision_ids": list(self.affected_decision_ids),
            "affected_opportunity_ids": list(self.affected_opportunity_ids),
            "related_task_ids": list(self.related_task_ids),
            "affected_decision_count": self.affected_decision_count,
            "affected_opportunity_count": self.affected_opportunity_count,
            "highest_opportunity_priority": self.highest_opportunity_priority.value,
            "shared_across_decisions": self.shared_across_decisions,
            "shared_across_opportunities": self.shared_across_opportunities,
            "existing_gap_id": self.existing_gap_id,
            "limitation": self.limitation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationSequenceItem:
    sequence_order: int
    task_id: str
    investigation_plan_id: str
    opportunity_id: str
    task_title: str
    task_kind: InvestigationTaskKind
    task_readiness: InvestigationReadiness
    affected_decision_ids: tuple[str, ...]
    addressed_requirement_ids: tuple[str, ...]
    affected_decision_count: int
    opportunity_priority: OpportunityPriority
    opportunity_order: int
    can_begin_now: bool
    sequencing_reason: str
    limitation: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.sequence_order, bool)
            or not isinstance(self.sequence_order, int)
            or self.sequence_order < 1
        ):
            raise SequencingValidationError("sequence_order must be a positive integer")
        if (
            isinstance(self.opportunity_order, bool)
            or not isinstance(self.opportunity_order, int)
            or self.opportunity_order < 1
        ):
            raise SequencingValidationError("opportunity_order must be a positive integer")
        for field, pattern in (
            ("task_id", _TASK_ID),
            ("investigation_plan_id", _PLAN_ID),
            ("opportunity_id", _OPPORTUNITY_ID),
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
            if not pattern.fullmatch(getattr(self, field)):
                raise SequencingValidationError(f"{field} has an invalid stable shape")
        for field in ("task_title", "sequencing_reason", "limitation"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        try:
            object.__setattr__(self, "task_kind", InvestigationTaskKind(self.task_kind))
            object.__setattr__(
                self, "task_readiness", InvestigationReadiness(self.task_readiness)
            )
            object.__setattr__(
                self, "opportunity_priority", OpportunityPriority(self.opportunity_priority)
            )
        except ValueError as exc:
            raise SequencingValidationError(str(exc)) from None
        object.__setattr__(
            self,
            "affected_decision_ids",
            _id_tuple(
                self.affected_decision_ids,
                "affected_decision_ids",
                _DECISION_ID,
                required=True,
            ),
        )
        object.__setattr__(
            self,
            "addressed_requirement_ids",
            _id_tuple(
                self.addressed_requirement_ids,
                "addressed_requirement_ids",
                _REQUIREMENT_ID,
            ),
        )
        object.__setattr__(
            self,
            "affected_decision_count",
            _count(self.affected_decision_count, "affected_decision_count"),
        )
        if self.affected_decision_count != len(self.affected_decision_ids):
            raise SequencingValidationError("affected_decision_count is inconsistent")
        if not isinstance(self.can_begin_now, bool):
            raise SequencingValidationError("can_begin_now must be boolean")
        if self.can_begin_now is not (
            self.task_readiness is InvestigationReadiness.READY_NOW
        ):
            raise SequencingValidationError(
                "can_begin_now must derive only from READY_NOW task readiness"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence_order": self.sequence_order,
            "task_id": self.task_id,
            "investigation_plan_id": self.investigation_plan_id,
            "opportunity_id": self.opportunity_id,
            "task_title": self.task_title,
            "task_kind": self.task_kind.value,
            "task_readiness": self.task_readiness.value,
            "affected_decision_ids": list(self.affected_decision_ids),
            "addressed_requirement_ids": list(self.addressed_requirement_ids),
            "affected_decision_count": self.affected_decision_count,
            "opportunity_priority": self.opportunity_priority.value,
            "opportunity_order": self.opportunity_order,
            "can_begin_now": self.can_begin_now,
            "sequencing_reason": self.sequencing_reason,
            "limitation": self.limitation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationSequencingPortfolio:
    business_id: str
    as_of_date: date
    state: SequencingPortfolioState
    evidence_leverage_items: tuple[EvidenceLeverageItem, ...]
    sequence_items: tuple[InvestigationSequenceItem, ...]
    top_evidence_focus_requirement_id: str | None
    recommended_next_task_id: str | None
    startable_task_count: int
    blocked_sequence_task_count: int
    targeted_needs_more_evidence_decision_ids: tuple[str, ...]
    boundary_blocked_decision_ids: tuple[str, ...]
    limitation: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "business_id", _text(self.business_id, "business_id"))
        object.__setattr__(self, "limitation", _text(self.limitation, "limitation"))
        if isinstance(self.as_of_date, datetime) or not isinstance(self.as_of_date, date):
            raise SequencingValidationError("as_of_date must be a date")
        try:
            object.__setattr__(self, "state", SequencingPortfolioState(self.state))
        except ValueError as exc:
            raise SequencingValidationError(str(exc)) from None
        object.__setattr__(
            self,
            "evidence_leverage_items",
            _typed_tuple(
                self.evidence_leverage_items,
                EvidenceLeverageItem,
                "evidence_leverage_items",
            ),
        )
        object.__setattr__(
            self,
            "sequence_items",
            _typed_tuple(
                self.sequence_items,
                InvestigationSequenceItem,
                "sequence_items",
            ),
        )
        object.__setattr__(
            self,
            "targeted_needs_more_evidence_decision_ids",
            _id_tuple(
                self.targeted_needs_more_evidence_decision_ids,
                "targeted_needs_more_evidence_decision_ids",
                _DECISION_ID,
            ),
        )
        object.__setattr__(
            self,
            "boundary_blocked_decision_ids",
            _id_tuple(
                self.boundary_blocked_decision_ids,
                "boundary_blocked_decision_ids",
                _DECISION_ID,
            ),
        )
        object.__setattr__(
            self, "startable_task_count", _count(self.startable_task_count, "startable_task_count")
        )
        object.__setattr__(
            self,
            "blocked_sequence_task_count",
            _count(self.blocked_sequence_task_count, "blocked_sequence_task_count"),
        )
        if [item.sequence_order for item in self.sequence_items] != list(
            range(1, len(self.sequence_items) + 1)
        ):
            raise SequencingValidationError("sequence orders must be unique and contiguous")
        requirement_ids = [item.requirement_id for item in self.evidence_leverage_items]
        if len(requirement_ids) != len(set(requirement_ids)):
            raise SequencingValidationError("evidence leverage requirements cannot duplicate")
        task_ids = [item.task_id for item in self.sequence_items]
        if len(task_ids) != len(set(task_ids)):
            raise SequencingValidationError("sequence tasks cannot duplicate")
        if self.top_evidence_focus_requirement_id is not None:
            object.__setattr__(
                self,
                "top_evidence_focus_requirement_id",
                _text(
                    self.top_evidence_focus_requirement_id,
                    "top_evidence_focus_requirement_id",
                ),
            )
            if self.top_evidence_focus_requirement_id not in requirement_ids:
                raise SequencingValidationError(
                    "top evidence focus must reference a leverage item"
                )
        if self.recommended_next_task_id is not None:
            object.__setattr__(
                self,
                "recommended_next_task_id",
                _text(self.recommended_next_task_id, "recommended_next_task_id"),
            )
            selected = next(
                (
                    item
                    for item in self.sequence_items
                    if item.task_id == self.recommended_next_task_id
                ),
                None,
            )
            if selected is None or not selected.can_begin_now:
                raise SequencingValidationError(
                    "recommended task must be a startable sequence item"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "state": self.state.value,
            "evidence_leverage_items": [
                item.to_dict() for item in self.evidence_leverage_items
            ],
            "sequence_items": [item.to_dict() for item in self.sequence_items],
            "top_evidence_focus_requirement_id": self.top_evidence_focus_requirement_id,
            "recommended_next_task_id": self.recommended_next_task_id,
            "startable_task_count": self.startable_task_count,
            "blocked_sequence_task_count": self.blocked_sequence_task_count,
            "targeted_needs_more_evidence_decision_ids": list(
                self.targeted_needs_more_evidence_decision_ids
            ),
            "boundary_blocked_decision_ids": list(self.boundary_blocked_decision_ids),
            "limitation": self.limitation,
        }
