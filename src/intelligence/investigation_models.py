"""Immutable contracts for deterministic investigation planning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
import re
from typing import Any

from src.intelligence.opportunity_models import (
    OpportunityConfidence,
    OpportunityPriority,
)


class InvestigationValidationError(ValueError):
    """An investigation plan failed its local deterministic contract."""


class InvestigationTaskKind(StrEnum):
    VALIDATE_EXISTING_EVIDENCE = "VALIDATE_EXISTING_EVIDENCE"
    RESOLVE_DATA_QUALITY = "RESOLVE_DATA_QUALITY"
    COMPARE_COHORTS = "COMPARE_COHORTS"
    RECONCILE_MEASUREMENT = "RECONCILE_MEASUREMENT"
    COLLECT_MISSING_EVIDENCE = "COLLECT_MISSING_EVIDENCE"


class InvestigationReadiness(StrEnum):
    READY_NOW = "READY_NOW"
    PARTIALLY_READY = "PARTIALLY_READY"
    BLOCKED_MISSING_EVIDENCE = "BLOCKED_MISSING_EVIDENCE"
    BLOCKED_BOUNDARY = "BLOCKED_BOUNDARY"


class PlanStatus(StrEnum):
    READY = "READY"
    PARTIAL = "PARTIAL"
    BLOCKED = "BLOCKED"


class EvidenceRequirementStatus(StrEnum):
    AVAILABLE = "AVAILABLE"
    MISSING_FROM_CONTEXT = "MISSING_FROM_CONTEXT"
    PARTIAL = "PARTIAL"
    BLOCKED_BY_BOUNDARY = "BLOCKED_BY_BOUNDARY"


_REQUIREMENT_ID = re.compile(r"^requirement:[a-z0-9][a-z0-9_:-]*$")
_TASK_ID = re.compile(
    r"^investigation-task:[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*$"
)
_PLAN_ID = re.compile(
    r"^investigation-plan:[a-z0-9][a-z0-9_-]*:[a-z0-9][a-z0-9_-]*$"
)
_GAP_ID = re.compile(r"^evidence-gap:[a-z0-9][a-z0-9_-]*$")


def _text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvestigationValidationError(f"{field} must be non-empty text")
    return value.strip()


def _tuple(value: tuple[str, ...], field: str, *, required: bool = False) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise InvestigationValidationError(f"{field} must be a tuple")
    normalized = tuple(_text(item, field) for item in value)
    if required and not normalized:
        raise InvestigationValidationError(f"{field} cannot be empty")
    if len(normalized) != len(set(normalized)):
        raise InvestigationValidationError(f"{field} cannot contain duplicates")
    return normalized


def _positive_order(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InvestigationValidationError(f"{field} must be a positive integer")
    return value


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", value.strip().lower()).strip("-_")
    if not slug:
        raise InvestigationValidationError("Stable identity cannot be empty")
    return slug


def stable_task_id(opportunity_type: str, scope_name: str, task_key: str) -> str:
    return (
        f"investigation-task:{_slug(opportunity_type)}:"
        f"{_slug(scope_name)}:{_slug(task_key)}"
    )


def stable_plan_id(opportunity_type: str, scope_name: str) -> str:
    return f"investigation-plan:{_slug(opportunity_type)}:{_slug(scope_name)}"


def stable_gap_id(requirement_id: str) -> str:
    if not _REQUIREMENT_ID.fullmatch(requirement_id):
        raise InvestigationValidationError("requirement_id has an invalid stable shape")
    return f"evidence-gap:{_slug(requirement_id.removeprefix('requirement:'))}"


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceRequirement:
    requirement_id: str
    name: str
    description: str
    status: EvidenceRequirementStatus
    evidence_refs: tuple[str, ...]
    missing_reason: str | None
    collection_hint: str
    source_scope: str
    required_for_task_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for field in ("requirement_id", "name", "description", "collection_hint", "source_scope"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not _REQUIREMENT_ID.fullmatch(self.requirement_id):
            raise InvestigationValidationError("requirement_id has an invalid stable shape")
        try:
            object.__setattr__(self, "status", EvidenceRequirementStatus(self.status))
        except ValueError as exc:
            raise InvestigationValidationError(str(exc)) from None
        object.__setattr__(self, "evidence_refs", _tuple(self.evidence_refs, "evidence_refs"))
        object.__setattr__(
            self,
            "required_for_task_ids",
            _tuple(self.required_for_task_ids, "required_for_task_ids", required=True),
        )
        if self.status is EvidenceRequirementStatus.AVAILABLE:
            if not self.evidence_refs or self.missing_reason is not None:
                raise InvestigationValidationError("Available requirements need evidence and no missing reason")
        else:
            if self.missing_reason is None:
                raise InvestigationValidationError("Unavailable requirements need a missing reason")
            object.__setattr__(self, "missing_reason", _text(self.missing_reason, "missing_reason"))
        if self.status is EvidenceRequirementStatus.MISSING_FROM_CONTEXT and self.evidence_refs:
            raise InvestigationValidationError("Missing requirements cannot claim available evidence")

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_id": self.requirement_id,
            "name": self.name,
            "description": self.description,
            "status": self.status.value,
            "evidence_refs": list(self.evidence_refs),
            "missing_reason": self.missing_reason,
            "collection_hint": self.collection_hint,
            "source_scope": self.source_scope,
            "required_for_task_ids": list(self.required_for_task_ids),
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationTask:
    task_order: int
    task_id: str
    opportunity_id: str
    task_kind: InvestigationTaskKind
    title: str
    objective: str
    readiness: InvestigationReadiness
    required_requirement_ids: tuple[str, ...]
    available_evidence_refs: tuple[str, ...]
    missing_requirement_ids: tuple[str, ...]
    expected_output: str
    strengthens_criteria: tuple[str, ...]
    weakens_criteria: tuple[str, ...]
    completion_criteria: tuple[str, ...]
    limitation: str
    autonomous_action: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "task_order", _positive_order(self.task_order, "task_order"))
        for field in ("task_id", "opportunity_id", "title", "objective", "expected_output", "limitation"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not _TASK_ID.fullmatch(self.task_id):
            raise InvestigationValidationError("task_id has an invalid stable shape")
        if not self.opportunity_id.startswith("opportunity:"):
            raise InvestigationValidationError("task opportunity_id is invalid")
        try:
            object.__setattr__(self, "task_kind", InvestigationTaskKind(self.task_kind))
            object.__setattr__(self, "readiness", InvestigationReadiness(self.readiness))
        except ValueError as exc:
            raise InvestigationValidationError(str(exc)) from None
        for field, required in (
            ("required_requirement_ids", True),
            ("available_evidence_refs", False),
            ("missing_requirement_ids", False),
            ("strengthens_criteria", False),
            ("weakens_criteria", False),
            ("completion_criteria", True),
        ):
            object.__setattr__(self, field, _tuple(getattr(self, field), field, required=required))
        if any(not _REQUIREMENT_ID.fullmatch(item) for item in self.required_requirement_ids):
            raise InvestigationValidationError("Task has an invalid requirement ID")
        if set(self.missing_requirement_ids) - set(self.required_requirement_ids):
            raise InvestigationValidationError("Task missing requirements must be required")
        if self.autonomous_action is not False:
            raise InvestigationValidationError("Investigation tasks cannot be autonomous")

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_order": self.task_order,
            "task_id": self.task_id,
            "opportunity_id": self.opportunity_id,
            "task_kind": self.task_kind.value,
            "title": self.title,
            "objective": self.objective,
            "readiness": self.readiness.value,
            "required_requirement_ids": list(self.required_requirement_ids),
            "available_evidence_refs": list(self.available_evidence_refs),
            "missing_requirement_ids": list(self.missing_requirement_ids),
            "expected_output": self.expected_output,
            "strengthens_criteria": list(self.strengthens_criteria),
            "weakens_criteria": list(self.weakens_criteria),
            "completion_criteria": list(self.completion_criteria),
            "limitation": self.limitation,
            "autonomous_action": self.autonomous_action,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationPlan:
    plan_id: str
    opportunity_id: str
    business_id: str
    as_of_date: date
    opportunity_priority: OpportunityPriority
    opportunity_confidence: OpportunityConfidence
    status: PlanStatus
    tasks: tuple[InvestigationTask, ...]
    recommended_start_task_id: str | None
    evidence_requirements: tuple[EvidenceRequirement, ...]
    unresolved_requirement_ids: tuple[str, ...]
    decision_unlocked: str
    limitation: str

    def __post_init__(self) -> None:
        for field in ("plan_id", "opportunity_id", "business_id", "decision_unlocked", "limitation"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not _PLAN_ID.fullmatch(self.plan_id):
            raise InvestigationValidationError("plan_id has an invalid stable shape")
        if isinstance(self.as_of_date, datetime) or not isinstance(self.as_of_date, date):
            raise InvestigationValidationError("as_of_date must be a date")
        try:
            object.__setattr__(self, "opportunity_priority", OpportunityPriority(self.opportunity_priority))
            object.__setattr__(self, "opportunity_confidence", OpportunityConfidence(self.opportunity_confidence))
            object.__setattr__(self, "status", PlanStatus(self.status))
        except ValueError as exc:
            raise InvestigationValidationError(str(exc)) from None
        if not isinstance(self.tasks, tuple) or not self.tasks:
            raise InvestigationValidationError("Plan tasks must be a non-empty tuple")
        if not all(isinstance(item, InvestigationTask) for item in self.tasks):
            raise InvestigationValidationError("Plan has an invalid task")
        if not isinstance(self.evidence_requirements, tuple) or not self.evidence_requirements:
            raise InvestigationValidationError("Plan evidence requirements must be a non-empty tuple")
        if not all(isinstance(item, EvidenceRequirement) for item in self.evidence_requirements):
            raise InvestigationValidationError("Plan has an invalid evidence requirement")
        object.__setattr__(
            self,
            "unresolved_requirement_ids",
            _tuple(self.unresolved_requirement_ids, "unresolved_requirement_ids"),
        )
        if self.recommended_start_task_id is not None:
            object.__setattr__(
                self,
                "recommended_start_task_id",
                _text(self.recommended_start_task_id, "recommended_start_task_id"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "opportunity_id": self.opportunity_id,
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "opportunity_priority": self.opportunity_priority.value,
            "opportunity_confidence": self.opportunity_confidence.value,
            "status": self.status.value,
            "tasks": [item.to_dict() for item in self.tasks],
            "recommended_start_task_id": self.recommended_start_task_id,
            "evidence_requirements": [item.to_dict() for item in self.evidence_requirements],
            "unresolved_requirement_ids": list(self.unresolved_requirement_ids),
            "decision_unlocked": self.decision_unlocked,
            "limitation": self.limitation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceGap:
    gap_id: str
    requirement_id: str
    description: str
    affected_opportunity_ids: tuple[str, ...]
    affected_task_ids: tuple[str, ...]
    opportunity_priorities: tuple[OpportunityPriority, ...]
    status: EvidenceRequirementStatus
    limitation: str

    def __post_init__(self) -> None:
        for field in ("gap_id", "requirement_id", "description", "limitation"):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if not _GAP_ID.fullmatch(self.gap_id) or self.gap_id != stable_gap_id(self.requirement_id):
            raise InvestigationValidationError("gap_id is not the stable expected ID")
        object.__setattr__(
            self, "affected_opportunity_ids",
            _tuple(self.affected_opportunity_ids, "affected_opportunity_ids", required=True),
        )
        object.__setattr__(
            self, "affected_task_ids",
            _tuple(self.affected_task_ids, "affected_task_ids", required=True),
        )
        if not isinstance(self.opportunity_priorities, tuple) or not self.opportunity_priorities:
            raise InvestigationValidationError("Evidence gap priorities must be a non-empty tuple")
        try:
            object.__setattr__(
                self,
                "opportunity_priorities",
                tuple(OpportunityPriority(item) for item in self.opportunity_priorities),
            )
            object.__setattr__(self, "status", EvidenceRequirementStatus(self.status))
        except ValueError as exc:
            raise InvestigationValidationError(str(exc)) from None
        if self.status is not EvidenceRequirementStatus.MISSING_FROM_CONTEXT:
            raise InvestigationValidationError("Evidence gaps must be missing from context")

    def to_dict(self) -> dict[str, Any]:
        return {
            "gap_id": self.gap_id,
            "requirement_id": self.requirement_id,
            "description": self.description,
            "affected_opportunity_ids": list(self.affected_opportunity_ids),
            "affected_task_ids": list(self.affected_task_ids),
            "opportunity_priorities": [item.value for item in self.opportunity_priorities],
            "status": self.status.value,
            "limitation": self.limitation,
        }


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationPortfolio:
    business_id: str
    as_of_date: date
    plans: tuple[InvestigationPlan, ...]
    evidence_gaps: tuple[EvidenceGap, ...]
    recommended_start_task_id: str | None
    ready_task_count: int
    partial_task_count: int
    blocked_task_count: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "business_id", _text(self.business_id, "business_id"))
        if isinstance(self.as_of_date, datetime) or not isinstance(self.as_of_date, date):
            raise InvestigationValidationError("as_of_date must be a date")
        if not isinstance(self.plans, tuple) or not all(
            isinstance(item, InvestigationPlan) for item in self.plans
        ):
            raise InvestigationValidationError("Portfolio plans must be a tuple")
        if not isinstance(self.evidence_gaps, tuple) or not all(
            isinstance(item, EvidenceGap) for item in self.evidence_gaps
        ):
            raise InvestigationValidationError("Portfolio evidence gaps must be a tuple")
        for field in ("ready_task_count", "partial_task_count", "blocked_task_count"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise InvestigationValidationError(f"{field} must be a non-negative integer")
        if self.recommended_start_task_id is not None:
            object.__setattr__(
                self,
                "recommended_start_task_id",
                _text(self.recommended_start_task_id, "recommended_start_task_id"),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "business_id": self.business_id,
            "as_of_date": self.as_of_date.isoformat(),
            "plans": [item.to_dict() for item in self.plans],
            "evidence_gaps": [item.to_dict() for item in self.evidence_gaps],
            "recommended_start_task_id": self.recommended_start_task_id,
            "ready_task_count": self.ready_task_count,
            "partial_task_count": self.partial_task_count,
            "blocked_task_count": self.blocked_task_count,
        }
