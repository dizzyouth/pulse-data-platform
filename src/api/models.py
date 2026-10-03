"""Strict public contracts for the Phase 6.7D Analyst API."""

from __future__ import annotations

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AskRequest(StrictApiModel):
    business_id: str = Field(min_length=1, max_length=64)
    question: str = Field(min_length=1, max_length=1000)
    opportunity_id: str | None = Field(default=None, min_length=1, max_length=200)
    investigation_task_id: str | None = Field(default=None, min_length=1, max_length=300)

    @field_validator("business_id", "question", "opportunity_id", "investigation_task_id")
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        return stripped

    @model_validator(mode="after")
    def select_one_answer_mode(self) -> "AskRequest":
        if self.opportunity_id is not None and self.investigation_task_id is not None:
            raise ValueError(
                "opportunity_id and investigation_task_id cannot both be supplied"
            )
        return self


class FindingResponse(StrictApiModel):
    statement: str
    evidence_refs: list[str]
    confidence: str
    claim_type: str
    causal_claim: bool


class AnalystAnswerResponse(StrictApiModel):
    answer_summary: str
    findings: list[FindingResponse]
    investigation_steps: list[str]
    evidence_refs: list[str]
    limitations: list[str]
    confidence: str
    cannot_answer_fully: bool
    safety_notes: list[str]


class ExecutionMetaResponse(StrictApiModel):
    provider: str
    model: str
    evidence_count: int
    provider_call_count: int
    repair_attempted: bool
    deterministic_fallback_used: bool
    fallback_intent: str | None
    answer_source: str
    latency_ms: float


class AskResponse(StrictApiModel):
    request_id: str
    business_id: str
    question_category: str
    answer: AnalystAnswerResponse
    meta: ExecutionMetaResponse


class HealthResponse(StrictApiModel):
    status: str
    service: str
    version: str
    warehouse: str
    provider: str
    provider_configured: bool


class CapabilitiesResponse(StrictApiModel):
    service: str
    capabilities: list[str]
    suggested_questions: list[str]
    limitations: list[str]


class OpportunityResponse(StrictApiModel):
    opportunity_order: int
    opportunity_id: str
    scope_type: str
    scope_name: str
    opportunity_type: str
    category: str
    title: str
    observation_summary: str
    hypothesis_to_test: str
    hypothesis_status: str
    priority: str
    confidence: str
    impact_proxy_name: str | None
    impact_proxy_value: float | None
    impact_proxy_unit: str | None
    supporting_evidence_refs: list[str]
    counter_evidence_refs: list[str]
    blocking_evidence_refs: list[str]
    investigation_steps: list[str]
    confirmation_criteria: list[str]
    refutation_criteria: list[str]
    decision_unlocked: str
    missing_evidence: list[str]
    limitation: str


class OpportunityListResponse(StrictApiModel):
    business_id: str
    as_of_date: date
    count: int
    opportunities: list[OpportunityResponse]


class OpportunityDetailResponse(StrictApiModel):
    business_id: str
    as_of_date: date
    opportunity: OpportunityResponse


class EvidenceRequirementResponse(StrictApiModel):
    requirement_id: str
    name: str
    description: str
    status: str
    evidence_refs: list[str]
    missing_reason: str | None
    collection_hint: str
    source_scope: str
    required_for_task_ids: list[str]


class InvestigationTaskResponse(StrictApiModel):
    task_order: int
    task_id: str
    opportunity_id: str
    task_kind: str
    title: str
    objective: str
    readiness: str
    required_requirement_ids: list[str]
    available_evidence_refs: list[str]
    missing_requirement_ids: list[str]
    expected_output: str
    strengthens_criteria: list[str]
    weakens_criteria: list[str]
    completion_criteria: list[str]
    limitation: str
    autonomous_action: bool


class InvestigationPlanResponse(StrictApiModel):
    plan_id: str
    opportunity_id: str
    business_id: str
    as_of_date: date
    opportunity_priority: str
    opportunity_confidence: str
    status: str
    tasks: list[InvestigationTaskResponse]
    recommended_start_task_id: str | None
    evidence_requirements: list[EvidenceRequirementResponse]
    unresolved_requirement_ids: list[str]
    decision_unlocked: str
    limitation: str


class EvidenceGapResponse(StrictApiModel):
    gap_id: str
    requirement_id: str
    description: str
    affected_opportunity_ids: list[str]
    affected_task_ids: list[str]
    opportunity_priorities: list[str]
    status: str
    limitation: str


class InvestigationPortfolioResponse(StrictApiModel):
    business_id: str
    as_of_date: date
    plans: list[InvestigationPlanResponse]
    evidence_gaps: list[EvidenceGapResponse]
    recommended_start_task_id: str | None
    ready_task_count: int
    partial_task_count: int
    blocked_task_count: int


class InvestigationPlanDetailResponse(StrictApiModel):
    business_id: str
    as_of_date: date
    plan: InvestigationPlanResponse


class InvestigationTaskDetailResponse(StrictApiModel):
    business_id: str
    as_of_date: date
    plan_id: str
    task: InvestigationTaskResponse


class ApiErrorCode(StrEnum):
    INVALID_REQUEST = "INVALID_REQUEST"
    BUSINESS_NOT_FOUND = "BUSINESS_NOT_FOUND"
    WAREHOUSE_UNAVAILABLE = "WAREHOUSE_UNAVAILABLE"
    ANALYST_VALIDATION_FAILED = "ANALYST_VALIDATION_FAILED"
    PROVIDER_CONFIGURATION_ERROR = "PROVIDER_CONFIGURATION_ERROR"
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    AGGREGATE_ONLY_REQUIRED = "AGGREGATE_ONLY_REQUIRED"
    INVALID_OPPORTUNITY_ID = "INVALID_OPPORTUNITY_ID"
    OPPORTUNITY_NOT_FOUND = "OPPORTUNITY_NOT_FOUND"
    OPPORTUNITY_VALIDATION_FAILED = "OPPORTUNITY_VALIDATION_FAILED"
    INVALID_INVESTIGATION_ID = "INVALID_INVESTIGATION_ID"
    INVESTIGATION_PLAN_NOT_FOUND = "INVESTIGATION_PLAN_NOT_FOUND"
    INVESTIGATION_TASK_NOT_FOUND = "INVESTIGATION_TASK_NOT_FOUND"
    INVESTIGATION_VALIDATION_FAILED = "INVESTIGATION_VALIDATION_FAILED"


class ErrorDetail(StrictApiModel):
    code: ApiErrorCode
    message: str
    request_id: str


class ErrorResponse(StrictApiModel):
    error: ErrorDetail
