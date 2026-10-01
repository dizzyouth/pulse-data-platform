"""Strict public contracts for the Phase 6.6C Analyst API."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AskRequest(StrictApiModel):
    business_id: str = Field(min_length=1, max_length=64)
    question: str = Field(min_length=1, max_length=1000)

    @field_validator("business_id", "question")
    @classmethod
    def strip_text(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be blank")
        return stripped


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


class ApiErrorCode(StrEnum):
    INVALID_REQUEST = "INVALID_REQUEST"
    BUSINESS_NOT_FOUND = "BUSINESS_NOT_FOUND"
    WAREHOUSE_UNAVAILABLE = "WAREHOUSE_UNAVAILABLE"
    ANALYST_VALIDATION_FAILED = "ANALYST_VALIDATION_FAILED"
    PROVIDER_CONFIGURATION_ERROR = "PROVIDER_CONFIGURATION_ERROR"
    PROVIDER_UNAVAILABLE = "PROVIDER_UNAVAILABLE"
    AGGREGATE_ONLY_REQUIRED = "AGGREGATE_ONLY_REQUIRED"


class ErrorDetail(StrictApiModel):
    code: ApiErrorCode
    message: str
    request_id: str


class ErrorResponse(StrictApiModel):
    error: ErrorDetail
