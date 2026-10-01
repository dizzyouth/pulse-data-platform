"""FastAPI application for the local Phase 6.6C Ask Pulse product surface."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from src.api.models import (
    AnalystAnswerResponse,
    ApiErrorCode,
    AskRequest,
    AskResponse,
    CapabilitiesResponse,
    ErrorDetail,
    ErrorResponse,
    ExecutionMetaResponse,
    FindingResponse,
    HealthResponse,
)
from src.api.safety import valid_business_id
from src.api.service import (
    CAPABILITIES,
    LIMITATIONS,
    SUGGESTED_QUESTIONS,
    AnalystService,
    AnalystServiceError,
)


STATIC_DIR = Path(__file__).resolve().parent / "static"
LOGGER = logging.getLogger("pulse.api")


def _request_id(request: Request) -> str:
    current = getattr(request.state, "request_id", None)
    if current is None:
        current = uuid4().hex
        request.state.request_id = current
    return current


def _error_response(
    *, code: ApiErrorCode, message: str, request_id: str, status_code: int
) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorDetail(code=code, message=message, request_id=request_id)
    )
    return JSONResponse(
        status_code=status_code,
        content=payload.model_dump(mode="json"),
        headers={"X-Request-ID": request_id},
    )


def create_app(service: AnalystService | None = None) -> FastAPI:
    analyst = service or AnalystService()
    application = FastAPI(
        title="Pulse Analyst API",
        version="6.6C",
        debug=False,
        description="Local development API for grounded aggregate business intelligence.",
    )
    application.state.analyst_service = analyst
    application.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @application.exception_handler(RequestValidationError)
    async def invalid_request(
        request: Request, _error: RequestValidationError
    ) -> JSONResponse:
        request_id = _request_id(request)
        LOGGER.info(
            "analyst_api request_id=%s success=false error_code=%s",
            request_id,
            ApiErrorCode.INVALID_REQUEST.value,
        )
        return _error_response(
            code=ApiErrorCode.INVALID_REQUEST,
            message="The request is invalid.",
            request_id=request_id,
            status_code=422,
        )

    @application.exception_handler(AnalystServiceError)
    async def service_error(
        request: Request, error: AnalystServiceError
    ) -> JSONResponse:
        request_id = _request_id(request)
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s success=false error_code=%s",
            request_id,
            getattr(request.state, "business_id", "UNKNOWN"),
            error.code.value,
        )
        return _error_response(
            code=error.code,
            message=error.safe_message,
            request_id=request_id,
            status_code=error.status_code,
        )

    @application.exception_handler(Exception)
    async def unexpected_error(request: Request, _error: Exception) -> JSONResponse:
        request_id = _request_id(request)
        LOGGER.error(
            "analyst_api request_id=%s business_id=%s success=false error_code=%s",
            request_id,
            getattr(request.state, "business_id", "UNKNOWN"),
            ApiErrorCode.ANALYST_VALIDATION_FAILED.value,
        )
        return _error_response(
            code=ApiErrorCode.ANALYST_VALIDATION_FAILED,
            message="The analyst could not produce a safely grounded answer.",
            request_id=request_id,
            status_code=500,
        )

    @application.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @application.get("/api/v1/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        warehouse, provider, configured = analyst.health()
        return HealthResponse(
            status="ok" if warehouse == "reachable" else "degraded",
            service="pulse-analyst",
            version="6.6C",
            warehouse=warehouse,
            provider=provider,
            provider_configured=configured,
        )

    @application.get(
        "/api/v1/analyst/capabilities", response_model=CapabilitiesResponse
    )
    async def capabilities() -> CapabilitiesResponse:
        return CapabilitiesResponse(
            service="pulse-analyst",
            capabilities=list(CAPABILITIES),
            suggested_questions=list(SUGGESTED_QUESTIONS),
            limitations=list(LIMITATIONS),
        )

    @application.post("/api/v1/analyst/ask", response_model=AskResponse)
    async def ask(request: Request, payload: AskRequest) -> AskResponse:
        request_id = _request_id(request)
        if valid_business_id(payload.business_id):
            request.state.business_id = payload.business_id
        result = analyst.ask(payload.business_id, payload.question)
        answer = result.answer
        response = AskResponse(
            request_id=request_id,
            business_id=payload.business_id,
            question_category=result.question_category,
            answer=AnalystAnswerResponse(
                answer_summary=answer.answer_summary,
                findings=[
                    FindingResponse(
                        statement=finding.statement,
                        evidence_refs=list(finding.evidence_refs),
                        confidence=finding.confidence.value,
                        claim_type=finding.claim_type.value,
                        causal_claim=finding.causal_claim,
                    )
                    for finding in answer.findings
                ],
                investigation_steps=list(answer.investigation_steps),
                evidence_refs=list(answer.evidence_refs),
                limitations=list(answer.limitations),
                confidence=answer.confidence.value,
                cannot_answer_fully=answer.cannot_answer_fully,
                safety_notes=list(answer.safety_notes),
            ),
            meta=ExecutionMetaResponse(
                provider=result.provider,
                model=result.model,
                evidence_count=result.evidence_count,
                provider_call_count=result.execution.provider_call_count,
                repair_attempted=result.execution.repair_attempted,
                deterministic_fallback_used=(
                    result.execution.deterministic_fallback_used
                ),
                fallback_intent=result.execution.fallback_intent,
                latency_ms=round(result.execution.latency_ms, 2),
            ),
        )
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s question_category=%s "
            "provider=%s model=%s evidence_count=%d latency_ms=%.2f "
            "provider_call_count=%d repair_attempted=%s "
            "deterministic_fallback_used=%s success=true",
            request_id,
            payload.business_id,
            result.question_category,
            result.provider,
            result.model,
            result.evidence_count,
            result.execution.latency_ms,
            result.execution.provider_call_count,
            result.execution.repair_attempted,
            result.execution.deterministic_fallback_used,
        )
        return response

    return application


app = create_app()


def main() -> None:
    """Run the localhost-only development server."""
    import uvicorn

    host = os.environ.get("PULSE_API_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("PULSE_API_PORT", "8088"))
    except ValueError:
        raise SystemExit("PULSE_API_PORT must be an integer") from None
    if not 1 <= port <= 65535:
        raise SystemExit("PULSE_API_PORT must be between 1 and 65535")
    uvicorn.run("src.api.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
