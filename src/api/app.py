"""FastAPI application for the local Phase 6.7D Ask Pulse product surface."""

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
    InvestigationPlanDetailResponse,
    InvestigationPlanResponse,
    InvestigationPortfolioResponse,
    InvestigationTaskDetailResponse,
    InvestigationTaskResponse,
    OpportunityDetailResponse,
    OpportunityListResponse,
    OpportunityResponse,
)
from src.api.safety import valid_business_id
from src.api.service import (
    CAPABILITIES,
    LIMITATIONS,
    SUGGESTED_QUESTIONS,
    AnalystService,
    AnalystServiceError,
)
from src.intelligence.opportunity_models import (
    InvestigationOpportunity,
    valid_opportunity_id_shape,
)
from src.intelligence.investigation_models import (
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationTask,
    valid_plan_id_shape,
    valid_task_id_shape,
)


STATIC_DIR = Path(__file__).resolve().parent / "static"
LOGGER = logging.getLogger("pulse.api")


def _opportunity_response(
    opportunity: InvestigationOpportunity,
) -> OpportunityResponse:
    return OpportunityResponse(
        opportunity_order=opportunity.opportunity_order,
        opportunity_id=opportunity.opportunity_id,
        scope_type=opportunity.scope_type.value,
        scope_name=opportunity.scope_name,
        opportunity_type=opportunity.opportunity_type.value,
        category=opportunity.category.value,
        title=opportunity.title,
        observation_summary=opportunity.observation_summary,
        hypothesis_to_test=opportunity.hypothesis_to_test,
        hypothesis_status=opportunity.hypothesis_status.value,
        priority=opportunity.priority.value,
        confidence=opportunity.confidence.value,
        impact_proxy_name=opportunity.impact_proxy_name,
        impact_proxy_value=opportunity.impact_proxy_value,
        impact_proxy_unit=opportunity.impact_proxy_unit,
        supporting_evidence_refs=list(opportunity.supporting_evidence_refs),
        counter_evidence_refs=list(opportunity.counter_evidence_refs),
        blocking_evidence_refs=list(opportunity.blocking_evidence_refs),
        investigation_steps=list(opportunity.investigation_steps),
        confirmation_criteria=list(opportunity.confirmation_criteria),
        refutation_criteria=list(opportunity.refutation_criteria),
        decision_unlocked=opportunity.decision_unlocked,
        missing_evidence=list(opportunity.missing_evidence),
        limitation=opportunity.limitation,
    )


def _investigation_plan_response(
    plan: InvestigationPlan,
) -> InvestigationPlanResponse:
    return InvestigationPlanResponse.model_validate(plan.to_dict())


def _investigation_task_response(
    task: InvestigationTask,
) -> InvestigationTaskResponse:
    return InvestigationTaskResponse.model_validate(task.to_dict())


def _investigation_portfolio_response(
    portfolio: InvestigationPortfolio,
) -> InvestigationPortfolioResponse:
    return InvestigationPortfolioResponse.model_validate(portfolio.to_dict())


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
        version="6.7D",
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
            version="6.7D",
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

    @application.get(
        "/api/v1/analyst/opportunities",
        response_model=OpportunityListResponse,
    )
    async def list_opportunities(
        request: Request, business_id: str
    ) -> OpportunityListResponse:
        if valid_business_id(business_id):
            request.state.business_id = business_id
        evaluation = analyst.list_opportunities(business_id)
        response = OpportunityListResponse(
            business_id=evaluation.business_id,
            as_of_date=evaluation.as_of_date,
            count=len(evaluation.opportunities),
            opportunities=[
                _opportunity_response(item) for item in evaluation.opportunities
            ],
        )
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s answer_source=%s "
            "opportunity_count=%d success=true",
            _request_id(request),
            business_id,
            "deterministic_opportunity",
            response.count,
        )
        return response

    @application.get(
        "/api/v1/analyst/opportunities/{opportunity_id}",
        response_model=OpportunityDetailResponse,
    )
    async def get_opportunity(
        request: Request, opportunity_id: str, business_id: str
    ) -> OpportunityDetailResponse:
        if valid_business_id(business_id):
            request.state.business_id = business_id
        if len(opportunity_id) <= 200 and valid_opportunity_id_shape(opportunity_id):
            request.state.opportunity_id = opportunity_id
        opportunity = analyst.get_opportunity(business_id, opportunity_id)
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s opportunity_id=%s "
            "opportunity_type=%s answer_source=%s success=true",
            _request_id(request),
            business_id,
            opportunity.opportunity_id,
            opportunity.opportunity_type.value,
            "deterministic_opportunity",
        )
        return OpportunityDetailResponse(
            business_id=opportunity.business_id,
            as_of_date=opportunity.as_of_date,
            opportunity=_opportunity_response(opportunity),
        )

    @application.get(
        "/api/v1/analyst/investigations",
        response_model=InvestigationPortfolioResponse,
    )
    async def list_investigations(
        request: Request, business_id: str
    ) -> InvestigationPortfolioResponse:
        if valid_business_id(business_id):
            request.state.business_id = business_id
        portfolio = analyst.list_investigations(business_id)
        response = _investigation_portfolio_response(portfolio)
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s answer_source=%s "
            "plan_count=%d ready_task_count=%d partial_task_count=%d "
            "blocked_task_count=%d success=true",
            _request_id(request),
            business_id,
            "deterministic_investigation",
            len(response.plans),
            response.ready_task_count,
            response.partial_task_count,
            response.blocked_task_count,
        )
        return response

    @application.get(
        "/api/v1/analyst/investigations/{plan_id}",
        response_model=InvestigationPlanDetailResponse,
    )
    async def get_investigation_plan(
        request: Request, plan_id: str, business_id: str
    ) -> InvestigationPlanDetailResponse:
        if valid_business_id(business_id):
            request.state.business_id = business_id
        if len(plan_id) <= 240 and valid_plan_id_shape(plan_id):
            request.state.plan_id = plan_id
        plan = analyst.get_investigation_plan(business_id, plan_id)
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s plan_id=%s "
            "answer_source=%s success=true",
            _request_id(request),
            business_id,
            plan.plan_id,
            "deterministic_investigation",
        )
        return InvestigationPlanDetailResponse(
            business_id=plan.business_id,
            as_of_date=plan.as_of_date,
            plan=_investigation_plan_response(plan),
        )

    @application.get(
        "/api/v1/analyst/investigation-tasks/{task_id}",
        response_model=InvestigationTaskDetailResponse,
    )
    async def get_investigation_task(
        request: Request, task_id: str, business_id: str
    ) -> InvestigationTaskDetailResponse:
        if valid_business_id(business_id):
            request.state.business_id = business_id
        if len(task_id) <= 300 and valid_task_id_shape(task_id):
            request.state.task_id = task_id
        plan, task = analyst.get_investigation_task(business_id, task_id)
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s plan_id=%s task_id=%s "
            "task_readiness=%s answer_source=%s success=true",
            _request_id(request),
            business_id,
            plan.plan_id,
            task.task_id,
            task.readiness.value,
            "deterministic_investigation",
        )
        return InvestigationTaskDetailResponse(
            business_id=plan.business_id,
            as_of_date=plan.as_of_date,
            plan_id=plan.plan_id,
            task=_investigation_task_response(task),
        )

    @application.post("/api/v1/analyst/ask", response_model=AskResponse)
    async def ask(request: Request, payload: AskRequest) -> AskResponse:
        request_id = _request_id(request)
        if valid_business_id(payload.business_id):
            request.state.business_id = payload.business_id
        if (
            payload.opportunity_id is not None
            and valid_opportunity_id_shape(payload.opportunity_id)
        ):
            request.state.opportunity_id = payload.opportunity_id
        if (
            payload.investigation_task_id is not None
            and valid_task_id_shape(payload.investigation_task_id)
        ):
            request.state.task_id = payload.investigation_task_id
        result = analyst.ask(
            payload.business_id,
            payload.question,
            payload.opportunity_id,
            payload.investigation_task_id,
        )
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
                answer_source=result.answer_source,
                latency_ms=round(result.execution.latency_ms, 2),
            ),
        )
        LOGGER.info(
            "analyst_api request_id=%s business_id=%s question_category=%s "
            "opportunity_id=%s opportunity_type=%s plan_id=%s task_id=%s "
            "task_readiness=%s answer_source=%s "
            "provider=%s model=%s evidence_count=%d latency_ms=%.2f "
            "provider_call_count=%d repair_attempted=%s "
            "deterministic_fallback_used=%s success=true",
            request_id,
            payload.business_id,
            result.question_category,
            getattr(request.state, "opportunity_id", "NONE"),
            result.opportunity_type or "NONE",
            result.investigation_plan_id or "NONE",
            result.investigation_task_id or "NONE",
            result.investigation_readiness or "NONE",
            result.answer_source,
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
