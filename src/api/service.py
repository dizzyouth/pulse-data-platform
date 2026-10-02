"""Thin service adapter over the analyst and opportunity engines."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Callable

import psycopg

from src.api.models import ApiErrorCode
from src.api.safety import is_aggregate_question, valid_business_id
from src.intelligence.answer_models import AnalystAnswer, AnswerValidationError
from src.intelligence.context import IntelligenceContext, build_context
from src.intelligence.narration import (
    AnswerExecutionMetadata,
    answer_question_with_metadata,
)
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_answers import (
    answer_opportunity_question,
    classify_opportunity_question,
)
from src.intelligence.opportunity_models import (
    InvestigationOpportunity,
    OpportunityEvaluation,
    OpportunityValidationError,
    valid_opportunity_id_shape,
)
from src.intelligence.providers import (
    NarrationProvider,
    OfflineFakeProvider,
    OpenAIProvider,
    ProviderError,
    classify_question_intent,
)
from src.warehouse.load_gold import connection_kwargs


SUGGESTED_QUESTIONS = (
    "What should I investigate first?",
    "Why are returns high?",
    "Should I pause Sama-NewUM?",
    "How much profit am I making?",
    "Did something abnormal happen today?",
)

CAPABILITIES = (
    "Prioritize deterministic business signals",
    "Explain observed return and campaign peer patterns",
    "Report persisted anomaly state",
    "Explain economic safety limitations",
    "Suggest investigation steps grounded in aggregate evidence",
    "Identify deterministic cross-domain investigation opportunities",
    "Explain opportunity hypotheses and confirmation or refutation criteria",
    "Explain what future decision better evidence could unlock",
)

LIMITATIONS = (
    "Aggregate business evidence only; no customer-level analysis.",
    "No autonomous campaign, budget, refund, or customer-contact actions.",
    "No trusted cross-currency profit while FX_REQUIRED applies.",
    "No persistent conversation memory.",
    "Observed differences do not establish causal reasons.",
)


class AnalystServiceError(RuntimeError):
    def __init__(
        self, code: ApiErrorCode, message: str, status_code: int
    ) -> None:
        self.code = code
        self.safe_message = message
        self.status_code = status_code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ServiceAnswer:
    answer: AnalystAnswer
    execution: AnswerExecutionMetadata
    question_category: str
    provider: str
    model: str
    evidence_count: int
    answer_source: str
    opportunity_type: str | None = None


ContextBuilder = Callable[[str], IntelligenceContext]
ProviderFactory = Callable[[str], NarrationProvider]
WarehouseChecker = Callable[[], bool]


def check_warehouse() -> bool:
    """Perform one bounded local connectivity query without involving a provider."""
    settings = dict(connection_kwargs())
    settings.setdefault("connect_timeout", 2)
    with psycopg.connect(**settings) as connection:
        connection.read_only = True
        row = connection.execute("SELECT 1").fetchone()
        return bool(row and row[0] == 1)


def configured_provider_name() -> str:
    return (os.environ.get("PULSE_LLM_PROVIDER") or "fake").strip().lower()


def provider_configuration() -> tuple[str, bool]:
    name = configured_provider_name()
    if name == "fake":
        return name, True
    if name == "openai":
        return name, bool(
            os.environ.get("PULSE_LLM_MODEL", "").strip()
            and os.environ.get("OPENAI_API_KEY", "").strip()
        )
    return name, False


def build_provider(name: str) -> NarrationProvider:
    if name == "fake":
        return OfflineFakeProvider()
    if name != "openai":
        raise AnalystServiceError(
            ApiErrorCode.PROVIDER_CONFIGURATION_ERROR,
            "The configured analyst provider is unsupported.",
            503,
        )
    if not os.environ.get("PULSE_LLM_MODEL", "").strip() or not os.environ.get(
        "OPENAI_API_KEY", ""
    ).strip():
        raise AnalystServiceError(
            ApiErrorCode.PROVIDER_CONFIGURATION_ERROR,
            "The configured analyst provider is unavailable.",
            503,
        )
    try:
        return OpenAIProvider.from_env()
    except ProviderError:
        raise AnalystServiceError(
            ApiErrorCode.PROVIDER_CONFIGURATION_ERROR,
            "The configured analyst provider is unavailable.",
            503,
        ) from None


class AnalystService:
    def __init__(
        self,
        *,
        context_builder: ContextBuilder = build_context,
        provider_factory: ProviderFactory = build_provider,
        warehouse_checker: WarehouseChecker = check_warehouse,
    ) -> None:
        self._context_builder = context_builder
        self._provider_factory = provider_factory
        self._warehouse_checker = warehouse_checker

    def health(self) -> tuple[str, str, bool]:
        provider, configured = provider_configuration()
        try:
            reachable = self._warehouse_checker()
        except Exception:
            reachable = False
        return ("reachable" if reachable else "unavailable", provider, configured)

    @staticmethod
    def _validate_business_id(business_id: str) -> None:
        if not valid_business_id(business_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_REQUEST,
                "The request contains an invalid business identifier.",
                422,
            )

    @staticmethod
    def _validate_opportunity_id(opportunity_id: str) -> None:
        if len(opportunity_id) > 200 or not valid_opportunity_id_shape(opportunity_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_OPPORTUNITY_ID,
                "The request contains an invalid opportunity identifier.",
                422,
            )

    def _build_context(self, business_id: str) -> IntelligenceContext:
        try:
            context = self._context_builder(business_id)
        except LookupError:
            raise AnalystServiceError(
                ApiErrorCode.BUSINESS_NOT_FOUND,
                "No analyst context is available for this business.",
                404,
            ) from None
        except (psycopg.Error, OSError, ValueError):
            raise AnalystServiceError(
                ApiErrorCode.WAREHOUSE_UNAVAILABLE,
                "The aggregate evidence warehouse is unavailable.",
                503,
            ) from None
        if context.business_id != business_id:
            raise AnalystServiceError(
                ApiErrorCode.BUSINESS_NOT_FOUND,
                "No analyst context is available for this business.",
                404,
            )
        return context

    @staticmethod
    def _evaluate(context: IntelligenceContext) -> OpportunityEvaluation:
        try:
            return evaluate_opportunities(context)
        except OpportunityValidationError:
            raise AnalystServiceError(
                ApiErrorCode.OPPORTUNITY_VALIDATION_FAILED,
                "The opportunity engine could not produce safely validated results.",
                422,
            ) from None

    def list_opportunities(self, business_id: str) -> OpportunityEvaluation:
        self._validate_business_id(business_id)
        return self._evaluate(self._build_context(business_id))

    def _resolve_opportunity(
        self, business_id: str, opportunity_id: str
    ) -> tuple[IntelligenceContext, InvestigationOpportunity]:
        self._validate_business_id(business_id)
        self._validate_opportunity_id(opportunity_id)
        context = self._build_context(business_id)
        evaluation = self._evaluate(context)
        opportunity = next(
            (item for item in evaluation.opportunities
             if item.opportunity_id == opportunity_id),
            None,
        )
        if opportunity is None or opportunity.business_id != business_id:
            raise AnalystServiceError(
                ApiErrorCode.OPPORTUNITY_NOT_FOUND,
                "No active opportunity is available for this identifier and business.",
                404,
            )
        return context, opportunity

    def get_opportunity(
        self, business_id: str, opportunity_id: str
    ) -> InvestigationOpportunity:
        return self._resolve_opportunity(business_id, opportunity_id)[1]

    def ask(
        self, business_id: str, question: str, opportunity_id: str | None = None
    ) -> ServiceAnswer:
        self._validate_business_id(business_id)
        if not is_aggregate_question(question):
            raise AnalystServiceError(
                ApiErrorCode.AGGREGATE_ONLY_REQUIRED,
                "This analyst accepts aggregate business questions only.",
                422,
            )

        if opportunity_id is not None:
            started = time.perf_counter()
            context, opportunity = self._resolve_opportunity(
                business_id, opportunity_id
            )
            try:
                answer = answer_opportunity_question(question, opportunity, context)
            except (AnswerValidationError, OpportunityValidationError):
                raise AnalystServiceError(
                    ApiErrorCode.ANALYST_VALIDATION_FAILED,
                    "The analyst could not produce a safely grounded answer.",
                    422,
                ) from None
            return ServiceAnswer(
                answer=answer,
                execution=AnswerExecutionMetadata(
                    provider_call_count=0,
                    repair_attempted=False,
                    deterministic_fallback_used=False,
                    fallback_intent=None,
                    latency_ms=(time.perf_counter() - started) * 1000,
                ),
                question_category=classify_opportunity_question(question).value,
                provider="deterministic",
                model="opportunity-engine-v1",
                evidence_count=len(context.evidence_items),
                answer_source="deterministic_opportunity",
                opportunity_type=opportunity.opportunity_type.value,
            )

        provider_name = configured_provider_name()
        provider = self._provider_factory(provider_name)
        context = self._build_context(business_id)

        try:
            result = answer_question_with_metadata(question, context, provider)
        except AnswerValidationError:
            raise AnalystServiceError(
                ApiErrorCode.ANALYST_VALIDATION_FAILED,
                "The analyst could not produce a safely grounded answer.",
                422,
            ) from None
        except ProviderError:
            raise AnalystServiceError(
                ApiErrorCode.PROVIDER_UNAVAILABLE,
                "The configured analyst provider is unavailable.",
                503,
            ) from None

        return ServiceAnswer(
            answer=result.answer,
            execution=result.metadata,
            question_category=classify_question_intent(question).value,
            provider=provider.name,
            model=provider.model,
            evidence_count=len(context.evidence_items),
            answer_source="grounded_analyst",
        )
