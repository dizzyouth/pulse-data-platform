"""Thin service adapter over the existing Phase 6.6B intelligence engine."""

from __future__ import annotations

import os
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

    def ask(self, business_id: str, question: str) -> ServiceAnswer:
        if not valid_business_id(business_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_REQUEST,
                "The request contains an invalid business identifier.",
                422,
            )
        if not is_aggregate_question(question):
            raise AnalystServiceError(
                ApiErrorCode.AGGREGATE_ONLY_REQUIRED,
                "This analyst accepts aggregate business questions only.",
                422,
            )

        provider_name = configured_provider_name()
        provider = self._provider_factory(provider_name)
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
        )
