"""Thin service adapter over the validated Pulse intelligence engines."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Callable

import psycopg

from src.api.models import ApiErrorCode
from src.api.safety import is_aggregate_question, valid_business_id
from src.intelligence.answer_models import AnalystAnswer, AnswerValidationError
from src.intelligence.context import IntelligenceContext, build_context
from src.intelligence.decision_answers import (
    answer_decision_question,
    classify_decision_question,
)
from src.intelligence.decision_models import (
    DecisionReadinessAssessment,
    DecisionReadinessPortfolio,
    DecisionValidationError,
    valid_decision_id_shape,
)
from src.intelligence.decisions import build_decision_readiness_portfolio
from src.intelligence.investigation_answers import (
    answer_investigation_question,
    classify_investigation_question,
)
from src.intelligence.investigation_models import (
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationTask,
    InvestigationValidationError,
    valid_plan_id_shape,
    valid_task_id_shape,
)
from src.intelligence.investigations import build_investigation_portfolio
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
from src.intelligence.sequencing import build_investigation_sequencing_portfolio
from src.intelligence.sequencing_answers import (
    answer_sequencing_question,
    classify_sequencing_question,
)
from src.intelligence.sequencing_models import (
    EvidenceLeverageItem,
    InvestigationSequenceItem,
    InvestigationSequencingPortfolio,
    SequencingValidationError,
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
    "Evaluate whether evidence is sufficient for bounded human decision review",
    "Explain why a decision is or is not ready",
    "Identify evidence and investigations that could raise readiness",
    "Identify evidence shared across multiple decision-readiness gaps",
    "Show which investigations could produce relevant evidence",
    "Identify whether any readiness-raising investigation can begin now",
    "Explain why no next investigation is currently startable",
)

LIMITATIONS = (
    "Aggregate business evidence only; no customer-level analysis.",
    "No autonomous campaign, budget, refund, or customer-contact actions.",
    "No trusted cross-currency profit while FX_REQUIRED applies.",
    "No persistent conversation memory.",
    "Observed differences do not establish causal reasons.",
    "No autonomous decisions or recommendation execution.",
    "No automatic evidence collection or task execution.",
    "No guaranteed decision unlock or business-action recommendation.",
)

_SEQUENCING_REQUIREMENT_ID = re.compile(
    r"^requirement:[a-z0-9][a-z0-9_:-]*$"
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
    investigation_plan_id: str | None = None
    investigation_task_id: str | None = None
    investigation_readiness: str | None = None
    decision_id: str | None = None
    decision_type: str | None = None
    decision_class: str | None = None
    decision_readiness: str | None = None
    sequencing_requirement_id: str | None = None
    sequencing_task_id: str | None = None
    sequencing_state: str | None = None


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

    @staticmethod
    def _validate_investigation_plan_id(plan_id: str) -> None:
        if len(plan_id) > 240 or not valid_plan_id_shape(plan_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_INVESTIGATION_ID,
                "The request contains an invalid investigation plan identifier.",
                422,
            )

    @staticmethod
    def _validate_investigation_task_id(task_id: str) -> None:
        if len(task_id) > 300 or not valid_task_id_shape(task_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_INVESTIGATION_ID,
                "The request contains an invalid investigation task identifier.",
                422,
            )

    @staticmethod
    def _validate_decision_id(decision_id: str) -> None:
        if len(decision_id) > 300 or not valid_decision_id_shape(decision_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_DECISION_ID,
                "The request contains an invalid decision identifier.",
                422,
            )

    @staticmethod
    def _validate_sequencing_requirement_id(requirement_id: str) -> None:
        if (
            len(requirement_id) > 240
            or not _SEQUENCING_REQUIREMENT_ID.fullmatch(requirement_id)
        ):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_SEQUENCING_REQUIREMENT_ID,
                "The request contains an invalid sequencing requirement identifier.",
                422,
            )

    @staticmethod
    def _validate_sequencing_task_id(task_id: str) -> None:
        if len(task_id) > 300 or not valid_task_id_shape(task_id):
            raise AnalystServiceError(
                ApiErrorCode.INVALID_SEQUENCING_TASK_ID,
                "The request contains an invalid sequencing task identifier.",
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

    @staticmethod
    def _plan_portfolio(
        context: IntelligenceContext,
        evaluation: OpportunityEvaluation,
    ) -> InvestigationPortfolio:
        try:
            return build_investigation_portfolio(context, evaluation)
        except InvestigationValidationError:
            raise AnalystServiceError(
                ApiErrorCode.INVESTIGATION_VALIDATION_FAILED,
                "The investigation planner could not produce safely validated results.",
                422,
            ) from None

    def list_investigations(self, business_id: str) -> InvestigationPortfolio:
        self._validate_business_id(business_id)
        context = self._build_context(business_id)
        evaluation = self._evaluate(context)
        return self._plan_portfolio(context, evaluation)

    @staticmethod
    def _decision_portfolio(
        context: IntelligenceContext,
        evaluation: OpportunityEvaluation,
        investigation_portfolio: InvestigationPortfolio,
    ) -> DecisionReadinessPortfolio:
        try:
            return build_decision_readiness_portfolio(
                context, evaluation, investigation_portfolio
            )
        except DecisionValidationError:
            raise AnalystServiceError(
                ApiErrorCode.DECISION_VALIDATION_FAILED,
                "The decision-readiness engine could not produce safely validated results.",
                422,
            ) from None

    def list_decisions(self, business_id: str) -> DecisionReadinessPortfolio:
        self._validate_business_id(business_id)
        context = self._build_context(business_id)
        evaluation = self._evaluate(context)
        investigations = self._plan_portfolio(context, evaluation)
        return self._decision_portfolio(context, evaluation, investigations)

    @staticmethod
    def _sequencing_portfolio(
        context: IntelligenceContext,
        evaluation: OpportunityEvaluation,
        investigations: InvestigationPortfolio,
        decisions: DecisionReadinessPortfolio,
    ) -> InvestigationSequencingPortfolio:
        try:
            return build_investigation_sequencing_portfolio(
                context, evaluation, investigations, decisions
            )
        except SequencingValidationError:
            raise AnalystServiceError(
                ApiErrorCode.SEQUENCING_VALIDATION_FAILED,
                "The evidence-sequencing engine could not produce safely validated results.",
                422,
            ) from None

    def _build_sequencing_layers(
        self, business_id: str
    ) -> tuple[
        IntelligenceContext,
        OpportunityEvaluation,
        InvestigationPortfolio,
        DecisionReadinessPortfolio,
        InvestigationSequencingPortfolio,
    ]:
        self._validate_business_id(business_id)
        context = self._build_context(business_id)
        evaluation = self._evaluate(context)
        investigations = self._plan_portfolio(context, evaluation)
        decisions = self._decision_portfolio(context, evaluation, investigations)
        sequencing = self._sequencing_portfolio(
            context, evaluation, investigations, decisions
        )
        return context, evaluation, investigations, decisions, sequencing

    def list_sequencing(
        self, business_id: str
    ) -> InvestigationSequencingPortfolio:
        return self._build_sequencing_layers(business_id)[4]

    def _resolve_sequencing_context(
        self,
        business_id: str,
        requirement_id: str | None,
        task_id: str | None,
    ) -> tuple[
        IntelligenceContext,
        OpportunityEvaluation,
        InvestigationPortfolio,
        DecisionReadinessPortfolio,
        InvestigationSequencingPortfolio,
        EvidenceLeverageItem | None,
        InvestigationSequenceItem | None,
    ]:
        if requirement_id is not None:
            self._validate_sequencing_requirement_id(requirement_id)
        if task_id is not None:
            self._validate_sequencing_task_id(task_id)
        context, evaluation, investigations, decisions, sequencing = (
            self._build_sequencing_layers(business_id)
        )
        leverage_item = None
        sequence_item = None
        if requirement_id is not None:
            leverage_item = next(
                (
                    item for item in sequencing.evidence_leverage_items
                    if item.requirement_id == requirement_id
                ),
                None,
            )
            if leverage_item is None:
                raise AnalystServiceError(
                    ApiErrorCode.SEQUENCING_REQUIREMENT_NOT_FOUND,
                    "No active evidence focus is available for this identifier and business.",
                    404,
                )
        if task_id is not None:
            sequence_item = next(
                (item for item in sequencing.sequence_items if item.task_id == task_id),
                None,
            )
            if sequence_item is None:
                raise AnalystServiceError(
                    ApiErrorCode.SEQUENCING_TASK_NOT_FOUND,
                    "No active sequencing task is available for this identifier and business.",
                    404,
                )
        return (
            context,
            evaluation,
            investigations,
            decisions,
            sequencing,
            leverage_item,
            sequence_item,
        )

    def _resolve_decision(
        self, business_id: str, decision_id: str
    ) -> tuple[
        IntelligenceContext,
        OpportunityEvaluation,
        InvestigationPortfolio,
        DecisionReadinessPortfolio,
        DecisionReadinessAssessment,
        InvestigationOpportunity,
        InvestigationPlan,
    ]:
        self._validate_business_id(business_id)
        self._validate_decision_id(decision_id)
        context = self._build_context(business_id)
        evaluation = self._evaluate(context)
        investigations = self._plan_portfolio(context, evaluation)
        decisions = self._decision_portfolio(context, evaluation, investigations)
        assessment = next(
            (item for item in decisions.assessments if item.decision_id == decision_id),
            None,
        )
        if assessment is None or assessment.business_id != business_id:
            raise AnalystServiceError(
                ApiErrorCode.DECISION_NOT_FOUND,
                "No active decision-readiness assessment is available for this identifier and business.",
                404,
            )
        opportunity = next(
            (
                item for item in evaluation.opportunities
                if item.opportunity_id == assessment.originating_opportunity_id
            ),
            None,
        )
        plan = next(
            (
                item for item in investigations.plans
                if item.plan_id == assessment.investigation_plan_id
            ),
            None,
        )
        if (
            opportunity is None
            or plan is None
            or opportunity.business_id != business_id
            or plan.business_id != business_id
            or plan.opportunity_id != opportunity.opportunity_id
        ):
            raise AnalystServiceError(
                ApiErrorCode.DECISION_VALIDATION_FAILED,
                "The decision assessment could not be resolved safely.",
                422,
            )
        return (
            context,
            evaluation,
            investigations,
            decisions,
            assessment,
            opportunity,
            plan,
        )

    def get_decision(
        self, business_id: str, decision_id: str
    ) -> DecisionReadinessAssessment:
        return self._resolve_decision(business_id, decision_id)[4]

    def _resolve_investigation_plan(
        self, business_id: str, plan_id: str
    ) -> tuple[IntelligenceContext, InvestigationPortfolio, InvestigationPlan]:
        self._validate_business_id(business_id)
        self._validate_investigation_plan_id(plan_id)
        context = self._build_context(business_id)
        portfolio = self._plan_portfolio(context, self._evaluate(context))
        plan = next((item for item in portfolio.plans if item.plan_id == plan_id), None)
        if plan is None or plan.business_id != business_id:
            raise AnalystServiceError(
                ApiErrorCode.INVESTIGATION_PLAN_NOT_FOUND,
                "No active investigation plan is available for this identifier and business.",
                404,
            )
        return context, portfolio, plan

    def get_investigation_plan(
        self, business_id: str, plan_id: str
    ) -> InvestigationPlan:
        return self._resolve_investigation_plan(business_id, plan_id)[2]

    def _resolve_investigation_task(
        self, business_id: str, task_id: str
    ) -> tuple[
        IntelligenceContext,
        InvestigationPortfolio,
        InvestigationPlan,
        InvestigationTask,
    ]:
        self._validate_business_id(business_id)
        self._validate_investigation_task_id(task_id)
        context = self._build_context(business_id)
        portfolio = self._plan_portfolio(context, self._evaluate(context))
        for plan in portfolio.plans:
            task = next((item for item in plan.tasks if item.task_id == task_id), None)
            if task is not None and plan.business_id == business_id:
                return context, portfolio, plan, task
        raise AnalystServiceError(
            ApiErrorCode.INVESTIGATION_TASK_NOT_FOUND,
            "No active investigation task is available for this identifier and business.",
            404,
        )

    def get_investigation_task(
        self, business_id: str, task_id: str
    ) -> tuple[InvestigationPlan, InvestigationTask]:
        _, _, plan, task = self._resolve_investigation_task(business_id, task_id)
        return plan, task

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
        self,
        business_id: str,
        question: str,
        opportunity_id: str | None = None,
        investigation_task_id: str | None = None,
        decision_id: str | None = None,
        sequencing_requirement_id: str | None = None,
        sequencing_task_id: str | None = None,
    ) -> ServiceAnswer:
        self._validate_business_id(business_id)
        if sum(item is not None for item in (
            opportunity_id, investigation_task_id, decision_id,
            sequencing_requirement_id, sequencing_task_id,
        )) > 1:
            raise AnalystServiceError(
                ApiErrorCode.INVALID_REQUEST,
                "Choose only one opportunity, investigation, decision, or sequencing context.",
                422,
            )
        if not is_aggregate_question(question):
            raise AnalystServiceError(
                ApiErrorCode.AGGREGATE_ONLY_REQUIRED,
                "This analyst accepts aggregate business questions only.",
                422,
            )

        if sequencing_requirement_id is not None or sequencing_task_id is not None:
            started = time.perf_counter()
            (
                context,
                evaluation,
                investigations,
                decisions,
                sequencing,
                leverage_item,
                sequence_item,
            ) = self._resolve_sequencing_context(
                business_id,
                sequencing_requirement_id,
                sequencing_task_id,
            )
            try:
                answer = answer_sequencing_question(
                    question,
                    sequencing,
                    context,
                    evaluation,
                    investigations,
                    decisions,
                    leverage_item=leverage_item,
                    sequence_item=sequence_item,
                )
            except AnswerValidationError:
                raise AnalystServiceError(
                    ApiErrorCode.SEQUENCING_VALIDATION_FAILED,
                    "The analyst could not produce a safely grounded sequencing answer.",
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
                question_category=classify_sequencing_question(question).value,
                provider="deterministic",
                model="sequencing-engine-v1",
                evidence_count=len(context.evidence_items),
                answer_source="deterministic_evidence_sequencing",
                sequencing_requirement_id=(
                    leverage_item.requirement_id if leverage_item is not None else None
                ),
                sequencing_task_id=(
                    sequence_item.task_id if sequence_item is not None else None
                ),
                sequencing_state=sequencing.state.value,
            )

        if decision_id is not None:
            started = time.perf_counter()
            (
                context,
                _evaluation,
                investigations,
                decisions,
                assessment,
                opportunity,
                plan,
            ) = self._resolve_decision(business_id, decision_id)
            try:
                answer = answer_decision_question(
                    question,
                    assessment,
                    opportunity,
                    plan,
                    decisions,
                    investigations,
                    context,
                )
            except AnswerValidationError:
                raise AnalystServiceError(
                    ApiErrorCode.DECISION_VALIDATION_FAILED,
                    "The analyst could not produce a safely grounded decision answer.",
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
                question_category=classify_decision_question(question).value,
                provider="deterministic",
                model="decision-readiness-engine-v1",
                evidence_count=len(context.evidence_items),
                answer_source="deterministic_decision_readiness",
                opportunity_type=opportunity.opportunity_type.value,
                investigation_plan_id=plan.plan_id,
                decision_id=assessment.decision_id,
                decision_type=assessment.decision_type.value,
                decision_class=assessment.decision_class.value,
                decision_readiness=assessment.readiness.value,
            )

        if investigation_task_id is not None:
            started = time.perf_counter()
            context, portfolio, plan, task = self._resolve_investigation_task(
                business_id, investigation_task_id
            )
            try:
                answer = answer_investigation_question(
                    question, task, plan, portfolio, context
                )
            except (AnswerValidationError, InvestigationValidationError):
                raise AnalystServiceError(
                    ApiErrorCode.INVESTIGATION_VALIDATION_FAILED,
                    "The analyst could not produce a safely grounded investigation answer.",
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
                question_category=classify_investigation_question(question).value,
                provider="deterministic",
                model="investigation-engine-v1",
                evidence_count=len(context.evidence_items),
                answer_source="deterministic_investigation",
                investigation_plan_id=plan.plan_id,
                investigation_task_id=task.task_id,
                investigation_readiness=task.readiness.value,
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
