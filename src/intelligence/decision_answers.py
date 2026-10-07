"""Deterministic, provider-free answers about validated decision readiness."""

from __future__ import annotations

from enum import StrEnum
import re

from src.intelligence.answer_models import (
    AnalystAnswer,
    AnswerValidationError,
    ClaimType,
    Confidence,
    Finding,
)
from src.intelligence.context import IntelligenceContext
from src.intelligence.decision_models import (
    DecisionReadiness,
    DecisionReadinessAssessment,
    DecisionReadinessPortfolio,
)
from src.intelligence.investigation_models import (
    EvidenceRequirement,
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationTask,
)
from src.intelligence.narration import validate_answer
from src.intelligence.opportunity_models import InvestigationOpportunity


class DecisionQuestionIntent(StrEnum):
    DECISION_OVERVIEW = "DECISION_OVERVIEW"
    DECISION_WHY_READY = "DECISION_WHY_READY"
    DECISION_WHY_NOT_READY = "DECISION_WHY_NOT_READY"
    DECISION_MISSING_EVIDENCE = "DECISION_MISSING_EVIDENCE"
    DECISION_NEXT_EVIDENCE = "DECISION_NEXT_EVIDENCE"
    DECISION_BOUNDARY = "DECISION_BOUNDARY"
    DECISION_PRIORITY_VS_READINESS = "DECISION_PRIORITY_VS_READINESS"
    DECISION_HUMAN_REVIEW = "DECISION_HUMAN_REVIEW"


def classify_decision_question(question: str) -> DecisionQuestionIntent:
    """Classify the small explicit decision-readiness question vocabulary."""
    normalized = re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()
    if any(term in normalized for term in (
        "recommend", "approved", "approval", "authorize", "execute", "autonomous",
    )):
        return DecisionQuestionIntent.DECISION_HUMAN_REVIEW
    if "priority" in normalized and any(term in normalized for term in (
        "ready", "readiness", "evidence",
    )):
        return DecisionQuestionIntent.DECISION_PRIORITY_VS_READINESS
    if "boundary" in normalized or "blocked by" in normalized:
        return DecisionQuestionIntent.DECISION_BOUNDARY
    if any(term in normalized for term in (
        "what could raise", "raise readiness", "next evidence", "which investigation",
    )):
        return DecisionQuestionIntent.DECISION_NEXT_EVIDENCE
    if any(term in normalized for term in (
        "missing evidence", "evidence is missing", "what evidence", "which evidence",
    )):
        return DecisionQuestionIntent.DECISION_MISSING_EVIDENCE
    if any(term in normalized for term in (
        "not ready", "isn t ready", "is not ready", "needs more evidence",
    )):
        return DecisionQuestionIntent.DECISION_WHY_NOT_READY
    if "why" in normalized and "ready" in normalized:
        return DecisionQuestionIntent.DECISION_WHY_READY
    return DecisionQuestionIntent.DECISION_OVERVIEW


def _grounding_refs(
    assessment: DecisionReadinessAssessment,
    context: IntelligenceContext,
) -> tuple[str, ...]:
    refs = tuple(dict.fromkeys((
        *assessment.supporting_evidence_refs,
        *assessment.counter_evidence_refs,
        *assessment.blocking_evidence_refs,
    )))
    grounded = tuple(
        ref for ref in refs
        if ref in context.evidence_by_id
        and context.evidence_by_id[ref].business_id == context.business_id
    )
    if grounded != refs or not grounded:
        raise AnswerValidationError(
            "Decision answer requires validated same-business aggregate evidence"
        )
    return grounded


def _requirements(
    assessment: DecisionReadinessAssessment,
    plan: InvestigationPlan,
) -> tuple[EvidenceRequirement, ...]:
    by_id = {item.requirement_id: item for item in plan.evidence_requirements}
    try:
        return tuple(by_id[item] for item in assessment.unresolved_requirement_ids)
    except KeyError:
        raise AnswerValidationError(
            "Decision references an unknown evidence requirement"
        ) from None


def _tasks(
    assessment: DecisionReadinessAssessment,
    plan: InvestigationPlan,
) -> tuple[InvestigationTask, ...]:
    by_id = {item.task_id: item for item in plan.tasks}
    try:
        return tuple(by_id[item] for item in assessment.next_evidence_task_ids)
    except KeyError:
        raise AnswerValidationError(
            "Decision references an unknown investigation task"
        ) from None


def _requirement_text(requirement: EvidenceRequirement) -> str:
    missing = requirement.missing_reason or (
        "Not available in the current validated intelligence context."
    )
    return (
        f"{requirement.name}: {requirement.description} Status: "
        f"{requirement.status.value}. {missing}"
    )


def answer_decision_question(
    question: str,
    assessment: DecisionReadinessAssessment,
    opportunity: InvestigationOpportunity,
    plan: InvestigationPlan,
    decision_portfolio: DecisionReadinessPortfolio,
    investigation_portfolio: InvestigationPortfolio,
    context: IntelligenceContext,
) -> AnalystAnswer:
    """Answer from validated portfolio fields without reevaluating readiness."""
    normalized_question = question.strip()
    if not normalized_question:
        raise AnswerValidationError("Decision question cannot be blank")
    if assessment not in decision_portfolio.assessments:
        raise AnswerValidationError("Decision is not part of the validated portfolio")
    if plan not in investigation_portfolio.plans:
        raise AnswerValidationError("Decision plan is not part of the validated portfolio")
    if (
        assessment.business_id != context.business_id
        or decision_portfolio.business_id != context.business_id
        or investigation_portfolio.business_id != context.business_id
        or opportunity.business_id != context.business_id
        or plan.business_id != context.business_id
        or assessment.originating_opportunity_id != opportunity.opportunity_id
        or assessment.investigation_plan_id != plan.plan_id
        or plan.opportunity_id != opportunity.opportunity_id
    ):
        raise AnswerValidationError("Decision answer crosses a validated object boundary")

    intent = classify_decision_question(normalized_question)
    refs = _grounding_refs(assessment, context)
    missing = _requirements(assessment, plan)
    next_tasks = _tasks(assessment, plan)
    confidence = Confidence(opportunity.confidence.value)
    reason_text = ", ".join(
        item.value.replace("_", " ").lower()
        for item in assessment.readiness_reason_codes
    )
    limitations = (assessment.decision_boundary, assessment.limitation)
    steps: tuple[str, ...] = ()

    if intent is DecisionQuestionIntent.DECISION_WHY_READY:
        if assessment.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW:
            summary = (
                "This decision is ready for human review because the current validated "
                "evidence meets its bounded review threshold."
            )
            statement = assessment.rationale_summary
            if assessment.counter_evidence_refs or assessment.blocking_evidence_refs:
                limitations = (
                    *limitations,
                    "Counter or blocking context remains explicitly disclosed for the human reviewer.",
                )
        else:
            summary = (
                f"This decision is {assessment.readiness.value}; its bounded evidence "
                "threshold has not been met."
            )
            statement = assessment.rationale_summary
            limitations = (*limitations, *(_requirement_text(item) for item in missing))
            steps = tuple(f"{item.title}: {item.objective}" for item in next_tasks)
    elif intent is DecisionQuestionIntent.DECISION_WHY_NOT_READY:
        if assessment.readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY:
            summary = (
                "This decision is blocked by an explicit boundary, not merely waiting for "
                "additional evidence."
            )
            statement = assessment.decision_boundary
        elif assessment.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE:
            names = "; ".join(item.name for item in missing)
            requirement_summary = (
                f" Unresolved requirements: {names}." if names else ""
            )
            summary = (
                "The decision-specific evidence bar has not been met in the current "
                f"validated context.{requirement_summary}"
            )
            statement = (
                "The opportunity may still deserve investigation, but opportunity priority "
                "does not make this bounded decision ready for human review."
            )
        else:
            summary = "This decision is ready for human review under its bounded evidence bar."
            statement = assessment.rationale_summary
        limitations = (*limitations, *(_requirement_text(item) for item in missing))
        steps = tuple(f"{item.title}: {item.objective}" for item in next_tasks)
    elif intent is DecisionQuestionIntent.DECISION_MISSING_EVIDENCE:
        if missing:
            summary = (
                "These requirements are not available in the current validated intelligence "
                "context: " + "; ".join(item.name for item in missing) + "."
            )
            statement = " ".join(_requirement_text(item) for item in missing)
            limitations = (*limitations, *(_requirement_text(item) for item in missing))
            steps = tuple(item.collection_hint for item in missing)
        else:
            summary = "This decision has no unresolved hard evidence requirements."
            statement = assessment.rationale_summary
    elif intent is DecisionQuestionIntent.DECISION_NEXT_EVIDENCE:
        if next_tasks:
            summary = (
                "These investigations could produce evidence relevant to this readiness assessment."
            )
            statement = (
                "Task readiness means an investigation can begin; it does not mean the task "
                "has been completed or produced new evidence."
            )
            steps = tuple(f"{item.title}: {item.objective}" for item in next_tasks)
        else:
            summary = "No next-evidence tasks are attached to this assessment."
            statement = assessment.rationale_summary
    elif intent is DecisionQuestionIntent.DECISION_BOUNDARY:
        summary = assessment.decision_boundary
        statement = (
            "The boundary remains applicable regardless of readiness and prevents readiness "
            "from being interpreted as authorization."
        )
    elif intent is DecisionQuestionIntent.DECISION_PRIORITY_VS_READINESS:
        summary = (
            f"The originating opportunity is {opportunity.priority.value} priority while "
            f"this decision is {assessment.readiness.value}. These states answer different questions."
        )
        statement = (
            "Opportunity priority describes how urgently the issue deserves investigation. "
            "Decision readiness describes whether current evidence is sufficient to place "
            "this bounded question before a human reviewer."
        )
        limitations = (*limitations, *(_requirement_text(item) for item in missing))
    elif intent is DecisionQuestionIntent.DECISION_HUMAN_REVIEW:
        summary = (
            "No. Ready for human review does not mean Pulse recommends, approves, or "
            "authorizes a change."
        )
        statement = (
            "The status means only that current evidence satisfies this bounded review "
            "threshold; a human must review it and no option is selected."
        )
        limitations = (
            assessment.decision_boundary,
            "Human review is required and autonomous action is not allowed.",
            assessment.limitation,
        )
    else:
        summary = (
            f"{assessment.decision_question} Readiness: {assessment.readiness.value}. "
            f"{assessment.rationale_summary}"
        )
        statement = f"Readiness reasons: {reason_text}."

    answer = AnalystAnswer(
        question=normalized_question,
        answer_summary=summary,
        findings=(Finding(
            statement=statement,
            evidence_refs=refs,
            confidence=confidence,
            claim_type=ClaimType.INVESTIGATION,
            causal_claim=False,
        ),),
        investigation_steps=steps,
        limitations=limitations,
        confidence=confidence,
        cannot_answer_fully=(
            assessment.readiness is not DecisionReadiness.READY_FOR_HUMAN_REVIEW
        ),
        safety_notes=(
            "This answer consumes the validated decision-readiness assessment without recalculating readiness.",
            "Pulse does not select an option, execute an investigation, or authorize a business action.",
        ),
    )
    return validate_answer(answer, normalized_question, context)
