"""Deterministic, provider-free answers about validated investigation tasks."""

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
from src.intelligence.investigation_models import (
    EvidenceRequirement,
    EvidenceRequirementStatus,
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationReadiness,
    InvestigationTask,
)
from src.intelligence.narration import validate_answer


class InvestigationQuestionIntent(StrEnum):
    INVESTIGATION_OVERVIEW = "INVESTIGATION_OVERVIEW"
    INVESTIGATION_WHY_READY = "INVESTIGATION_WHY_READY"
    INVESTIGATION_WHY_BLOCKED = "INVESTIGATION_WHY_BLOCKED"
    INVESTIGATION_MISSING_EVIDENCE = "INVESTIGATION_MISSING_EVIDENCE"
    INVESTIGATION_EXPECTED_OUTPUT = "INVESTIGATION_EXPECTED_OUTPUT"
    INVESTIGATION_COMPLETION = "INVESTIGATION_COMPLETION"
    INVESTIGATION_CONFIRM_REFUTE = "INVESTIGATION_CONFIRM_REFUTE"
    INVESTIGATION_DECISION = "INVESTIGATION_DECISION"


def classify_investigation_question(question: str) -> InvestigationQuestionIntent:
    """Classify the bounded investigation-task question vocabulary."""
    normalized = re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()
    if any(term in normalized for term in ("confirm", "refute", "strengthen", "weaken")):
        return InvestigationQuestionIntent.INVESTIGATION_CONFIRM_REFUTE
    if any(term in normalized for term in ("decision", "unlock")):
        return InvestigationQuestionIntent.INVESTIGATION_DECISION
    if any(term in normalized for term in ("complete", "completion", "done", "finish")):
        return InvestigationQuestionIntent.INVESTIGATION_COMPLETION
    if any(term in normalized for term in ("produce", "expected output", "result")):
        return InvestigationQuestionIntent.INVESTIGATION_EXPECTED_OUTPUT
    if any(term in normalized for term in ("missing", "what data", "which data", "evidence gap")):
        return InvestigationQuestionIntent.INVESTIGATION_MISSING_EVIDENCE
    if any(term in normalized for term in ("blocked", "cannot", "can t")):
        return InvestigationQuestionIntent.INVESTIGATION_WHY_BLOCKED
    if "ready" in normalized or "why can i" in normalized or "begin now" in normalized:
        return InvestigationQuestionIntent.INVESTIGATION_WHY_READY
    return InvestigationQuestionIntent.INVESTIGATION_OVERVIEW


def _grounding_refs(
    task: InvestigationTask,
    plan: InvestigationPlan,
    context: IntelligenceContext,
) -> tuple[str, ...]:
    refs = list(task.available_evidence_refs)
    if not refs:
        refs.extend(
            ref
            for requirement in plan.evidence_requirements
            for ref in requirement.evidence_refs
        )
    grounded = tuple(dict.fromkeys(
        ref for ref in refs
        if ref in context.evidence_by_id
        and context.evidence_by_id[ref].business_id == context.business_id
    ))
    if not grounded:
        raise AnswerValidationError(
            "Investigation answer has no validated aggregate grounding evidence"
        )
    return grounded


def _missing_requirements(
    task: InvestigationTask,
    plan: InvestigationPlan,
) -> tuple[EvidenceRequirement, ...]:
    by_id = {item.requirement_id: item for item in plan.evidence_requirements}
    try:
        return tuple(by_id[item] for item in task.missing_requirement_ids)
    except KeyError:
        raise AnswerValidationError(
            "Investigation task references an unknown evidence requirement"
        ) from None


def _requirement_limitations(
    requirements: tuple[EvidenceRequirement, ...],
) -> tuple[str, ...]:
    return tuple(
        f"{item.name}: {item.description} {item.missing_reason}"
        for item in requirements
        if item.missing_reason is not None
    )


def answer_investigation_question(
    question: str,
    task: InvestigationTask,
    plan: InvestigationPlan,
    portfolio: InvestigationPortfolio,
    context: IntelligenceContext,
) -> AnalystAnswer:
    """Answer from existing plan fields, then apply the grounded-answer validator."""
    normalized_question = question.strip()
    if not normalized_question:
        raise AnswerValidationError("Investigation question cannot be blank")
    if plan not in portfolio.plans or task not in plan.tasks:
        raise AnswerValidationError("Investigation task is not part of the validated portfolio")
    if plan.business_id != context.business_id or portfolio.business_id != context.business_id:
        raise AnswerValidationError("Investigation answer crosses a business boundary")

    intent = classify_investigation_question(normalized_question)
    refs = _grounding_refs(task, plan, context)
    missing = _missing_requirements(task, plan)
    confidence = Confidence(plan.opportunity_confidence.value)
    completion = task.completion_criteria
    limitations = (task.limitation, plan.limitation)
    steps = completion

    if intent is InvestigationQuestionIntent.INVESTIGATION_WHY_READY:
        if task.readiness is InvestigationReadiness.READY_NOW:
            summary = "This task can begin with the current validated aggregate evidence."
            statement = (
                f"The available evidence supports beginning this bounded check: {task.objective}"
            )
        else:
            summary = "This task is not fully ready in the current validated intelligence context."
            statement = (
                "The active investigation is grounded in aggregate evidence, but this task "
                "still has unresolved evidence requirements."
            )
            limitations = (*limitations, *_requirement_limitations(missing))
    elif intent in {
        InvestigationQuestionIntent.INVESTIGATION_WHY_BLOCKED,
        InvestigationQuestionIntent.INVESTIGATION_MISSING_EVIDENCE,
    }:
        if task.readiness is InvestigationReadiness.BLOCKED_BOUNDARY:
            summary = (
                "This task is blocked by a validated safety or economic boundary in the "
                "current intelligence context."
            )
        elif missing:
            names = "; ".join(item.name for item in missing)
            summary = (
                "This investigation cannot be completed from the current validated "
                f"intelligence context because these requirements are missing: {names}."
            )
        else:
            summary = "This task is not blocked by a missing evidence requirement."
        statement = (
            "The active investigation is grounded in validated aggregate evidence; missing "
            "requirements are limitations and do not mean the evidence does not exist elsewhere."
        )
        limitations = (*limitations, *_requirement_limitations(missing))
        hints = tuple(item.collection_hint for item in missing)
        if hints:
            steps = hints
    elif intent is InvestigationQuestionIntent.INVESTIGATION_EXPECTED_OUTPUT:
        summary = task.expected_output
        statement = (
            "This is the expected analytical output of the task, not a predetermined "
            "business conclusion."
        )
    elif intent is InvestigationQuestionIntent.INVESTIGATION_COMPLETION:
        summary = "The task is complete when its existing bounded completion criteria are met."
        statement = " ".join(completion)
    elif intent is InvestigationQuestionIntent.INVESTIGATION_CONFIRM_REFUTE:
        if task.strengthens_criteria or task.weakens_criteria:
            summary = (
                "The task can strengthen or weaken the existing opportunity hypothesis; "
                "it cannot prove a cause."
            )
            statement = " ".join((
                *(f"Would strengthen: {item}" for item in task.strengthens_criteria),
                *(f"Would weaken: {item}" for item in task.weakens_criteria),
            ))
            steps = (*task.strengthens_criteria, *task.weakens_criteria)
        else:
            summary = "This task contributes contextual or coverage evidence to the investigation."
            statement = task.expected_output
    elif intent is InvestigationQuestionIntent.INVESTIGATION_DECISION:
        summary = (
            "Better evidence from this investigation may support a future decision; this "
            "answer does not prescribe that decision."
        )
        statement = f"The future decision the plan could support is: {plan.decision_unlocked}"
        steps = completion
    else:
        summary = f"{task.title} is a {task.readiness.value} investigation task."
        statement = task.objective

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
        cannot_answer_fully=(task.readiness is not InvestigationReadiness.READY_NOW),
        safety_notes=(
            "This answer uses the existing deterministic investigation plan and does not re-plan the task.",
            "Pulse does not execute this task or authorize a business action.",
        ),
    )
    return validate_answer(answer, normalized_question, context)
