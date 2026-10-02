"""Deterministic, provider-free narration for validated opportunities."""

from __future__ import annotations

from enum import StrEnum
import re

from src.intelligence.answer_models import (
    AnalystAnswer,
    ClaimType,
    Confidence,
    Finding,
)
from src.intelligence.context import IntelligenceContext
from src.intelligence.narration import (
    SafetyValidationError,
    ValidationErrorCode,
    validate_answer,
)
from src.intelligence.opportunities import validate_opportunity
from src.intelligence.opportunity_models import InvestigationOpportunity


class OpportunityQuestionIntent(StrEnum):
    OPPORTUNITY_OVERVIEW = "OPPORTUNITY_OVERVIEW"
    OPPORTUNITY_WHY = "OPPORTUNITY_WHY"
    OPPORTUNITY_CONFIRM = "OPPORTUNITY_CONFIRM"
    OPPORTUNITY_REFUTE = "OPPORTUNITY_REFUTE"
    OPPORTUNITY_INVESTIGATE = "OPPORTUNITY_INVESTIGATE"
    OPPORTUNITY_DECISION = "OPPORTUNITY_DECISION"


def classify_opportunity_question(question: str) -> OpportunityQuestionIntent:
    """Classify a small, explicit opportunity question vocabulary."""
    normalized = re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()
    if any(term in normalized for term in ("refute", "weaken", "disprove")):
        return OpportunityQuestionIntent.OPPORTUNITY_REFUTE
    if any(term in normalized for term in ("confirm", "strengthen", "support hypothesis")):
        return OpportunityQuestionIntent.OPPORTUNITY_CONFIRM
    if any(term in normalized for term in ("investigate", "next step", "look into")):
        return OpportunityQuestionIntent.OPPORTUNITY_INVESTIGATE
    if any(term in normalized for term in ("decision", "unlock")):
        return OpportunityQuestionIntent.OPPORTUNITY_DECISION
    if "why" in normalized:
        return OpportunityQuestionIntent.OPPORTUNITY_WHY
    return OpportunityQuestionIntent.OPPORTUNITY_OVERVIEW


def _refs(opportunity: InvestigationOpportunity) -> tuple[str, ...]:
    return tuple(dict.fromkeys((
        *opportunity.supporting_evidence_refs,
        *opportunity.counter_evidence_refs,
        *opportunity.blocking_evidence_refs,
    )))


def _joined(prefix: str, values: tuple[str, ...]) -> str:
    return prefix + " " + " ".join(values)


def answer_opportunity_question(
    question: str,
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
) -> AnalystAnswer:
    """Build and validate one answer using only the selected opportunity."""
    normalized_question = question.strip()
    validate_opportunity(opportunity, context)
    intent = classify_opportunity_question(normalized_question)
    refs = _refs(opportunity)
    confidence = Confidence(opportunity.confidence.value)
    limitations = (opportunity.limitation,)
    safety_notes = (
        "The hypothesis remains untested; this answer does not establish a cause.",
        "Any decision described is a future decision that better aggregate evidence could support, not an instruction.",
    )

    if intent in {
        OpportunityQuestionIntent.OPPORTUNITY_OVERVIEW,
        OpportunityQuestionIntent.OPPORTUNITY_WHY,
    }:
        summary = (
            f"{opportunity.title}: {opportunity.observation_summary} It is a "
            f"{opportunity.priority.value} priority, {opportunity.confidence.value} "
            "confidence investigation opportunity, not a guaranteed business impact."
        )
        statement = (
            f"{opportunity.observation_summary} The current opportunity tests whether "
            f"{opportunity.hypothesis_to_test}"
        )
        steps = opportunity.investigation_steps
    elif intent is OpportunityQuestionIntent.OPPORTUNITY_CONFIRM:
        summary = "The hypothesis is untested. The listed evidence would strengthen it if observed."
        statement = _joined(
            "Evidence that would strengthen the hypothesis:",
            opportunity.confirmation_criteria,
        )
        steps = opportunity.confirmation_criteria
    elif intent is OpportunityQuestionIntent.OPPORTUNITY_REFUTE:
        summary = "The hypothesis is untested. The listed evidence would weaken or refute it if observed."
        statement = _joined(
            "Evidence that would weaken or refute the hypothesis:",
            opportunity.refutation_criteria,
        )
        steps = opportunity.refutation_criteria
    elif intent is OpportunityQuestionIntent.OPPORTUNITY_INVESTIGATE:
        summary = "Investigate the opportunity through the bounded aggregate checks defined by the opportunity engine."
        statement = (
            "The current aggregate observation warrants investigation, but the evidence "
            "does not establish the cause."
        )
        steps = opportunity.investigation_steps
        if opportunity.missing_evidence:
            limitations = (
                opportunity.limitation,
                _joined("Missing evidence:", opportunity.missing_evidence),
            )
    else:
        summary = (
            "Better evidence could support a future decision, but this answer does not "
            "prescribe that decision."
        )
        statement = (
            "The future decision this evidence could support is: "
            f"{opportunity.decision_unlocked}"
        )
        steps = (
            "Gather the confirmation or refutation evidence before considering that future decision.",
        )

    answer = AnalystAnswer(
        question=normalized_question,
        answer_summary=summary,
        findings=(Finding(
            statement=statement,
            evidence_refs=refs,
            confidence=confidence,
            claim_type=ClaimType.OBSERVATION,
            causal_claim=False,
        ),),
        investigation_steps=steps,
        limitations=limitations,
        confidence=confidence,
        cannot_answer_fully=True,
        safety_notes=safety_notes,
    )
    try:
        return validate_answer(answer, normalized_question, context)
    except SafetyValidationError as error:
        if (
            intent not in {
                OpportunityQuestionIntent.OPPORTUNITY_OVERVIEW,
                OpportunityQuestionIntent.OPPORTUNITY_WHY,
            }
            or error.codes != (ValidationErrorCode.UNSUPPORTED_NUMBER,)
        ):
            raise
        # Some 6.7A observations contain a traceable derived ratio. The existing
        # analyst validator intentionally permits only numbers present directly in
        # context, so keep its contract unchanged and use a nonnumeric overview.
        safe_answer = AnalystAnswer(
            question=normalized_question,
            answer_summary=(
                f"{opportunity.title} is a {opportunity.priority.value} priority, "
                f"{opportunity.confidence.value} confidence investigation opportunity, "
                "not a guaranteed business impact."
            ),
            findings=(Finding(
                statement=(
                    "The validated aggregate observation in this opportunity warrants "
                    "investigation. The current opportunity tests whether "
                    f"{opportunity.hypothesis_to_test}"
                ),
                evidence_refs=refs,
                confidence=confidence,
                claim_type=ClaimType.OBSERVATION,
                causal_claim=False,
            ),),
            investigation_steps=steps,
            limitations=limitations,
            confidence=confidence,
            cannot_answer_fully=True,
            safety_notes=safety_notes,
        )
        return validate_answer(safe_answer, normalized_question, context)
