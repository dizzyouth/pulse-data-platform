"""Deterministic, provider-free answers about evidence sequencing."""

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
    DecisionReadinessAssessment,
    DecisionReadinessPortfolio,
)
from src.intelligence.investigation_models import InvestigationPortfolio
from src.intelligence.narration import validate_answer
from src.intelligence.opportunity_models import OpportunityEvaluation
from src.intelligence.sequencing_models import (
    EvidenceLeverageItem,
    InvestigationSequenceItem,
    InvestigationSequencingPortfolio,
    SequencingPortfolioState,
)


class SequencingQuestionIntent(StrEnum):
    SEQUENCING_OVERVIEW = "SEQUENCING_OVERVIEW"
    SEQUENCING_WHAT_LEARN_NEXT = "SEQUENCING_WHAT_LEARN_NEXT"
    SEQUENCING_TOP_EVIDENCE_FOCUS = "SEQUENCING_TOP_EVIDENCE_FOCUS"
    SEQUENCING_WHY_THIS_EVIDENCE = "SEQUENCING_WHY_THIS_EVIDENCE"
    SEQUENCING_AFFECTED_DECISIONS = "SEQUENCING_AFFECTED_DECISIONS"
    SEQUENCING_NEXT_STARTABLE_TASK = "SEQUENCING_NEXT_STARTABLE_TASK"
    SEQUENCING_WHY_NO_STARTABLE_TASK = "SEQUENCING_WHY_NO_STARTABLE_TASK"
    SEQUENCING_TASK_WHY_ORDERED = "SEQUENCING_TASK_WHY_ORDERED"
    SEQUENCING_TASK_CAN_BEGIN = "SEQUENCING_TASK_CAN_BEGIN"
    SEQUENCING_TASK_WHAT_COULD_HELP = "SEQUENCING_TASK_WHAT_COULD_HELP"
    SEQUENCING_GUARANTEE_BOUNDARY = "SEQUENCING_GUARANTEE_BOUNDARY"


def classify_sequencing_question(question: str) -> SequencingQuestionIntent:
    """Classify the explicit evidence-sequencing question vocabulary."""
    normalized = re.sub(r"[^a-z0-9]+", " ", question.lower()).strip()
    if any(term in normalized for term in (
        "guarantee", "guarantees", "unlock", "make ready", "makes ready",
    )):
        return SequencingQuestionIntent.SEQUENCING_GUARANTEE_BOUNDARY
    if any(term in normalized for term in (
        "nothing startable", "no next investigation", "no next startable",
        "there no next", "there is no next", "why no investigation",
    )):
        return SequencingQuestionIntent.SEQUENCING_WHY_NO_STARTABLE_TASK
    if any(term in normalized for term in (
        "which decisions", "affected decisions", "decisions depend",
    )):
        return SequencingQuestionIntent.SEQUENCING_AFFECTED_DECISIONS
    if any(term in normalized for term in (
        "why this focus", "why this evidence", "why is this the top",
    )):
        return SequencingQuestionIntent.SEQUENCING_WHY_THIS_EVIDENCE
    if "top evidence focus" in normalized:
        return SequencingQuestionIntent.SEQUENCING_TOP_EVIDENCE_FOCUS
    if any(term in normalized for term in (
        "why is this ordered", "why this task first", "why is this task first",
        "why ordered here",
    )):
        return SequencingQuestionIntent.SEQUENCING_TASK_WHY_ORDERED
    if any(term in normalized for term in (
        "can this begin", "can this investigation begin", "can this task begin",
        "is this startable",
    )):
        return SequencingQuestionIntent.SEQUENCING_TASK_CAN_BEGIN
    if any(term in normalized for term in (
        "what could this help", "help clarify", "what could this investigation",
    )):
        return SequencingQuestionIntent.SEQUENCING_TASK_WHAT_COULD_HELP
    if any(term in normalized for term in (
        "next startable", "investigation can start", "what can start",
    )):
        return SequencingQuestionIntent.SEQUENCING_NEXT_STARTABLE_TASK
    if any(term in normalized for term in (
        "what should we learn", "what should i learn", "learn next",
    )):
        return SequencingQuestionIntent.SEQUENCING_WHAT_LEARN_NEXT
    return SequencingQuestionIntent.SEQUENCING_OVERVIEW


_SMALL_NUMBERS = (
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight",
    "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
    "sixteen", "seventeen", "eighteen", "nineteen",
)
_TENS = ("", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety")
_ORDINALS = {
    1: "first", 2: "second", 3: "third", 4: "fourth", 5: "fifth",
    6: "sixth", 7: "seventh", 8: "eighth", 9: "ninth", 10: "tenth",
    11: "eleventh", 12: "twelfth", 13: "thirteenth", 14: "fourteenth",
    15: "fifteenth", 16: "sixteenth", 17: "seventeenth", 18: "eighteenth",
    19: "nineteenth", 20: "twentieth",
}


def _number_word(value: int) -> str:
    if value < 20:
        return _SMALL_NUMBERS[value]
    if value < 100:
        tens, remainder = divmod(value, 10)
        return _TENS[tens] + (f"-{_SMALL_NUMBERS[remainder]}" if remainder else "")
    if value < 1000:
        hundreds, remainder = divmod(value, 100)
        suffix = f" {_number_word(remainder)}" if remainder else ""
        return f"{_SMALL_NUMBERS[hundreds]} hundred{suffix}"
    return "the recorded number of"


def _ordinal_word(value: int) -> str:
    return _ORDINALS.get(value, f"position {_number_word(value)}")


def _state_label(state: SequencingPortfolioState) -> str:
    return {
        SequencingPortfolioState.READY_TASK_AVAILABLE: "Startable investigation available",
        SequencingPortfolioState.EVIDENCE_GAP_FIRST: "Evidence gap first",
        SequencingPortfolioState.BOUNDARY_ONLY: "Boundary-limited",
        SequencingPortfolioState.NO_OPEN_READINESS_GAPS: "No open readiness gaps",
    }[state]


def _validated_objects(
    sequencing_portfolio: InvestigationSequencingPortfolio,
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
    decision_portfolio: DecisionReadinessPortfolio,
    leverage_item: EvidenceLeverageItem | None,
    sequence_item: InvestigationSequenceItem | None,
) -> None:
    identities = (
        (sequencing_portfolio.business_id, sequencing_portfolio.as_of_date),
        (opportunity_evaluation.business_id, opportunity_evaluation.as_of_date),
        (investigation_portfolio.business_id, investigation_portfolio.as_of_date),
        (decision_portfolio.business_id, decision_portfolio.as_of_date),
    )
    if any(
        business_id != context.business_id or as_of_date != context.as_of_date
        for business_id, as_of_date in identities
    ):
        raise AnswerValidationError("Sequencing answer crosses a validated object boundary")
    if leverage_item is not None and leverage_item not in sequencing_portfolio.evidence_leverage_items:
        raise AnswerValidationError("Evidence focus is not part of the validated portfolio")
    if sequence_item is not None and sequence_item not in sequencing_portfolio.sequence_items:
        raise AnswerValidationError("Sequence task is not part of the validated portfolio")
    if leverage_item is not None and sequence_item is not None:
        raise AnswerValidationError("Choose one sequencing answer context")


def _decision_map(
    decision_portfolio: DecisionReadinessPortfolio,
) -> dict[str, DecisionReadinessAssessment]:
    return {item.decision_id: item for item in decision_portfolio.assessments}


def _grounding_refs(
    decision_ids: tuple[str, ...],
    decision_portfolio: DecisionReadinessPortfolio,
    context: IntelligenceContext,
) -> tuple[str, ...]:
    decisions = _decision_map(decision_portfolio)
    selected = []
    if decision_ids:
        for decision_id in decision_ids:
            assessment = decisions.get(decision_id)
            if assessment is None:
                raise AnswerValidationError("Sequencing answer references an unknown decision")
            selected.append(assessment)
    else:
        selected = list(decision_portfolio.assessments)
    refs: list[str] = []
    for assessment in selected:
        for ref in (
            *assessment.supporting_evidence_refs,
            *assessment.counter_evidence_refs,
            *assessment.blocking_evidence_refs,
        ):
            evidence = context.evidence_by_id.get(ref)
            if evidence is None or evidence.business_id != context.business_id:
                raise AnswerValidationError("Sequencing answer contains invalid evidence grounding")
            if ref not in refs:
                refs.append(ref)
    if not refs and context.evidence_items:
        refs.append(context.evidence_items[0].evidence_id)
    if not refs:
        raise AnswerValidationError("Sequencing answer requires aggregate evidence")
    return tuple(refs)


def _requirement_names(
    requirement_ids: tuple[str, ...],
    investigation_portfolio: InvestigationPortfolio,
) -> tuple[str, ...]:
    names = {
        requirement.requirement_id: requirement.name
        for plan in investigation_portfolio.plans
        for requirement in plan.evidence_requirements
    }
    try:
        return tuple(names[item] for item in requirement_ids)
    except KeyError:
        raise AnswerValidationError("Sequence task references an unknown requirement") from None


def _decision_descriptions(
    decision_ids: tuple[str, ...],
    decision_portfolio: DecisionReadinessPortfolio,
) -> tuple[str, ...]:
    decisions = _decision_map(decision_portfolio)
    try:
        return tuple(
            f"{decisions[item].decision_question} ({decisions[item].readiness.value})"
            for item in decision_ids
        )
    except KeyError:
        raise AnswerValidationError("Sequencing answer references an unknown decision") from None


def _safe_reason(item: InvestigationSequenceItem) -> str:
    return item.sequencing_reason.replace(
        str(item.affected_decision_count), _number_word(item.affected_decision_count)
    )


def answer_sequencing_question(
    question: str,
    sequencing_portfolio: InvestigationSequencingPortfolio,
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
    decision_portfolio: DecisionReadinessPortfolio,
    leverage_item: EvidenceLeverageItem | None = None,
    sequence_item: InvestigationSequenceItem | None = None,
) -> AnalystAnswer:
    """Answer only from validated sequencing and upstream objects."""
    normalized_question = question.strip()
    if not normalized_question:
        raise AnswerValidationError("Sequencing question cannot be blank")
    _validated_objects(
        sequencing_portfolio,
        context,
        opportunity_evaluation,
        investigation_portfolio,
        decision_portfolio,
        leverage_item,
        sequence_item,
    )
    intent = classify_sequencing_question(normalized_question)
    leverage_by_id = {
        item.requirement_id: item
        for item in sequencing_portfolio.evidence_leverage_items
    }
    sequence_by_id = {
        item.task_id: item for item in sequencing_portfolio.sequence_items
    }
    top = leverage_by_id.get(sequencing_portfolio.top_evidence_focus_requirement_id)
    recommended = sequence_by_id.get(sequencing_portfolio.recommended_next_task_id)
    focus = leverage_item or top
    task = sequence_item or recommended
    decision_ids = (
        sequence_item.affected_decision_ids if sequence_item is not None
        else leverage_item.affected_decision_ids if leverage_item is not None
        else task.affected_decision_ids if task is not None
        else focus.affected_decision_ids if focus is not None
        else sequencing_portfolio.targeted_needs_more_evidence_decision_ids
    )
    refs = _grounding_refs(decision_ids, decision_portfolio, context)
    limitations = [sequencing_portfolio.limitation]
    if focus is not None:
        limitations.append(focus.limitation)
    if task is not None:
        limitations.append(task.limitation)
    summary = f"Portfolio state: {_state_label(sequencing_portfolio.state)}."
    statement = "The portfolio uses only current validated decision-readiness associations."
    cannot_answer_fully = False

    if intent is SequencingQuestionIntent.SEQUENCING_WHAT_LEARN_NEXT:
        if recommended is not None:
            names = _requirement_names(
                recommended.addressed_requirement_ids, investigation_portfolio
            )
            summary = f"Next startable investigation: {recommended.task_title}."
            statement = (
                f"Its current readiness is {recommended.task_readiness.value}; it can begin now. "
                f"It is associated with {_number_word(recommended.affected_decision_count)} "
                "current non-ready decision assessments and these unresolved requirements: "
                + "; ".join(names) + "."
            )
        elif top is not None:
            summary = (
                "No readiness-raising investigation can currently begin. "
                f"Top evidence focus: {top.requirement_name}."
            )
            statement = (
                f"This focus is referenced by {_number_word(top.affected_decision_count)} "
                "current non-ready decisions across "
                f"{_number_word(top.affected_opportunity_count)} opportunities."
            )
            cannot_answer_fully = True
        else:
            summary = "No current unresolved evidence focus or startable investigation is available."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
    elif intent is SequencingQuestionIntent.SEQUENCING_TOP_EVIDENCE_FOCUS:
        if top is None:
            summary = "There is no current top evidence focus."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
        else:
            summary = f"Top evidence focus: {top.requirement_name}."
            statement = (
                f"It is referenced by {_number_word(top.affected_decision_count)} current "
                f"non-ready decisions across {_number_word(top.affected_opportunity_count)} "
                f"opportunities. The highest associated opportunity priority is "
                f"{top.highest_opportunity_priority.value}. Shared across decisions: "
                f"{'Yes' if top.shared_across_decisions else 'No'}. Shared across "
                f"opportunities: {'Yes' if top.shared_across_opportunities else 'No'}. "
                "This ranking reflects evidence breadth, not expected business value."
            )
    elif intent is SequencingQuestionIntent.SEQUENCING_WHY_THIS_EVIDENCE:
        if focus is None:
            summary = "There is no current evidence focus to explain."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
        else:
            position = next(
                index for index, item in enumerate(
                    sequencing_portfolio.evidence_leverage_items, 1
                ) if item.requirement_id == focus.requirement_id
            )
            summary = f"Evidence focus: {focus.requirement_name}."
            statement = (
                f"This focus is referenced by {_number_word(focus.affected_decision_count)} "
                f"current non-ready decisions across {_number_word(focus.affected_opportunity_count)} "
                "opportunities. The ordering first compares affected current "
                "NEEDS_MORE_EVIDENCE decisions, "
                "then opportunity priority, affected-opportunity breadth, stable appearance, "
                f"and the stable requirement identifier. This focus is {_ordinal_word(position)} "
                "under those transparent rules. The ranking is not an expected-value or ROI score."
            )
    elif intent is SequencingQuestionIntent.SEQUENCING_AFFECTED_DECISIONS:
        if focus is None:
            summary = "There is no current evidence focus with affected decisions."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
        else:
            descriptions = _decision_descriptions(
                focus.affected_decision_ids, decision_portfolio
            )
            summary = (
                f"{focus.requirement_name} is referenced by "
                f"{_number_word(focus.affected_decision_count)} current non-ready decisions."
            )
            statement = (
                "These current non-ready decisions reference this unresolved requirement: "
                + "; ".join(descriptions) + "."
            )
    elif intent is SequencingQuestionIntent.SEQUENCING_NEXT_STARTABLE_TASK:
        if recommended is None:
            summary = "No readiness-raising investigation can currently begin."
            statement = (
                "No sequence item is marked startable in the current validated portfolio."
            )
            cannot_answer_fully = True
        else:
            summary = f"Next startable investigation: {recommended.task_title}."
            statement = (
                f"Its current task readiness is {recommended.task_readiness.value}, and its "
                f"can-begin-now field is Yes. {_safe_reason(recommended)}"
            )
    elif intent is SequencingQuestionIntent.SEQUENCING_WHY_NO_STARTABLE_TASK:
        if recommended is not None:
            summary = f"A startable investigation is available: {recommended.task_title}."
            statement = (
                f"Its current task readiness is {recommended.task_readiness.value}, and its "
                "can-begin-now field is Yes."
            )
        elif sequencing_portfolio.state is SequencingPortfolioState.EVIDENCE_GAP_FIRST:
            summary = (
                "There are current decision-readiness gaps, but no readiness-raising "
                "investigation can begin with the current validated context."
            )
            statement = (
                "Current non-ready decisions, unresolved evidence focuses, and associated "
                "investigation tasks remain in the portfolio. Every sequenced task is currently "
                "blocked by missing evidence in the validated context, so no task is presented "
                "as the next startable investigation."
            )
            cannot_answer_fully = True
        else:
            summary = "No next startable investigation is present in the current portfolio."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
            cannot_answer_fully = True
    elif intent is SequencingQuestionIntent.SEQUENCING_TASK_WHY_ORDERED:
        if task is None:
            summary = "There is no current sequence task to explain."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
        else:
            names = _requirement_names(
                task.addressed_requirement_ids, investigation_portfolio
            )
            summary = (
                f"{task.task_title} is {_ordinal_word(task.sequence_order)} in the current sequence."
            )
            statement = (
                f"{_safe_reason(task)} Addressed unresolved requirements: "
                + ("; ".join(names) if names else "none recorded") + "."
            )
    elif intent is SequencingQuestionIntent.SEQUENCING_TASK_CAN_BEGIN:
        if task is None:
            summary = "There is no current sequence task to evaluate."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
        else:
            answer = "Yes" if task.can_begin_now else "No"
            summary = f"Can this investigation begin now? {answer}."
            statement = (
                f"The engine reports task readiness {task.task_readiness.value} and "
                f"can begin now: {answer}. {task.limitation}"
            )
            cannot_answer_fully = not task.can_begin_now
    elif intent is SequencingQuestionIntent.SEQUENCING_TASK_WHAT_COULD_HELP:
        if task is None:
            summary = "There is no current sequence task to explain."
            statement = f"The current portfolio state is {_state_label(sequencing_portfolio.state)}."
        else:
            names = _requirement_names(
                task.addressed_requirement_ids, investigation_portfolio
            )
            summary = f"{task.task_title} could produce evidence relevant to current gaps."
            statement = (
                f"It is associated with {_number_word(task.affected_decision_count)} current "
                "non-ready decision assessments and these unresolved requirements: "
                + ("; ".join(names) if names else "none recorded") + "."
            )
            cannot_answer_fully = not task.can_begin_now
    elif intent is SequencingQuestionIntent.SEQUENCING_GUARANTEE_BOUNDARY:
        summary = "No guarantee. Evidence relevance does not guarantee decision readiness."
        if sequence_item is not None:
            statement = (
                f"{sequence_item.task_title} may produce relevant evidence. Decision "
                "readiness must be recalculated only after validated evidence actually exists; "
                "task readiness is not completion."
            )
        elif focus is not None:
            statement = (
                f"{focus.requirement_name} is relevant to current evidence gaps. Decision "
                "readiness must be recalculated only after validated evidence actually exists; "
                "task readiness is not completion."
            )
        else:
            statement = (
                "The sequencing portfolio identifies relevant evidence relationships only. "
                "Decision readiness must be recalculated after validated evidence exists."
            )
        cannot_answer_fully = True
    else:
        top_name = top.requirement_name if top is not None else "None"
        next_title = recommended.task_title if recommended is not None else "None currently available"
        summary = (
            f"Portfolio state: {_state_label(sequencing_portfolio.state)}. "
            f"Top evidence focus: {top_name}. Next startable investigation: {next_title}."
        )
        statement = (
            f"The portfolio has {_number_word(sequencing_portfolio.startable_task_count)} "
            "startable readiness-raising investigations and "
            f"{_number_word(len(sequencing_portfolio.boundary_blocked_decision_ids))} "
            "boundary-blocked decisions."
        )
        if sequencing_portfolio.state is SequencingPortfolioState.EVIDENCE_GAP_FIRST:
            summary += (
                " There are current decision-readiness gaps, but no readiness-raising "
                "investigation can begin with the current validated context."
            )
            cannot_answer_fully = True

    answer = AnalystAnswer(
        question=normalized_question,
        answer_summary=summary,
        findings=(Finding(
            statement=statement,
            evidence_refs=refs,
            confidence=Confidence.MEDIUM,
            claim_type=ClaimType.INVESTIGATION,
            causal_claim=False,
        ),),
        investigation_steps=(),
        limitations=tuple(dict.fromkeys(limitations)),
        confidence=Confidence.MEDIUM,
        cannot_answer_fully=cannot_answer_fully,
        safety_notes=(
            "Task readiness is not completion.",
            "Evidence relevance does not guarantee a decision-readiness change.",
            "Pulse does not execute investigations or select business actions.",
        ),
    )
    return validate_answer(answer, normalized_question, context)
