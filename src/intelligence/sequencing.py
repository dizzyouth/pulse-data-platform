"""Deterministic evidence leverage and readiness-raising task sequencing."""

from __future__ import annotations

from dataclasses import dataclass
import re

from src.intelligence.context import IntelligenceContext
from src.intelligence.decision_models import (
    DecisionReadiness,
    DecisionReadinessAssessment,
    DecisionReadinessPortfolio,
)
from src.intelligence.investigation_models import (
    EvidenceGap,
    EvidenceRequirement,
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationReadiness,
    InvestigationTask,
)
from src.intelligence.opportunity_models import (
    InvestigationOpportunity,
    OpportunityEvaluation,
    OpportunityPriority,
)
from src.intelligence.sequencing_models import (
    EvidenceLeverageItem,
    InvestigationSequenceItem,
    InvestigationSequencingPortfolio,
    SequencingPortfolioState,
    SequencingValidationError,
)


_PRIORITY_ORDER = {
    OpportunityPriority.HIGH: 0,
    OpportunityPriority.MEDIUM: 1,
    OpportunityPriority.LOW: 2,
}
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE = re.compile(r"(?<!\w)(?:\+?\d[\d .()\-]{7,}\d)(?!\w)")
_PII = re.compile(
    r"\b(?:customer|lead|order|shipment|tracking)[ _-]?(?:id|number|name)|"
    r"phone number|email address|street address|raw row\b",
    re.IGNORECASE,
)
_FORBIDDEN_ACTION = re.compile(
    r"\b(?:pause (?:the )?campaign|change (?:the )?budget|contact customers?|"
    r"issue refunds?|switch providers?|execute (?:a )?query|trigger etl|"
    r"collect automatically|deploy (?:a )?change|apply (?:the )?recommendation)\b",
    re.IGNORECASE,
)
_FALSE_UNLOCK = re.compile(
    r"\b(?:will|guaranteed to)\s+(?:unlock|resolve|make ready|increase readiness)\b",
    re.IGNORECASE,
)
_CAUSAL_ASSERTION = re.compile(
    r"\b(?:is|are|was|were) (?:the )?(?:cause|reason)\b|\bcaused by\b",
    re.IGNORECASE,
)
_FINANCIAL_ASSERTION = re.compile(
    r"\b(?:expected|projected|guaranteed)\s+(?:roi|roas|mer|profit|revenue|"
    r"contribution|upside)|(?:roi|roas|mer|profit|revenue|contribution)\s+"
    r"(?:will|would)\s+(?:increase|improve|grow)\b",
    re.IGNORECASE,
)

_LEVERAGE_LIMITATION = (
    "This breadth count shows where unresolved evidence is relevant; obtaining it "
    "does not guarantee that any decision will become ready."
)
_TASK_LIMITATION = (
    "Task readiness is not completion, and performing this investigation does not "
    "guarantee that a requirement will be resolved or a decision will become ready."
)
_PORTFOLIO_LIMITATION = (
    "This portfolio sequences bounded analytical learning only. It does not execute "
    "tasks, acquire evidence automatically, select a business action, or guarantee "
    "that decision readiness will change."
)


@dataclass(frozen=True, slots=True)
class _SourceIndexes:
    opportunities: dict[str, InvestigationOpportunity]
    plans: dict[str, InvestigationPlan]
    tasks: dict[str, tuple[InvestigationPlan, InvestigationTask]]
    requirements: dict[tuple[str, str], EvidenceRequirement]
    gaps: dict[str, EvidenceGap]
    decisions: dict[str, DecisionReadinessAssessment]


def _unique_index(items: tuple[object, ...], attribute: str, label: str) -> dict[str, object]:
    result: dict[str, object] = {}
    for item in items:
        key = getattr(item, attribute)
        if key in result:
            raise SequencingValidationError(f"Duplicate {label}: {key}")
        result[key] = item
    return result


def _validate_source_graph(
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
    decision_portfolio: DecisionReadinessPortfolio,
) -> _SourceIndexes:
    """Validate the typed cross-layer graph needed by sequencing."""
    if not isinstance(context, IntelligenceContext):
        raise SequencingValidationError("A validated IntelligenceContext is required")
    if not isinstance(opportunity_evaluation, OpportunityEvaluation):
        raise SequencingValidationError("A validated OpportunityEvaluation is required")
    if not isinstance(investigation_portfolio, InvestigationPortfolio):
        raise SequencingValidationError("A validated InvestigationPortfolio is required")
    if not isinstance(decision_portfolio, DecisionReadinessPortfolio):
        raise SequencingValidationError("A validated DecisionReadinessPortfolio is required")
    identities = (
        (opportunity_evaluation.business_id, opportunity_evaluation.as_of_date),
        (investigation_portfolio.business_id, investigation_portfolio.as_of_date),
        (decision_portfolio.business_id, decision_portfolio.as_of_date),
    )
    if any(
        business_id != context.business_id or as_of_date != context.as_of_date
        for business_id, as_of_date in identities
    ):
        raise SequencingValidationError("Sequencing inputs cross business or date boundaries")

    opportunities = _unique_index(
        opportunity_evaluation.opportunities, "opportunity_id", "opportunity ID"
    )
    if any(
        item.business_id != context.business_id or item.as_of_date != context.as_of_date
        for item in opportunity_evaluation.opportunities
    ):
        raise SequencingValidationError("Opportunity crosses business or date boundaries")
    opportunity_orders = [
        item.opportunity_order for item in opportunity_evaluation.opportunities
    ]
    if len(opportunity_orders) != len(set(opportunity_orders)):
        raise SequencingValidationError("Opportunity orders cannot duplicate")

    plan_by_id = _unique_index(
        investigation_portfolio.plans, "plan_id", "investigation plan ID"
    )
    plan_opportunity_ids = tuple(
        plan.opportunity_id for plan in investigation_portfolio.plans
    )
    if plan_opportunity_ids != tuple(
        item.opportunity_id for item in opportunity_evaluation.opportunities
    ):
        raise SequencingValidationError(
            "Investigation plans must cover active opportunities in upstream order"
        )
    tasks: dict[str, tuple[InvestigationPlan, InvestigationTask]] = {}
    requirements: dict[tuple[str, str], EvidenceRequirement] = {}
    definitions: dict[str, tuple[str, str, str]] = {}
    statuses: dict[str, object] = {}
    for plan in investigation_portfolio.plans:
        if (
            plan.business_id != context.business_id
            or plan.as_of_date != context.as_of_date
            or plan.opportunity_id not in opportunities
        ):
            raise SequencingValidationError(
                "Investigation plan references an unknown or cross-business opportunity"
            )
        task_ids: set[str] = set()
        for task in plan.tasks:
            if task.task_id in tasks or task.task_id in task_ids:
                raise SequencingValidationError(f"Duplicate task ID: {task.task_id}")
            if task.opportunity_id != plan.opportunity_id:
                raise SequencingValidationError("Task belongs to an unrelated plan")
            task_ids.add(task.task_id)
            tasks[task.task_id] = (plan, task)
        requirement_ids: set[str] = set()
        for requirement in plan.evidence_requirements:
            if requirement.requirement_id in requirement_ids:
                raise SequencingValidationError(
                    f"Duplicate requirement in plan: {requirement.requirement_id}"
                )
            requirement_ids.add(requirement.requirement_id)
            requirements[(plan.plan_id, requirement.requirement_id)] = requirement
            definition = (
                requirement.name,
                requirement.description,
                requirement.source_scope,
            )
            previous = definitions.setdefault(requirement.requirement_id, definition)
            if previous != definition:
                raise SequencingValidationError(
                    "Conflicting semantic definitions for requirement ID "
                    f"{requirement.requirement_id}"
                )
            previous_status = statuses.setdefault(
                requirement.requirement_id, requirement.status
            )
            if previous_status is not requirement.status:
                raise SequencingValidationError(
                    "Conflicting current statuses for requirement ID "
                    f"{requirement.requirement_id}"
                )

    gaps = _unique_index(
        investigation_portfolio.evidence_gaps, "requirement_id", "evidence gap requirement"
    )
    decisions = _unique_index(
        decision_portfolio.assessments, "decision_id", "decision ID"
    )
    decision_orders = [
        item.decision_order for item in decision_portfolio.assessments
    ]
    if decision_orders != list(range(1, len(decision_orders) + 1)):
        raise SequencingValidationError("Decision ordering is unstable")
    for assessment in decision_portfolio.assessments:
        if (
            assessment.business_id != context.business_id
            or assessment.as_of_date != context.as_of_date
        ):
            raise SequencingValidationError("Decision crosses business or date boundaries")
        opportunity = opportunities.get(assessment.originating_opportunity_id)
        plan = plan_by_id.get(assessment.investigation_plan_id)
        if opportunity is None:
            raise SequencingValidationError("Decision references an unknown opportunity ID")
        if plan is None:
            raise SequencingValidationError("Decision references an unknown plan ID")
        if plan.opportunity_id != opportunity.opportunity_id:
            raise SequencingValidationError("Decision references an unrelated plan")
        plan_requirement_ids = {
            item.requirement_id for item in plan.evidence_requirements
        }
        if set(assessment.required_requirement_ids) - plan_requirement_ids:
            raise SequencingValidationError("Decision references an unknown requirement ID")
        if set(assessment.unresolved_requirement_ids) - plan_requirement_ids:
            raise SequencingValidationError("Decision references an unknown unresolved requirement")
        plan_task_ids = {item.task_id for item in plan.tasks}
        if set(assessment.relevant_investigation_task_ids) - plan_task_ids:
            raise SequencingValidationError(
                "Decision references an unknown or unrelated task ID"
            )
        if set(assessment.next_evidence_task_ids) - plan_task_ids:
            raise SequencingValidationError("Decision references an unknown or unrelated task")
        if (
            assessment.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
            and assessment.unresolved_requirement_ids
        ):
            raise SequencingValidationError(
                "READY_FOR_HUMAN_REVIEW decisions cannot have unresolved requirements"
            )
    return _SourceIndexes(
        opportunities=opportunities,
        plans=plan_by_id,
        tasks=tasks,
        requirements=requirements,
        gaps=gaps,
        decisions=decisions,
    )


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _derive_portfolio(
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
    decision_portfolio: DecisionReadinessPortfolio,
) -> InvestigationSequencingPortfolio:
    indexes = _validate_source_graph(
        context,
        opportunity_evaluation,
        investigation_portfolio,
        decision_portfolio,
    )
    targeted = tuple(
        item
        for item in decision_portfolio.assessments
        if item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
    )
    boundary = tuple(
        item
        for item in decision_portfolio.assessments
        if item.readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY
    )

    leverage: dict[str, dict[str, object]] = {}
    appearance = 0
    for assessment in targeted:
        plan = indexes.plans[assessment.investigation_plan_id]
        opportunity = indexes.opportunities[assessment.originating_opportunity_id]
        for requirement_id in assessment.unresolved_requirement_ids:
            requirement = indexes.requirements.get((plan.plan_id, requirement_id))
            if requirement is None:
                raise SequencingValidationError(
                    f"Unknown requirement ID: {requirement_id}"
                )
            if requirement_id not in leverage:
                leverage[requirement_id] = {
                    "requirement": requirement,
                    "decisions": [],
                    "opportunities": [],
                    "tasks": [],
                    "first_appearance": appearance,
                }
                appearance += 1
            aggregate = leverage[requirement_id]
            _append_unique(aggregate["decisions"], assessment.decision_id)  # type: ignore[arg-type]
            _append_unique(
                aggregate["opportunities"], opportunity.opportunity_id  # type: ignore[arg-type]
            )
            for task_id in assessment.next_evidence_task_ids:
                task_entry = indexes.tasks.get(task_id)
                if task_entry is None:
                    raise SequencingValidationError(f"Unknown task ID: {task_id}")
                _, task = task_entry
                if requirement_id in task.required_requirement_ids:
                    _append_unique(aggregate["tasks"], task_id)  # type: ignore[arg-type]

    leverage_items: list[tuple[int, EvidenceLeverageItem]] = []
    for requirement_id, aggregate in leverage.items():
        requirement = aggregate["requirement"]
        decision_ids = tuple(aggregate["decisions"])
        opportunity_ids = tuple(aggregate["opportunities"])
        task_ids = tuple(aggregate["tasks"])
        priorities = tuple(
            indexes.opportunities[item].priority for item in opportunity_ids
        )
        highest_priority = min(priorities, key=lambda item: _PRIORITY_ORDER[item])
        gap = indexes.gaps.get(requirement_id)
        item = EvidenceLeverageItem(
            requirement_id=requirement.requirement_id,
            requirement_name=requirement.name,
            description=requirement.description,
            requirement_status=requirement.status,
            affected_decision_ids=decision_ids,
            affected_opportunity_ids=opportunity_ids,
            related_task_ids=task_ids,
            affected_decision_count=len(decision_ids),
            affected_opportunity_count=len(opportunity_ids),
            highest_opportunity_priority=highest_priority,
            shared_across_decisions=len(decision_ids) > 1,
            shared_across_opportunities=len(opportunity_ids) > 1,
            existing_gap_id=gap.gap_id if gap is not None else None,
            limitation=_LEVERAGE_LIMITATION,
        )
        leverage_items.append((int(aggregate["first_appearance"]), item))
    leverage_items.sort(key=lambda entry: (
        -entry[1].affected_decision_count,
        _PRIORITY_ORDER[entry[1].highest_opportunity_priority],
        -entry[1].affected_opportunity_count,
        entry[0],
        entry[1].requirement_id,
    ))
    ordered_leverage = tuple(item for _, item in leverage_items)

    task_links: dict[str, list[DecisionReadinessAssessment]] = {}
    for assessment in targeted:
        for task_id in assessment.next_evidence_task_ids:
            if task_id not in indexes.tasks:
                raise SequencingValidationError(f"Unknown task ID: {task_id}")
            task_links.setdefault(task_id, []).append(assessment)

    unsorted_sequence: list[
        tuple[
            bool,
            OpportunityPriority,
            int,
            int,
            int,
            str,
            InvestigationPlan,
            InvestigationTask,
            InvestigationOpportunity,
            tuple[str, ...],
            tuple[str, ...],
        ]
    ] = []
    for task_id, assessments in task_links.items():
        plan, task = indexes.tasks[task_id]
        opportunity = indexes.opportunities[plan.opportunity_id]
        affected_decision_ids = tuple(item.decision_id for item in assessments)
        unresolved = {
            requirement_id
            for assessment in assessments
            for requirement_id in assessment.unresolved_requirement_ids
        }
        addressed = tuple(
            item for item in task.required_requirement_ids if item in unresolved
        )
        can_begin = task.readiness is InvestigationReadiness.READY_NOW
        unsorted_sequence.append((
            can_begin,
            opportunity.priority,
            len(affected_decision_ids),
            opportunity.opportunity_order,
            task.task_order,
            task_id,
            plan,
            task,
            opportunity,
            affected_decision_ids,
            addressed,
        ))
    unsorted_sequence.sort(key=lambda item: (
        not item[0],
        _PRIORITY_ORDER[item[1]],
        -item[2],
        item[3],
        item[4],
        item[5],
    ))
    sequence_items: list[InvestigationSequenceItem] = []
    for sequence_order, entry in enumerate(unsorted_sequence, 1):
        (
            can_begin,
            priority,
            affected_count,
            opportunity_order,
            _task_order,
            task_id,
            plan,
            task,
            opportunity,
            affected_decision_ids,
            addressed,
        ) = entry
        if can_begin:
            reason = (
                "This existing analytical task can begin now and could produce evidence "
                f"relevant to {affected_count} current non-ready decision "
                f"assessment{'s' if affected_count != 1 else ''}."
            )
        else:
            reason = (
                "This existing analytical task could produce evidence relevant to "
                f"{affected_count} current non-ready decision "
                f"assessment{'s' if affected_count != 1 else ''}, but cannot begin "
                "now under its existing "
                f"{task.readiness.value} status."
            )
        sequence_items.append(InvestigationSequenceItem(
            sequence_order=sequence_order,
            task_id=task_id,
            investigation_plan_id=plan.plan_id,
            opportunity_id=opportunity.opportunity_id,
            task_title=task.title,
            task_kind=task.task_kind,
            task_readiness=task.readiness,
            affected_decision_ids=affected_decision_ids,
            addressed_requirement_ids=addressed,
            affected_decision_count=affected_count,
            opportunity_priority=priority,
            opportunity_order=opportunity_order,
            can_begin_now=can_begin,
            sequencing_reason=reason,
            limitation=_TASK_LIMITATION,
        ))
    ordered_sequence = tuple(sequence_items)
    recommended = next(
        (item.task_id for item in ordered_sequence if item.can_begin_now), None
    )
    if recommended is not None:
        state = SequencingPortfolioState.READY_TASK_AVAILABLE
    elif targeted:
        state = SequencingPortfolioState.EVIDENCE_GAP_FIRST
    elif boundary:
        state = SequencingPortfolioState.BOUNDARY_ONLY
    else:
        state = SequencingPortfolioState.NO_OPEN_READINESS_GAPS
    return InvestigationSequencingPortfolio(
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        state=state,
        evidence_leverage_items=ordered_leverage,
        sequence_items=ordered_sequence,
        top_evidence_focus_requirement_id=(
            ordered_leverage[0].requirement_id if ordered_leverage else None
        ),
        recommended_next_task_id=recommended,
        startable_task_count=sum(item.can_begin_now for item in ordered_sequence),
        blocked_sequence_task_count=sum(
            not item.can_begin_now for item in ordered_sequence
        ),
        targeted_needs_more_evidence_decision_ids=tuple(
            item.decision_id for item in targeted
        ),
        boundary_blocked_decision_ids=tuple(item.decision_id for item in boundary),
        limitation=_PORTFOLIO_LIMITATION,
    )


def _output_text(portfolio: InvestigationSequencingPortfolio) -> str:
    values = [portfolio.limitation]
    for item in portfolio.evidence_leverage_items:
        values.extend((item.requirement_name, item.description, item.limitation))
    for item in portfolio.sequence_items:
        values.extend((item.task_title, item.sequencing_reason, item.limitation))
    return "\n".join(values)


def validate_investigation_sequencing_portfolio(
    portfolio: InvestigationSequencingPortfolio,
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
    decision_portfolio: DecisionReadinessPortfolio,
) -> InvestigationSequencingPortfolio:
    """Fail closed on source linkage, ordering, state, safety, and exact derivation."""
    if not isinstance(portfolio, InvestigationSequencingPortfolio):
        raise SequencingValidationError(
            "Portfolio must use the strict InvestigationSequencingPortfolio model"
        )
    indexes = _validate_source_graph(
        context,
        opportunity_evaluation,
        investigation_portfolio,
        decision_portfolio,
    )
    if portfolio.business_id != context.business_id or portfolio.as_of_date != context.as_of_date:
        raise SequencingValidationError("Sequencing portfolio crosses context identity")
    targeted = set(portfolio.targeted_needs_more_evidence_decision_ids)
    boundary = set(portfolio.boundary_blocked_decision_ids)
    for item in portfolio.evidence_leverage_items:
        for decision_id in item.affected_decision_ids:
            assessment = indexes.decisions.get(decision_id)
            if assessment is None:
                raise SequencingValidationError("Evidence leverage references unknown decision ID")
            if decision_id not in targeted or assessment.readiness is not DecisionReadiness.NEEDS_MORE_EVIDENCE:
                raise SequencingValidationError(
                    "Evidence leverage can include only NEEDS_MORE_EVIDENCE decisions"
                )
            if item.requirement_id not in assessment.unresolved_requirement_ids:
                raise SequencingValidationError(
                    "Evidence leverage references a requirement not unresolved for its decision"
                )
        if set(item.affected_decision_ids) & boundary:
            raise SequencingValidationError(
                "Boundary-blocked decisions cannot inflate evidence leverage"
            )
    for item in portfolio.sequence_items:
        source = indexes.tasks.get(item.task_id)
        if source is None:
            raise SequencingValidationError("Sequence references unknown task ID")
        source_plan, _ = source
        if source_plan.plan_id != item.investigation_plan_id:
            raise SequencingValidationError("Sequence task belongs to an unrelated plan")
        for decision_id in item.affected_decision_ids:
            assessment = indexes.decisions.get(decision_id)
            if assessment is None or item.task_id not in assessment.next_evidence_task_ids:
                raise SequencingValidationError(
                    "Sequence task is not present in assessment.next_evidence_task_ids"
                )
    text = _output_text(portfolio)
    if _EMAIL.search(text) or _PHONE.search(text) or _PII.search(text):
        raise SequencingValidationError("Sequencing output contains PII-shaped content")
    if _FORBIDDEN_ACTION.search(text):
        raise SequencingValidationError("Sequencing output contains business execution language")
    if _FALSE_UNLOCK.search(text):
        raise SequencingValidationError("Sequencing output promises a decision unlock")
    if _CAUSAL_ASSERTION.search(text):
        raise SequencingValidationError("Sequencing output contains a causal assertion")
    if _FINANCIAL_ASSERTION.search(text):
        raise SequencingValidationError("Sequencing output contains a financial claim")
    expected = _derive_portfolio(
        context,
        opportunity_evaluation,
        investigation_portfolio,
        decision_portfolio,
    )
    if portfolio != expected:
        raise SequencingValidationError(
            "Sequencing portfolio does not match deterministic upstream derivation"
        )
    return portfolio


def build_investigation_sequencing_portfolio(
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
    decision_portfolio: DecisionReadinessPortfolio,
) -> InvestigationSequencingPortfolio:
    """Sequence learning from four already-built upstream intelligence layers."""
    portfolio = _derive_portfolio(
        context,
        opportunity_evaluation,
        investigation_portfolio,
        decision_portfolio,
    )
    return validate_investigation_sequencing_portfolio(
        portfolio,
        context,
        opportunity_evaluation,
        investigation_portfolio,
        decision_portfolio,
    )
