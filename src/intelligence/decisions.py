"""Deterministic decision-readiness evaluation over validated intelligence layers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re

from src.intelligence.context import IntelligenceContext
from src.intelligence.decision_models import (
    DecisionClass,
    DecisionReadiness,
    DecisionReadinessAssessment,
    DecisionReadinessPortfolio,
    DecisionType,
    DecisionValidationError,
    ReadinessReasonCode,
    stable_decision_id,
)
from src.intelligence.investigation_models import (
    EvidenceRequirementStatus,
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationReadiness,
)
from src.intelligence.investigations import validate_investigation_portfolio
from src.intelligence.narration import (
    _AUTONOMOUS_ASSERTION,
    _AUTONOMOUS_STEP,
    _EMAIL,
    _PHONE,
    _PII_TERMS,
    _has_unsupported_causal_wording,
)
from src.intelligence.opportunities import _FINANCIAL_ASSERTION, validate_opportunity
from src.intelligence.opportunity_models import (
    InvestigationOpportunity,
    OpportunityEvaluation,
    OpportunityPriority,
    OpportunityType,
)


class CounterEvidenceBehavior(StrEnum):
    ALLOW_IF_DISCLOSED = "ALLOW_IF_DISCLOSED"
    PREVENT_READINESS = "PREVENT_READINESS"


class BlockerBehavior(StrEnum):
    ALLOW_CONTEXTUAL = "ALLOW_CONTEXTUAL"
    PREVENT_READINESS = "PREVENT_READINESS"


class BoundaryBehavior(StrEnum):
    STANDARD = "STANDARD"
    REQUIRE_TRUSTED_ECONOMICS = "REQUIRE_TRUSTED_ECONOMICS"


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionFrameSpec:
    """One explicit evidence bar for a bounded human decision question."""

    frame_order: int
    frame_id: str
    originating_opportunity_type: OpportunityType
    decision_type: DecisionType
    decision_class: DecisionClass
    decision_question: str
    required_requirement_ids: tuple[str, ...]
    relevant_task_keys: tuple[str, ...]
    counter_evidence_behavior: CounterEvidenceBehavior
    blocker_behavior: BlockerBehavior
    boundary_behavior: BoundaryBehavior
    require_ready_task: bool
    rationale_design: str
    decision_boundary: str
    limitation: str


def _frame(
    frame_order: int,
    frame_id: str,
    opportunity_type: OpportunityType,
    decision_type: DecisionType,
    decision_class: DecisionClass,
    question: str,
    requirements: tuple[str, ...],
    task_keys: tuple[str, ...],
    *,
    counter: CounterEvidenceBehavior = CounterEvidenceBehavior.PREVENT_READINESS,
    blocker: BlockerBehavior = BlockerBehavior.PREVENT_READINESS,
    boundary: BoundaryBehavior = BoundaryBehavior.STANDARD,
    require_ready_task: bool = True,
    rationale: str,
    decision_boundary: str,
    limitation: str,
) -> DecisionFrameSpec:
    return DecisionFrameSpec(
        frame_order=frame_order,
        frame_id=frame_id,
        originating_opportunity_type=opportunity_type,
        decision_type=decision_type,
        decision_class=decision_class,
        decision_question=question,
        required_requirement_ids=requirements,
        relevant_task_keys=task_keys,
        counter_evidence_behavior=counter,
        blocker_behavior=blocker,
        boundary_behavior=boundary,
        require_ready_task=require_ready_task,
        rationale_design=rationale,
        decision_boundary=decision_boundary,
        limitation=limitation,
    )


DECISION_FRAME_REGISTRY: dict[OpportunityType, tuple[DecisionFrameSpec, ...]] = {
    OpportunityType.CONFIRMATION_LEAKAGE: (
        _frame(
            1,
            "confirmation-investigation-review",
            OpportunityType.CONFIRMATION_LEAKAGE,
            DecisionType.CONFIRMATION_INVESTIGATION_REVIEW,
            DecisionClass.INVESTIGATION_DIRECTION,
            "Is current evidence sufficient to prioritize confirmation leakage for human investigation review?",
            ("requirement:confirmation_gap_baseline",),
            ("validate-confirmation-baseline",),
            blocker=BlockerBehavior.ALLOW_CONTEXTUAL,
            rationale="Validated confirmation-gap evidence and an investigation that can begin now form the bounded review bar.",
            decision_boundary=(
                "This readiness assessment supports human investigation review only and does not "
                "authorize a change to the confirmation process."
            ),
            limitation=(
                "The observed confirmation gap does not establish its cause or select an operational response."
            ),
        ),
        _frame(
            2,
            "confirmation-operational-change-consideration",
            OpportunityType.CONFIRMATION_LEAKAGE,
            DecisionType.CONFIRMATION_OPERATIONAL_CHANGE_CONSIDERATION,
            DecisionClass.BUSINESS_CHANGE_CONSIDERATION,
            "Is current evidence sufficient to consider an operational change to the confirmation process?",
            (
                "requirement:confirmation_gap_baseline",
                "requirement:confirmation_dispositions",
                "requirement:campaign_offer_mix",
                "requirement:upstream_order_cohorts",
            ),
            (
                "validate-confirmation-baseline",
                "segment-confirmation-dispositions",
                "compare-offer-upstream-cohorts",
            ),
            rationale="Operational-change review requires the baseline, dispositions, and competing cohort explanations.",
            decision_boundary=(
                "Human review may consider the bounded question only; this assessment does not "
                "authorize or select an operational change."
            ),
            limitation=(
                "Observational evidence cannot establish why the gap exists or predict an outcome from a change."
            ),
        ),
    ),
    OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT: (
        _frame(
            1,
            "acquisition-fulfillment-investigation-direction",
            OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
            DecisionType.ACQUISITION_FULFILLMENT_INVESTIGATION_DIRECTION,
            DecisionClass.INVESTIGATION_DIRECTION,
            "Is current evidence sufficient to determine which acquisition, offer, or fulfillment area deserves deeper investigation first?",
            (
                "requirement:campaign_downstream_peer_evidence",
                "requirement:campaign_offer_mix",
                "requirement:adgroup_downstream_cohorts",
            ),
            ("validate-peer-pattern", "compare-offer-mix", "compare-adgroup-cohorts"),
            counter=CounterEvidenceBehavior.ALLOW_IF_DISCLOSED,
            blocker=BlockerBehavior.PREVENT_READINESS,
            rationale="Direction review requires peer evidence plus offer and below-campaign cohorts that distinguish domains.",
            decision_boundary=(
                "This assessment can prioritize a domain for human investigation but cannot attribute "
                "cause or authorize a campaign, offer, or fulfillment change."
            ),
            limitation="Campaign peer differences are observational and do not identify their cause.",
        ),
        _frame(
            2,
            "campaign-change-consideration",
            OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
            DecisionType.CAMPAIGN_CHANGE_CONSIDERATION,
            DecisionClass.BUSINESS_CHANGE_CONSIDERATION,
            "Is current evidence sufficient to consider a campaign spend or setup change?",
            (
                "requirement:campaign_downstream_peer_evidence",
                "requirement:campaign_offer_mix",
                "requirement:adgroup_downstream_cohorts",
            ),
            ("validate-peer-pattern", "compare-offer-mix", "compare-adgroup-cohorts"),
            rationale="Campaign-change review requires evidence that can distinguish competing explanations, not peer differences alone.",
            decision_boundary=(
                "Human review of the question does not authorize, select, or execute any spend or setup change."
            ),
            limitation="Current observational comparisons cannot establish causal impact or forecast a campaign outcome.",
        ),
    ),
    OpportunityType.MEASUREMENT_RECONCILIATION: (
        _frame(
            1,
            "measurement-reconciliation-review",
            OpportunityType.MEASUREMENT_RECONCILIATION,
            DecisionType.MEASUREMENT_RECONCILIATION_REVIEW,
            DecisionClass.MEASUREMENT_GOVERNANCE,
            "Is current evidence sufficient to prioritize measurement reconciliation?",
            ("requirement:platform_observed_gap",),
            ("validate-measurement-gap",),
            counter=CounterEvidenceBehavior.ALLOW_IF_DISCLOSED,
            blocker=BlockerBehavior.ALLOW_CONTEXTUAL,
            rationale="A validated platform-versus-observed gap is sufficient for bounded reconciliation review.",
            decision_boundary=(
                "Current evidence supports measurement reconciliation review but not an interpretation "
                "of the gap as operational loss, fraud, or tracking failure."
            ),
            limitation="The aggregate difference does not establish operational loss or financial impact.",
        ),
        _frame(
            2,
            "measurement-use-readiness",
            OpportunityType.MEASUREMENT_RECONCILIATION,
            DecisionType.MEASUREMENT_USE_READINESS,
            DecisionClass.MEASUREMENT_GOVERNANCE,
            "Is platform conversion evidence sufficiently reconciled for use in downstream business decisions?",
            (
                "requirement:platform_observed_gap",
                "requirement:platform_event_definition",
                "requirement:lightfunnels_inclusion_rules",
            ),
            (
                "validate-measurement-gap",
                "reconcile-platform-semantics",
                "reconcile-observed-inclusion",
            ),
            rationale="Downstream use requires both sides of the measurement semantics to be documented and reconciled.",
            decision_boundary=(
                "Readiness for downstream use requires reconciled definitions and does not convert the "
                "observed gap into operational or financial loss."
            ),
            limitation="A high-confidence gap does not mean platform and observed-order semantics are reconciled.",
        ),
    ),
    OpportunityType.ACQUISITION_EFFICIENCY_REVIEW: (
        _frame(
            1,
            "acquisition-efficiency-investigation-review",
            OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
            DecisionType.ACQUISITION_EFFICIENCY_INVESTIGATION_REVIEW,
            DecisionClass.INVESTIGATION_DIRECTION,
            "Is current evidence sufficient to prioritize acquisition efficiency for deeper human review?",
            (
                "requirement:acquisition_cost_peer_evidence",
                "requirement:campaign_downstream_outcomes",
            ),
            ("validate-cost-gap", "compare-downstream-evidence"),
            counter=CounterEvidenceBehavior.ALLOW_IF_DISCLOSED,
            blocker=BlockerBehavior.ALLOW_CONTEXTUAL,
            rationale="Peer-adjusted cost and downstream context form the bounded investigation-review bar.",
            decision_boundary=(
                "This review does not establish CAC, ROAS, profit, budget impact, or authorize a campaign action."
            ),
            limitation="The available aggregate evidence cannot establish causal or trusted cross-currency economics.",
        ),
        _frame(
            2,
            "acquisition-efficiency-campaign-change-consideration",
            OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
            DecisionType.ACQUISITION_EFFICIENCY_CAMPAIGN_CHANGE_CONSIDERATION,
            DecisionClass.BUSINESS_CHANGE_CONSIDERATION,
            "Is current evidence sufficient to consider a campaign change based on acquisition efficiency?",
            (
                "requirement:acquisition_cost_peer_evidence",
                "requirement:campaign_downstream_outcomes",
                "requirement:below_campaign_acquisition_cohorts",
            ),
            (
                "validate-cost-gap",
                "compare-downstream-evidence",
                "collect-below-campaign-cohorts",
            ),
            rationale="Campaign-change review requires below-campaign cohorts and downstream context in addition to a cost gap.",
            decision_boundary=(
                "Human review does not authorize, select, or execute a campaign or budget change and "
                "does not assert trusted cross-currency returns."
            ),
            limitation="Cost differences alone cannot establish cause, ROI, or expected business outcome.",
        ),
    ),
}


# This synthetic/future-facing frame proves the explicit economic contract without
# attaching a financial decision to an opportunity whose current 6.7C plan does not
# contain the trusted-economics requirement. It is never emitted automatically.
FINANCIAL_BOUNDARY_FRAME = _frame(
    1,
    "trusted-economics-review",
    OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
    DecisionType.TRUSTED_ECONOMICS_REVIEW,
    DecisionClass.FINANCIAL_DECISION,
    "Is trusted cross-currency evidence sufficient for human review of acquisition economics?",
    ("requirement:trusted_cross_currency_economics",),
    ("validate-trusted-economics",),
    boundary=BoundaryBehavior.REQUIRE_TRUSTED_ECONOMICS,
    rationale="A financial decision frame requires trusted cross-currency economics.",
    decision_boundary="Trusted cross-currency economics are unavailable while FX_REQUIRED applies.",
    limitation="No trusted profit, contribution, margin, MER, ROAS, ROI, or dollar-upside claim is available.",
)

# Boundary-only frames are explicit registry entries but are not automatically
# emitted until an active 6.7C plan actually carries their requirement and task.
BOUNDARY_FRAME_REGISTRY: tuple[DecisionFrameSpec, ...] = (
    FINANCIAL_BOUNDARY_FRAME,
)


_PRIORITY_ORDER = {
    OpportunityPriority.HIGH: 0,
    OpportunityPriority.MEDIUM: 1,
    OpportunityPriority.LOW: 2,
}
_INTELLIGENCE_OBJECT_REF = re.compile(
    r"^(?:opportunity|investigation-plan|investigation-task|requirement|"
    r"evidence-gap|decision-readiness):"
)
_FORBIDDEN_ACTION = re.compile(
    r"(?:^|[.!?]\s+)(?:pause|reduce|increase|scale|switch|change|execute|approve|authorize)\b",
    re.IGNORECASE,
)
_FORBIDDEN_FINANCIAL = re.compile(
    r"\b(?:recovered revenue|dollar upside|expected return|predicted outcome)\b|"
    r"\b(?:profit|contribution|margin|mer|roas|roi)\b[^.!?]{0,20}(?:is|=|\$|\d)",
    re.IGNORECASE,
)


def _task_key(task_id: str) -> str:
    return task_id.rsplit(":", 1)[-1]


def _relevant_tasks(plan: InvestigationPlan, frame: DecisionFrameSpec):
    by_key = {_task_key(task.task_id): task for task in plan.tasks}
    unknown = set(frame.relevant_task_keys) - by_key.keys()
    if unknown:
        raise DecisionValidationError("Decision frame references an unknown investigation task")
    return tuple(task for task in plan.tasks if _task_key(task.task_id) in frame.relevant_task_keys)


def _components(
    context: IntelligenceContext,
    opportunity: InvestigationOpportunity,
    plan: InvestigationPlan,
    frame: DecisionFrameSpec,
) -> dict[str, object]:
    if frame.originating_opportunity_type is not opportunity.opportunity_type:
        raise DecisionValidationError("Decision frame crosses opportunity type")
    requirement_by_id = {item.requirement_id: item for item in plan.evidence_requirements}
    unknown_requirements = set(frame.required_requirement_ids) - requirement_by_id.keys()
    if unknown_requirements:
        raise DecisionValidationError("Decision frame references an unknown plan requirement")
    requirements = tuple(requirement_by_id[item] for item in frame.required_requirement_ids)
    unresolved = tuple(
        item.requirement_id
        for item in requirements
        if item.status is not EvidenceRequirementStatus.AVAILABLE
    )
    relevant_tasks = _relevant_tasks(plan, frame)
    boundary_blocked = (
        any(item.status is EvidenceRequirementStatus.BLOCKED_BY_BOUNDARY for item in requirements)
        or (
            frame.boundary_behavior is BoundaryBehavior.REQUIRE_TRUSTED_ECONOMICS
            and context.economic_status.facts.get("economic_status") == "FX_REQUIRED"
        )
    )
    counter_blocks = bool(opportunity.counter_evidence_refs) and (
        frame.counter_evidence_behavior is CounterEvidenceBehavior.PREVENT_READINESS
    )
    blocker_blocks = bool(opportunity.blocking_evidence_refs) and (
        frame.blocker_behavior is BlockerBehavior.PREVENT_READINESS
    )
    has_ready_task = any(
        task.readiness is InvestigationReadiness.READY_NOW for task in relevant_tasks
    )
    if boundary_blocked:
        readiness = DecisionReadiness.BLOCKED_BY_BOUNDARY
    elif unresolved or counter_blocks or blocker_blocks or (
        frame.require_ready_task and not has_ready_task
    ):
        readiness = DecisionReadiness.NEEDS_MORE_EVIDENCE
    else:
        readiness = DecisionReadiness.READY_FOR_HUMAN_REVIEW

    reasons: list[ReadinessReasonCode] = []
    if readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW:
        reasons.append(ReadinessReasonCode.SUFFICIENT_CURRENT_EVIDENCE)
    if unresolved and not boundary_blocked:
        reasons.append(ReadinessReasonCode.MISSING_REQUIRED_EVIDENCE)
    if opportunity.counter_evidence_refs:
        reasons.append(ReadinessReasonCode.COUNTER_EVIDENCE_PRESENT)
    if opportunity.blocking_evidence_refs:
        reasons.append(ReadinessReasonCode.DATA_QUALITY_LIMITATION)
    if readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE and (
        unresolved or counter_blocks or blocker_blocks or not has_ready_task
    ):
        reasons.append(ReadinessReasonCode.OPEN_CRITICAL_INVESTIGATION)
    if (
        readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
        and frame.decision_class is DecisionClass.BUSINESS_CHANGE_CONSIDERATION
    ):
        reasons.append(ReadinessReasonCode.CAUSAL_UNCERTAINTY)
    if (
        readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
        and frame.decision_type is DecisionType.MEASUREMENT_USE_READINESS
    ):
        reasons.append(ReadinessReasonCode.MEASUREMENT_UNRESOLVED)
    if boundary_blocked:
        reasons.append(ReadinessReasonCode.ECONOMIC_BOUNDARY)
    reasons.append(ReadinessReasonCode.HUMAN_REVIEW_REQUIRED)

    counter = opportunity.counter_evidence_refs
    blocking = opportunity.blocking_evidence_refs
    excluded = set((*counter, *blocking))
    support = tuple(dict.fromkeys((
        *opportunity.supporting_evidence_refs,
        *(ref for item in requirements for ref in item.evidence_refs),
    )))
    support = tuple(ref for ref in support if ref not in excluded)
    if not support:
        raise DecisionValidationError("Decision frame has no validated supporting evidence")

    # READY_NOW means only that analysis can begin. Because 6.7C has no completion
    # state, task readiness never satisfies a missing requirement and expected output
    # is never treated as evidence. Only EvidenceRequirement.status does that.
    next_tasks = tuple(
        task.task_id for task in relevant_tasks
        if set(task.required_requirement_ids) & set(unresolved)
    )
    if readiness is not DecisionReadiness.READY_FOR_HUMAN_REVIEW and not next_tasks:
        next_tasks = tuple(task.task_id for task in relevant_tasks)

    if readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW:
        rationale = (
            "The current validated aggregate requirements meet this frame's bounded "
            "evidence bar. Human review remains required and no option is selected."
        )
    elif readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY:
        rationale = (
            "The explicit economic boundary prevents responsible review of this frame "
            "under the current validated context."
        )
    else:
        rationale = (
            "Further evidence is required before this decision can be responsibly reviewed. "
            "Investigation tasks identify analytical work that could raise readiness."
        )
    return {
        "readiness": readiness,
        "reasons": tuple(reasons),
        "rationale": rationale,
        "support": support,
        "counter": counter,
        "blocking": blocking,
        "unresolved": unresolved,
        "relevant_task_ids": tuple(task.task_id for task in relevant_tasks),
        "next_task_ids": next_tasks,
    }


def assess_decision_frame(
    context: IntelligenceContext,
    opportunity: InvestigationOpportunity,
    plan: InvestigationPlan,
    frame: DecisionFrameSpec,
    *,
    decision_order: int = 1,
) -> DecisionReadinessAssessment:
    """Assess one explicit frame; primarily useful for registry and boundary tests."""
    validate_opportunity(opportunity, context)
    if (
        plan.opportunity_id != opportunity.opportunity_id
        or plan.business_id != context.business_id
        or plan.as_of_date != context.as_of_date
    ):
        raise DecisionValidationError("Decision frame crosses plan or context identity")
    parts = _components(context, opportunity, plan, frame)
    assessment = DecisionReadinessAssessment(
        decision_order=decision_order,
        decision_id=stable_decision_id(
            opportunity.opportunity_type.value, opportunity.scope_name, frame.frame_id
        ),
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        originating_opportunity_id=opportunity.opportunity_id,
        investigation_plan_id=plan.plan_id,
        decision_type=frame.decision_type,
        decision_class=frame.decision_class,
        decision_question=frame.decision_question,
        readiness=parts["readiness"],
        readiness_reason_codes=parts["reasons"],
        rationale_summary=parts["rationale"],
        supporting_evidence_refs=parts["support"],
        counter_evidence_refs=parts["counter"],
        blocking_evidence_refs=parts["blocking"],
        required_requirement_ids=frame.required_requirement_ids,
        unresolved_requirement_ids=parts["unresolved"],
        relevant_investigation_task_ids=parts["relevant_task_ids"],
        next_evidence_task_ids=parts["next_task_ids"],
        decision_boundary=frame.decision_boundary,
        human_review_required=True,
        autonomous_action_allowed=False,
        limitation=frame.limitation,
    )
    return validate_decision_assessment(assessment, context, opportunity, plan, frame)


def _assessment_text(assessment: DecisionReadinessAssessment) -> str:
    return "\n".join((
        assessment.decision_question,
        assessment.rationale_summary,
        assessment.decision_boundary,
        assessment.limitation,
    ))


def validate_decision_assessment(
    assessment: DecisionReadinessAssessment,
    context: IntelligenceContext,
    opportunity: InvestigationOpportunity,
    plan: InvestigationPlan,
    frame: DecisionFrameSpec,
) -> DecisionReadinessAssessment:
    """Fail closed on identity, traceability, evidence bars, language, and agency."""
    if assessment.originating_opportunity_id != opportunity.opportunity_id:
        raise DecisionValidationError("Decision references an unknown opportunity ID")
    if assessment.investigation_plan_id != plan.plan_id:
        raise DecisionValidationError("Decision references an unknown plan ID")
    expected_id = stable_decision_id(
        opportunity.opportunity_type.value, opportunity.scope_name, frame.frame_id
    )
    if assessment.decision_id != expected_id:
        raise DecisionValidationError("decision_id is not the stable expected ID")
    if assessment.business_id != context.business_id or assessment.as_of_date != context.as_of_date:
        raise DecisionValidationError("Decision crosses context identity")
    parts = _components(context, opportunity, plan, frame)
    fixed = (
        assessment.decision_type is frame.decision_type
        and assessment.decision_class is frame.decision_class
        and assessment.decision_question == frame.decision_question
        and assessment.required_requirement_ids == frame.required_requirement_ids
        and assessment.readiness is parts["readiness"]
        and assessment.readiness_reason_codes == parts["reasons"]
        and assessment.rationale_summary == parts["rationale"]
        and assessment.supporting_evidence_refs == parts["support"]
        and assessment.counter_evidence_refs == parts["counter"]
        and assessment.blocking_evidence_refs == parts["blocking"]
        and assessment.unresolved_requirement_ids == parts["unresolved"]
        and assessment.relevant_investigation_task_ids == parts["relevant_task_ids"]
        and assessment.next_evidence_task_ids == parts["next_task_ids"]
        and assessment.decision_boundary == frame.decision_boundary
        and assessment.limitation == frame.limitation
    )
    if not fixed:
        raise DecisionValidationError("Decision assessment does not match its registered frame")
    refs = (
        *assessment.supporting_evidence_refs,
        *assessment.counter_evidence_refs,
        *assessment.blocking_evidence_refs,
    )
    if any(_INTELLIGENCE_OBJECT_REF.match(ref) for ref in refs):
        raise DecisionValidationError("Intelligence object IDs cannot be used as evidence")
    known = context.evidence_by_id
    if set(refs) - known.keys():
        raise DecisionValidationError("Decision references unknown evidence")
    if any(known[ref].business_id != context.business_id for ref in refs):
        raise DecisionValidationError("Decision evidence crosses business boundaries")
    task_ids = {task.task_id for task in plan.tasks}
    if set(assessment.relevant_investigation_task_ids) - task_ids:
        raise DecisionValidationError("Decision references an unknown or unrelated task")
    text = _assessment_text(assessment)
    if _EMAIL.search(text) or _PHONE.search(text) or _PII_TERMS.search(text):
        raise DecisionValidationError("Decision contains PII-shaped content")
    if _has_unsupported_causal_wording(text):
        raise DecisionValidationError("Decision contains an unsupported causal assertion")
    if _AUTONOMOUS_ASSERTION.search(text) or _AUTONOMOUS_STEP.search(text) or _FORBIDDEN_ACTION.search(text):
        raise DecisionValidationError("Decision contains imperative business action wording")
    if _FINANCIAL_ASSERTION.search(text) or _FORBIDDEN_FINANCIAL.search(text):
        raise DecisionValidationError("Decision contains an unsupported financial claim")
    if (
        assessment.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        and assessment.unresolved_requirement_ids
    ):
        raise DecisionValidationError("Ready decision has unresolved hard requirements")
    if assessment.readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY and not assessment.decision_boundary:
        raise DecisionValidationError("Blocked decision requires an explicit boundary")
    return assessment


def validate_decision_readiness_portfolio(
    portfolio: DecisionReadinessPortfolio,
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
) -> DecisionReadinessPortfolio:
    """Validate active-only coverage, ordering, counts, and every assessment."""
    validate_investigation_portfolio(
        investigation_portfolio, opportunity_evaluation, context
    )
    if portfolio.business_id != context.business_id or portfolio.as_of_date != context.as_of_date:
        raise DecisionValidationError("Decision portfolio crosses context identity")
    opportunities = {item.opportunity_id: item for item in opportunity_evaluation.opportunities}
    plans = {item.opportunity_id: item for item in investigation_portfolio.plans}
    expected_pairs = tuple(
        (opportunity, frame)
        for opportunity in sorted(
            opportunity_evaluation.opportunities,
            key=lambda item: (_PRIORITY_ORDER[item.priority], item.opportunity_order),
        )
        for frame in DECISION_FRAME_REGISTRY[opportunity.opportunity_type]
    )
    if len(portfolio.assessments) != len(expected_pairs):
        raise DecisionValidationError("Portfolio does not cover every registered active frame")
    if [item.decision_order for item in portfolio.assessments] != list(
        range(1, len(portfolio.assessments) + 1)
    ):
        raise DecisionValidationError("Decision ordering is invalid")
    ids = [item.decision_id for item in portfolio.assessments]
    if len(ids) != len(set(ids)):
        raise DecisionValidationError("Decision IDs or orders are duplicated")
    for assessment, (opportunity, frame) in zip(
        portfolio.assessments, expected_pairs, strict=True
    ):
        if opportunity.opportunity_id not in opportunities or opportunity.opportunity_id not in plans:
            raise DecisionValidationError("Decision requires an active opportunity and plan")
        validate_decision_assessment(
            assessment, context, opportunity, plans[opportunity.opportunity_id], frame
        )
    counts = (
        sum(item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW for item in portfolio.assessments),
        sum(item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE for item in portfolio.assessments),
        sum(item.readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY for item in portfolio.assessments),
    )
    if counts != (
        portfolio.ready_for_human_review_count,
        portfolio.needs_more_evidence_count,
        portfolio.blocked_by_boundary_count,
    ):
        raise DecisionValidationError("Decision readiness counts are inconsistent")
    first = next((
        item.decision_id for item in portfolio.assessments
        if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
    ), None)
    if portfolio.first_reviewable_decision_id != first:
        raise DecisionValidationError("First reviewable decision is inconsistent")
    return portfolio


def build_decision_readiness_portfolio(
    context: IntelligenceContext,
    opportunity_evaluation: OpportunityEvaluation,
    investigation_portfolio: InvestigationPortfolio,
) -> DecisionReadinessPortfolio:
    """Build readiness once from already-built, validated upstream layers."""
    validate_investigation_portfolio(
        investigation_portfolio, opportunity_evaluation, context
    )
    plan_by_opportunity = {
        plan.opportunity_id: plan for plan in investigation_portfolio.plans
    }
    ordered_opportunities = sorted(
        opportunity_evaluation.opportunities,
        key=lambda item: (_PRIORITY_ORDER[item.priority], item.opportunity_order),
    )
    assessments: list[DecisionReadinessAssessment] = []
    for opportunity in ordered_opportunities:
        plan = plan_by_opportunity.get(opportunity.opportunity_id)
        if plan is None:
            raise DecisionValidationError("Active opportunity has no validated investigation plan")
        frames = DECISION_FRAME_REGISTRY.get(opportunity.opportunity_type)
        if not frames:
            raise DecisionValidationError("Active opportunity has no decision frame registry")
        for frame in frames:
            assessments.append(assess_decision_frame(
                context,
                opportunity,
                plan,
                frame,
                decision_order=len(assessments) + 1,
            ))
    values = tuple(assessments)
    portfolio = DecisionReadinessPortfolio(
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        assessments=values,
        ready_for_human_review_count=sum(
            item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW for item in values
        ),
        needs_more_evidence_count=sum(
            item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE for item in values
        ),
        blocked_by_boundary_count=sum(
            item.readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY for item in values
        ),
        first_reviewable_decision_id=next((
            item.decision_id for item in values
            if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        ), None),
    )
    return validate_decision_readiness_portfolio(
        portfolio, context, opportunity_evaluation, investigation_portfolio
    )
