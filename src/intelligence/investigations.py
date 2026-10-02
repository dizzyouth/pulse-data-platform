"""Deterministic evidence-gap resolution and investigation planning."""

from __future__ import annotations

from dataclasses import dataclass, replace
import re
from src.intelligence.context import EvidenceItem, IntelligenceContext
from src.intelligence.investigation_models import (
    EvidenceGap,
    EvidenceRequirement,
    EvidenceRequirementStatus,
    InvestigationPlan,
    InvestigationPortfolio,
    InvestigationReadiness,
    InvestigationTask,
    InvestigationTaskKind,
    InvestigationValidationError,
    PlanStatus,
    stable_gap_id,
    stable_plan_id,
    stable_task_id,
)
from src.intelligence.narration import (
    _AUTONOMOUS_ASSERTION,
    _AUTONOMOUS_STEP,
    _EMAIL,
    _PHONE,
    _PII_TERMS,
    _has_unsupported_causal_wording,
)
from src.intelligence.opportunities import (
    _FINANCIAL_ASSERTION,
    _UNSAFE_HYPOTHESIS,
    evaluate_opportunities,
    validate_opportunity,
)
from src.intelligence.opportunity_models import (
    InvestigationOpportunity,
    OpportunityEvaluation,
    OpportunityPriority,
    OpportunityType,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceSelector:
    evidence_type: str | None = None
    evidence_id: str | None = None
    fact_key: str | None = None
    fact_values: tuple[str, ...] = ()
    match_opportunity_scope: bool = False
    opportunity_refs_only: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceRequirementSpec:
    requirement_id: str
    name: str
    description: str
    selector_groups: tuple[tuple[EvidenceSelector, ...], ...]
    collection_hint: str
    source_scope: str
    blocked_economic_status: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationTaskSpec:
    task_key: str
    task_kind: InvestigationTaskKind
    title: str
    objective: str
    requirement_ids: tuple[str, ...]
    expected_output: str
    completion_criteria: tuple[str, ...]
    criteria_link: str = "BOTH"
    include_when: str = "ALWAYS"
    limitation: str = (
        "This bounded analytical task does not establish a causal explanation "
        "or authorize an action."
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class InvestigationPlanSpec:
    plan_rule_id: str
    opportunity_type: OpportunityType
    tasks: tuple[InvestigationTaskSpec, ...]


def _selector(**values: object) -> tuple[EvidenceSelector, ...]:
    return (EvidenceSelector(**values),)


REQUIREMENT_REGISTRY: dict[str, EvidenceRequirementSpec] = {
    "requirement:confirmation_gap_baseline": EvidenceRequirementSpec(
        requirement_id="requirement:confirmation_gap_baseline",
        name="Confirmation-gap baseline",
        description="Validated matched-not-confirmed leakage and confirmation-leakage signal evidence.",
        selector_groups=(
            _selector(fact_key="leakage_stage", fact_values=("MATCHED_NOT_CONFIRMED",)),
            _selector(fact_key="signal_type", fact_values=("CONFIRMATION_LEAKAGE",)),
        ),
        collection_hint="Add validated aggregate confirmation-gap evidence to the intelligence context.",
        source_scope="BUSINESS_AGGREGATE",
    ),
    "requirement:identity_coverage": EvidenceRequirementSpec(
        requirement_id="requirement:identity_coverage",
        name="Identity and linkage coverage",
        description="Validated aggregate identity-resolution coverage relevant to this opportunity.",
        selector_groups=((
            EvidenceSelector(
                fact_key="leakage_stage", fact_values=("IDENTITY_UNRESOLVED",),
                opportunity_refs_only=True,
            ),
            EvidenceSelector(
                fact_key="signal_type", fact_values=("IDENTITY_RESOLUTION_GAP",),
                opportunity_refs_only=True,
            ),
        ),),
        collection_hint="Add validated aggregate identity-coverage evidence to the intelligence context.",
        source_scope="BUSINESS_OR_LINKED_COHORT",
    ),
    "requirement:confirmation_dispositions": EvidenceRequirementSpec(
        requirement_id="requirement:confirmation_dispositions",
        name="Aggregate confirmation dispositions",
        description="Aggregate categories describing matched-order confirmation dispositions.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("CONFIRMATION_DISPOSITIONS",)
        ),),
        collection_hint="Add validated aggregate confirmation-disposition evidence to the intelligence context.",
        source_scope="BUSINESS_AGGREGATE",
    ),
    "requirement:campaign_offer_mix": EvidenceRequirementSpec(
        requirement_id="requirement:campaign_offer_mix",
        name="Campaign offer and quantity mix",
        description="Validated aggregate offer and quantity mix for the selected campaign cohort.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("CAMPAIGN_OFFER_MIX",),
            match_opportunity_scope=True,
        ),),
        collection_hint="Add validated aggregate campaign offer-mix evidence to the intelligence context.",
        source_scope="CAMPAIGN_AGGREGATE",
    ),
    "requirement:upstream_order_cohorts": EvidenceRequirementSpec(
        requirement_id="requirement:upstream_order_cohorts",
        name="Upstream order-quality cohorts",
        description="Validated aggregate upstream order-quality cohort evidence.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("UPSTREAM_ORDER_COHORTS",)
        ),),
        collection_hint="Add validated aggregate upstream cohort evidence to the intelligence context.",
        source_scope="BUSINESS_OR_CAMPAIGN_AGGREGATE",
    ),
    "requirement:campaign_downstream_peer_evidence": EvidenceRequirementSpec(
        requirement_id="requirement:campaign_downstream_peer_evidence",
        name="Campaign downstream peer evidence",
        description="Campaign diagnostics plus validated delivery-gap or return-pressure signals.",
        selector_groups=(
            _selector(evidence_type="CAMPAIGN_DIAGNOSTIC", match_opportunity_scope=True),
            _selector(
                fact_key="signal_type",
                fact_values=("CAMPAIGN_DELIVERY_GAP", "CAMPAIGN_RETURN_PRESSURE"),
                match_opportunity_scope=True,
            ),
        ),
        collection_hint="Add validated aggregate campaign peer evidence to the intelligence context.",
        source_scope="CAMPAIGN_AGGREGATE",
    ),
    "requirement:adgroup_downstream_cohorts": EvidenceRequirementSpec(
        requirement_id="requirement:adgroup_downstream_cohorts",
        name="Below-campaign downstream cohorts",
        description="Adequately sized aggregate creative or ad-group downstream cohort evidence.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("ADGROUP_DOWNSTREAM_COHORTS",),
            match_opportunity_scope=True,
        ),),
        collection_hint="Add validated aggregate below-campaign downstream cohort evidence to the intelligence context.",
        source_scope="BELOW_CAMPAIGN_AGGREGATE",
    ),
    "requirement:platform_observed_gap": EvidenceRequirementSpec(
        requirement_id="requirement:platform_observed_gap",
        name="Platform versus observed measurement gap",
        description="Validated aggregate platform-versus-observed measurement evidence.",
        selector_groups=(_selector(
            fact_key="leakage_stage", fact_values=("PLATFORM_VS_OBSERVED",)
        ),),
        collection_hint="Add validated aggregate measurement-gap evidence to the intelligence context.",
        source_scope="BUSINESS_AGGREGATE",
    ),
    "requirement:platform_event_definition": EvidenceRequirementSpec(
        requirement_id="requirement:platform_event_definition",
        name="Platform conversion definition and attribution window",
        description="Documented aggregate platform conversion semantics and attribution window.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("PLATFORM_EVENT_DEFINITION",)
        ),),
        collection_hint="Add a validated aggregate platform-event definition to the intelligence context.",
        source_scope="MEASUREMENT_DOCUMENTATION",
    ),
    "requirement:lightfunnels_inclusion_rules": EvidenceRequirementSpec(
        requirement_id="requirement:lightfunnels_inclusion_rules",
        name="Observed-order inclusion rules",
        description="Documented aggregate Lightfunnels observed-order inclusion semantics.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("LIGHTFUNNELS_INCLUSION_RULES",)
        ),),
        collection_hint="Add validated aggregate observed-order inclusion rules to the intelligence context.",
        source_scope="MEASUREMENT_DOCUMENTATION",
    ),
    "requirement:acquisition_cost_peer_evidence": EvidenceRequirementSpec(
        requirement_id="requirement:acquisition_cost_peer_evidence",
        name="Peer-adjusted acquisition cost evidence",
        description="Campaign diagnostics plus a validated acquisition-cost-gap signal.",
        selector_groups=(
            _selector(evidence_type="CAMPAIGN_DIAGNOSTIC", match_opportunity_scope=True),
            _selector(
                fact_key="signal_type", fact_values=("CAMPAIGN_ACQUISITION_COST_GAP",),
                match_opportunity_scope=True,
            ),
        ),
        collection_hint="Add validated aggregate peer-adjusted acquisition evidence to the intelligence context.",
        source_scope="CAMPAIGN_AGGREGATE",
    ),
    "requirement:campaign_downstream_outcomes": EvidenceRequirementSpec(
        requirement_id="requirement:campaign_downstream_outcomes",
        name="Campaign downstream outcomes",
        description="Validated aggregate campaign delivery and return outcomes for contextual comparison.",
        selector_groups=(_selector(
            evidence_type="CAMPAIGN_DIAGNOSTIC", match_opportunity_scope=True
        ),),
        collection_hint="Add validated aggregate campaign downstream outcomes to the intelligence context.",
        source_scope="CAMPAIGN_AGGREGATE",
    ),
    "requirement:below_campaign_acquisition_cohorts": EvidenceRequirementSpec(
        requirement_id="requirement:below_campaign_acquisition_cohorts",
        name="Below-campaign acquisition cohorts",
        description="Adequately sized aggregate acquisition cohorts below campaign level.",
        selector_groups=(_selector(
            fact_key="evidence_dimension", fact_values=("BELOW_CAMPAIGN_ACQUISITION_COHORTS",),
            match_opportunity_scope=True,
        ),),
        collection_hint="Add validated aggregate below-campaign acquisition cohort evidence to the intelligence context.",
        source_scope="BELOW_CAMPAIGN_AGGREGATE",
    ),
    "requirement:trusted_cross_currency_economics": EvidenceRequirementSpec(
        requirement_id="requirement:trusted_cross_currency_economics",
        name="Trusted cross-currency economics",
        description="Trusted economic evidence required for cross-currency financial analysis.",
        selector_groups=(_selector(evidence_type="ECONOMIC_STATUS"),),
        collection_hint="Add validated trusted economics to the intelligence context before financial analysis.",
        source_scope="BUSINESS_ECONOMICS",
        blocked_economic_status="FX_REQUIRED",
    ),
}


def _task(
    task_key: str,
    task_kind: InvestigationTaskKind,
    title: str,
    objective: str,
    requirement_ids: tuple[str, ...],
    expected_output: str,
    completion: str,
    *,
    criteria_link: str = "BOTH",
    include_when: str = "ALWAYS",
    limitation: str | None = None,
) -> InvestigationTaskSpec:
    return InvestigationTaskSpec(
        task_key=task_key,
        task_kind=task_kind,
        title=title,
        objective=objective,
        requirement_ids=requirement_ids,
        expected_output=expected_output,
        completion_criteria=(completion,),
        criteria_link=criteria_link,
        include_when=include_when,
        limitation=limitation or InvestigationTaskSpec.__dataclass_fields__["limitation"].default,
    )


PLAN_REGISTRY: dict[OpportunityType, InvestigationPlanSpec] = {
    OpportunityType.CONFIRMATION_LEAKAGE: InvestigationPlanSpec(
        plan_rule_id="PLAN_001_CONFIRMATION_LEAKAGE",
        opportunity_type=OpportunityType.CONFIRMATION_LEAKAGE,
        tasks=(
            _task(
                "validate-confirmation-baseline", InvestigationTaskKind.VALIDATE_EXISTING_EVIDENCE,
                "Validate confirmation-gap baseline",
                "Validate the current matched-order confirmation gap and its aggregate denominator.",
                ("requirement:confirmation_gap_baseline",),
                "A confirmation-gap baseline showing the observed aggregate gap and denominator.",
                "Complete when matched-not-confirmed leakage and confirmation-leakage signal evidence resolve.",
            ),
            _task(
                "assess-identity-coverage", InvestigationTaskKind.RESOLVE_DATA_QUALITY,
                "Assess identity coverage boundary",
                "Assess how validated identity coverage constrains interpretation of the linked cohort.",
                ("requirement:identity_coverage",),
                "A bounded coverage assessment for the analyzed aggregate cohort.",
                "Complete when relevant identity-coverage evidence resolves for this opportunity.",
                criteria_link="CONTEXT",
                include_when="IDENTITY_CONTEXT",
            ),
            _task(
                "segment-confirmation-dispositions", InvestigationTaskKind.COLLECT_MISSING_EVIDENCE,
                "Segment confirmation dispositions",
                "Compare aggregate confirmation disposition categories without fabricating categories.",
                ("requirement:confirmation_dispositions",),
                "An aggregate distribution of validated confirmation dispositions.",
                "Complete when validated aggregate confirmation-disposition evidence is added to the intelligence context.",
            ),
            _task(
                "compare-offer-upstream-cohorts", InvestigationTaskKind.COMPARE_COHORTS,
                "Compare offer and upstream cohorts",
                "Compare aggregate offer mix and upstream order-quality cohorts against the confirmation gap.",
                ("requirement:campaign_offer_mix", "requirement:upstream_order_cohorts"),
                "An aggregate cohort comparison that can strengthen or weaken the current hypothesis.",
                "Complete when validated offer-mix and upstream cohort evidence is available in the intelligence context.",
            ),
        ),
    ),
    OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT: InvestigationPlanSpec(
        plan_rule_id="PLAN_002_ACQ_FULFILLMENT",
        opportunity_type=OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
        tasks=(
            _task(
                "validate-peer-pattern", InvestigationTaskKind.VALIDATE_EXISTING_EVIDENCE,
                "Validate campaign acquisition and downstream peer pattern",
                "Validate campaign diagnostics beside delivery-gap and return-pressure peer evidence.",
                ("requirement:campaign_downstream_peer_evidence",),
                "A peer-adjusted campaign comparison showing whether the downstream gap remains.",
                "Complete when campaign diagnostics and relevant downstream peer signals resolve for the selected scope.",
            ),
            _task(
                "assess-linkage-coverage", InvestigationTaskKind.RESOLVE_DATA_QUALITY,
                "Assess identity and linkage coverage",
                "Assess the validated identity coverage carried by this opportunity.",
                ("requirement:identity_coverage",),
                "A bounded linkage-coverage assessment for the selected campaign cohort.",
                "Complete when opportunity-relevant identity evidence resolves.",
                criteria_link="CONTEXT",
                include_when="IDENTITY_CONTEXT",
            ),
            _task(
                "compare-offer-mix", InvestigationTaskKind.COMPARE_COHORTS,
                "Compare campaign offer and quantity mix",
                "Compare aggregate campaign offer and quantity mix as a bounded descriptive analysis.",
                ("requirement:campaign_offer_mix",),
                "An aggregate offer-mix comparison that can strengthen or weaken the current hypothesis.",
                "Complete when validated campaign offer-mix evidence is available in the intelligence context.",
            ),
            _task(
                "compare-adgroup-cohorts", InvestigationTaskKind.COMPARE_COHORTS,
                "Compare creative or ad-group downstream cohorts",
                "Compare adequately sized below-campaign downstream cohorts using descriptive associations only.",
                ("requirement:adgroup_downstream_cohorts",),
                "A below-campaign downstream cohort comparison for the selected campaign.",
                "Complete when validated adequately sized below-campaign cohort evidence is available.",
            ),
        ),
    ),
    OpportunityType.MEASUREMENT_RECONCILIATION: InvestigationPlanSpec(
        plan_rule_id="PLAN_003_MEASUREMENT_RECONCILIATION",
        opportunity_type=OpportunityType.MEASUREMENT_RECONCILIATION,
        tasks=(
            _task(
                "validate-measurement-gap", InvestigationTaskKind.VALIDATE_EXISTING_EVIDENCE,
                "Validate current aggregate measurement gap",
                "Validate the bounded platform-versus-observed aggregate measurement difference.",
                ("requirement:platform_observed_gap",),
                "An aggregate measurement comparison under the current documented evidence semantics.",
                "Complete when platform-versus-observed evidence resolves from the validated context.",
                limitation=(
                    "This task does not establish lost orders, fraud, tracking failure, "
                    "operational loss, or financial impact."
                ),
            ),
            _task(
                "reconcile-platform-semantics", InvestigationTaskKind.RECONCILE_MEASUREMENT,
                "Reconcile platform event semantics",
                "Compare documented platform conversion definitions and attribution windows.",
                ("requirement:platform_event_definition",),
                "A documented mapping of platform conversion semantics and attribution windows.",
                "Complete when validated platform event definitions are available in the intelligence context.",
                limitation="This task reconciles definitions and does not establish operational loss or financial impact.",
            ),
            _task(
                "reconcile-observed-inclusion", InvestigationTaskKind.RECONCILE_MEASUREMENT,
                "Reconcile observed-order inclusion semantics",
                "Compare documented aggregate observed-order inclusion rules.",
                ("requirement:lightfunnels_inclusion_rules",),
                "A documented mapping of observed-order inclusion semantics.",
                "Complete when validated observed-order inclusion rules are available in the intelligence context.",
                limitation="This task reconciles definitions and does not establish tracking failure or financial impact.",
            ),
        ),
    ),
    OpportunityType.ACQUISITION_EFFICIENCY_REVIEW: InvestigationPlanSpec(
        plan_rule_id="PLAN_004_ACQUISITION_EFFICIENCY",
        opportunity_type=OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
        tasks=(
            _task(
                "validate-cost-gap", InvestigationTaskKind.VALIDATE_EXISTING_EVIDENCE,
                "Validate peer-adjusted acquisition cost gap",
                "Validate the aggregate cost-per-observed-order peer comparison.",
                ("requirement:acquisition_cost_peer_evidence",),
                "A peer-adjusted acquisition cost comparison for the selected campaign.",
                "Complete when campaign diagnostics and the acquisition-cost-gap signal resolve.",
            ),
            _task(
                "compare-downstream-evidence", InvestigationTaskKind.COMPARE_COHORTS,
                "Compare downstream counter-evidence",
                "Compare aggregate downstream outcomes before interpreting acquisition cost in isolation.",
                ("requirement:campaign_downstream_outcomes",),
                "A downstream outcome comparison that can strengthen or weaken the cost-gap hypothesis.",
                "Complete when validated campaign delivery and return outcomes resolve.",
            ),
            _task(
                "collect-below-campaign-cohorts", InvestigationTaskKind.COLLECT_MISSING_EVIDENCE,
                "Collect adequate below-campaign acquisition cohorts",
                "Identify the missing aggregate cohort evidence needed below campaign level.",
                ("requirement:below_campaign_acquisition_cohorts",),
                "An adequately sized aggregate below-campaign acquisition cohort comparison.",
                "Complete when validated below-campaign acquisition cohort evidence is added to the context.",
            ),
        ),
    ),
}


_READINESS_ORDER = {
    InvestigationReadiness.READY_NOW: 0,
    InvestigationReadiness.PARTIALLY_READY: 1,
    InvestigationReadiness.BLOCKED_MISSING_EVIDENCE: 2,
    InvestigationReadiness.BLOCKED_BOUNDARY: 3,
}
_PRIORITY_ORDER = {
    OpportunityPriority.HIGH: 0,
    OpportunityPriority.MEDIUM: 1,
    OpportunityPriority.LOW: 2,
}
_INVESTIGATION_CAUSAL_ASSERTION = re.compile(
    r"\b(?:the\s+)?reason\s+is\b|\btargeting\s+creates?\b|"
    r"\bfulfillment\s+caus(?:e|es|ed)\b",
    re.IGNORECASE,
)
_ECONOMIC_TOPIC = re.compile(
    r"\b(?:profit|contribution|margin|mer|roas|roi|recovered revenue|"
    r"revenue upside|expected upside)\b",
    re.IGNORECASE,
)
_INVESTIGATION_PII_TERMS = re.compile(
    r"\b(?:customer name|lead id|raw row|raw phone|postal address)\b",
    re.IGNORECASE,
)
_FORBIDDEN_EXECUTION = re.compile(
    r"\b(?:execute sql|trigger etl|deploy(?:ment)?|mutate external systems?|"
    r"pause (?:the )?campaign|increase (?:the )?budget|issue refunds?|"
    r"change (?:the )?fulfillment provider)\b",
    re.IGNORECASE,
)


def _opportunity_refs(opportunity: InvestigationOpportunity) -> set[str]:
    return set((
        *opportunity.supporting_evidence_refs,
        *opportunity.counter_evidence_refs,
        *opportunity.blocking_evidence_refs,
    ))


def _matches(
    item: EvidenceItem,
    selector: EvidenceSelector,
    opportunity: InvestigationOpportunity,
) -> bool:
    if selector.evidence_type is not None and item.evidence_type != selector.evidence_type:
        return False
    if selector.evidence_id is not None and item.evidence_id != selector.evidence_id:
        return False
    if selector.fact_key is not None:
        value = item.facts.get(selector.fact_key)
        if str(value) not in selector.fact_values:
            return False
    if selector.match_opportunity_scope and item.scope_name != opportunity.scope_name:
        return False
    if selector.opportunity_refs_only and item.evidence_id not in _opportunity_refs(opportunity):
        return False
    return True


def _resolve_requirement(
    spec: EvidenceRequirementSpec,
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
    task_ids: tuple[str, ...],
) -> EvidenceRequirement:
    economics = context.economic_status.facts.get("economic_status")
    if spec.blocked_economic_status == economics:
        return EvidenceRequirement(
            requirement_id=spec.requirement_id,
            name=spec.name,
            description=spec.description,
            status=EvidenceRequirementStatus.BLOCKED_BY_BOUNDARY,
            evidence_refs=(context.economic_status.evidence_id,),
            missing_reason="The current validated economic boundary does not support this analysis.",
            collection_hint=spec.collection_hint,
            source_scope=spec.source_scope,
            required_for_task_ids=task_ids,
        )
    group_refs: list[tuple[str, ...]] = []
    for group in spec.selector_groups:
        refs = tuple(
            item.evidence_id for item in context.evidence_items
            if any(_matches(item, selector, opportunity) for selector in group)
        )
        group_refs.append(tuple(dict.fromkeys(refs)))
    satisfied = sum(bool(refs) for refs in group_refs)
    refs = tuple(dict.fromkeys(ref for group in group_refs for ref in group))
    if satisfied == len(group_refs):
        status = EvidenceRequirementStatus.AVAILABLE
        missing_reason = None
    elif satisfied:
        status = EvidenceRequirementStatus.PARTIAL
        missing_reason = "Only part of this requirement is available in the current validated intelligence context."
    else:
        status = EvidenceRequirementStatus.MISSING_FROM_CONTEXT
        missing_reason = "Not available in the current validated intelligence context."
    return EvidenceRequirement(
        requirement_id=spec.requirement_id,
        name=spec.name,
        description=spec.description,
        status=status,
        evidence_refs=refs,
        missing_reason=missing_reason,
        collection_hint=spec.collection_hint,
        source_scope=spec.source_scope,
        required_for_task_ids=task_ids,
    )


def derive_task_readiness(
    requirements: tuple[EvidenceRequirement, ...],
) -> InvestigationReadiness:
    statuses = {item.status for item in requirements}
    if EvidenceRequirementStatus.BLOCKED_BY_BOUNDARY in statuses:
        return InvestigationReadiness.BLOCKED_BOUNDARY
    if EvidenceRequirementStatus.MISSING_FROM_CONTEXT in statuses:
        return InvestigationReadiness.BLOCKED_MISSING_EVIDENCE
    if EvidenceRequirementStatus.PARTIAL in statuses:
        return InvestigationReadiness.PARTIALLY_READY
    return InvestigationReadiness.READY_NOW


def _has_identity_context(opportunity: InvestigationOpportunity, context: IntelligenceContext) -> bool:
    spec = REQUIREMENT_REGISTRY["requirement:identity_coverage"]
    probe = _resolve_requirement(spec, opportunity, context, ("investigation-task:probe:probe:probe",))
    return probe.status is not EvidenceRequirementStatus.MISSING_FROM_CONTEXT


def _criteria(
    link: str, opportunity: InvestigationOpportunity
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if link == "CONTEXT":
        return (), ()
    if link == "CONFIRM":
        return opportunity.confirmation_criteria, ()
    if link == "REFUTE":
        return (), opportunity.refutation_criteria
    return opportunity.confirmation_criteria, opportunity.refutation_criteria


def _plan_status(tasks: tuple[InvestigationTask, ...]) -> PlanStatus:
    if all(task.readiness is InvestigationReadiness.READY_NOW for task in tasks):
        return PlanStatus.READY
    if any(task.readiness in {
        InvestigationReadiness.READY_NOW,
        InvestigationReadiness.PARTIALLY_READY,
    } for task in tasks):
        return PlanStatus.PARTIAL
    return PlanStatus.BLOCKED


def _recommended_task(tasks: tuple[InvestigationTask, ...]) -> str | None:
    for readiness in (
        InvestigationReadiness.READY_NOW,
        InvestigationReadiness.PARTIALLY_READY,
    ):
        match = next((task for task in tasks if task.readiness is readiness), None)
        if match is not None:
            return match.task_id
    return None


def _selected_task_specs(
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
) -> tuple[InvestigationTaskSpec, ...]:
    plan_spec = PLAN_REGISTRY.get(opportunity.opportunity_type)
    if plan_spec is None:
        raise InvestigationValidationError("No investigation plan is registered for this opportunity type")
    return tuple(
        spec for spec in plan_spec.tasks
        if spec.include_when == "ALWAYS"
        or (spec.include_when == "IDENTITY_CONTEXT" and _has_identity_context(opportunity, context))
    )


def _build_investigation_plan(
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
) -> InvestigationPlan:
    validate_opportunity(opportunity, context)
    selected_specs = _selected_task_specs(opportunity, context)
    task_id_by_key = {
        spec.task_key: stable_task_id(
            opportunity.opportunity_type.value,
            opportunity.scope_name,
            spec.task_key,
        )
        for spec in selected_specs
    }
    required_task_ids: dict[str, list[str]] = {}
    for spec in selected_specs:
        for requirement_id in spec.requirement_ids:
            required_task_ids.setdefault(requirement_id, []).append(task_id_by_key[spec.task_key])
    requirements = tuple(
        _resolve_requirement(
            REQUIREMENT_REGISTRY[requirement_id],
            opportunity,
            context,
            tuple(task_ids),
        )
        for requirement_id, task_ids in required_task_ids.items()
    )
    requirement_by_id = {item.requirement_id: item for item in requirements}
    draft_tasks: list[tuple[int, InvestigationTask]] = []
    for spec_order, spec in enumerate(selected_specs, 1):
        task_requirements = tuple(requirement_by_id[item] for item in spec.requirement_ids)
        readiness = derive_task_readiness(task_requirements)
        available_refs = tuple(dict.fromkeys(
            ref for requirement in task_requirements for ref in requirement.evidence_refs
        ))
        missing_ids = tuple(
            requirement.requirement_id for requirement in task_requirements
            if requirement.status is not EvidenceRequirementStatus.AVAILABLE
        )
        strengthens, weakens = _criteria(spec.criteria_link, opportunity)
        draft_tasks.append((spec_order, InvestigationTask(
            task_order=spec_order,
            task_id=task_id_by_key[spec.task_key],
            opportunity_id=opportunity.opportunity_id,
            task_kind=spec.task_kind,
            title=spec.title,
            objective=spec.objective,
            readiness=readiness,
            required_requirement_ids=spec.requirement_ids,
            available_evidence_refs=available_refs,
            missing_requirement_ids=missing_ids,
            expected_output=spec.expected_output,
            strengthens_criteria=strengthens,
            weakens_criteria=weakens,
            completion_criteria=spec.completion_criteria,
            limitation=spec.limitation,
            autonomous_action=False,
        )))
    draft_tasks.sort(key=lambda item: (_READINESS_ORDER[item[1].readiness], item[0]))
    tasks = tuple(
        replace(task, task_order=index)
        for index, (_, task) in enumerate(draft_tasks, 1)
    )
    unresolved = tuple(
        item.requirement_id for item in requirements
        if item.status is not EvidenceRequirementStatus.AVAILABLE
    )
    plan = InvestigationPlan(
        plan_id=stable_plan_id(opportunity.opportunity_type.value, opportunity.scope_name),
        opportunity_id=opportunity.opportunity_id,
        business_id=opportunity.business_id,
        as_of_date=opportunity.as_of_date,
        opportunity_priority=opportunity.priority,
        opportunity_confidence=opportunity.confidence,
        status=_plan_status(tasks),
        tasks=tasks,
        recommended_start_task_id=_recommended_task(tasks),
        evidence_requirements=requirements,
        unresolved_requirement_ids=unresolved,
        decision_unlocked=opportunity.decision_unlocked,
        limitation=(
            "This deterministic investigation plan uses only the current validated "
            "aggregate intelligence context and does not authorize an action."
        ),
    )
    return validate_investigation_plan(plan, opportunity, context)


def build_investigation_plan(
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
    evaluation: OpportunityEvaluation | None = None,
) -> InvestigationPlan:
    """Build one plan, rejecting opportunities not active in the supplied context."""
    resolved = evaluation or evaluate_opportunities(context)
    if resolved.business_id != context.business_id or resolved.as_of_date != context.as_of_date:
        raise InvestigationValidationError("Opportunity evaluation crosses context identity")
    active = {item.opportunity_id: item for item in resolved.opportunities}
    if opportunity.opportunity_id not in active or active[opportunity.opportunity_id] != opportunity:
        raise InvestigationValidationError("Investigation plans require an active opportunity")
    return _build_investigation_plan(opportunity, context)


def _plan_text(plan: InvestigationPlan) -> str:
    values = [plan.decision_unlocked, plan.limitation]
    for task in plan.tasks:
        values.extend((task.title, task.objective, task.expected_output, task.limitation))
        values.extend(task.completion_criteria)
    for requirement in plan.evidence_requirements:
        values.extend((requirement.name, requirement.description, requirement.collection_hint))
        if requirement.missing_reason:
            values.append(requirement.missing_reason)
    return "\n".join(values)


def validate_investigation_plan(
    plan: InvestigationPlan,
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
) -> InvestigationPlan:
    """Fail closed on identity, grounding, readiness, ordering, and safety."""
    validate_opportunity(opportunity, context)
    if (
        plan.opportunity_id != opportunity.opportunity_id
        or plan.business_id != context.business_id
        or plan.as_of_date != context.as_of_date
        or plan.plan_id != stable_plan_id(opportunity.opportunity_type.value, opportunity.scope_name)
        or plan.opportunity_priority is not opportunity.priority
        or plan.opportunity_confidence is not opportunity.confidence
        or plan.decision_unlocked != opportunity.decision_unlocked
    ):
        raise InvestigationValidationError("Plan crosses or changes opportunity identity")

    selected_specs = _selected_task_specs(opportunity, context)
    expected_task_ids = {
        stable_task_id(opportunity.opportunity_type.value, opportunity.scope_name, spec.task_key)
        for spec in selected_specs
    }
    if [task.task_order for task in plan.tasks] != list(range(1, len(plan.tasks) + 1)):
        raise InvestigationValidationError("Task ordering is invalid")
    task_ids = [task.task_id for task in plan.tasks]
    if len(task_ids) != len(set(task_ids)):
        raise InvestigationValidationError("Task IDs must be unique")
    if set(task_ids) != expected_task_ids:
        raise InvestigationValidationError("Task IDs do not match the registered plan")
    if any(task.opportunity_id != opportunity.opportunity_id for task in plan.tasks):
        raise InvestigationValidationError("Task crosses opportunity identity")
    readiness_order = [_READINESS_ORDER[task.readiness] for task in plan.tasks]
    if readiness_order != sorted(readiness_order):
        raise InvestigationValidationError("Tasks do not follow readiness ordering")
    requirement_by_id = {item.requirement_id: item for item in plan.evidence_requirements}
    if len(requirement_by_id) != len(plan.evidence_requirements):
        raise InvestigationValidationError("Requirement IDs must be unique")
    known = context.evidence_by_id
    required_task_ids: dict[str, list[str]] = {}
    for spec in selected_specs:
        task_id = stable_task_id(
            opportunity.opportunity_type.value, opportunity.scope_name, spec.task_key
        )
        for requirement_id in spec.requirement_ids:
            required_task_ids.setdefault(requirement_id, []).append(task_id)
    if set(requirement_by_id) != set(required_task_ids):
        raise InvestigationValidationError("Requirements do not match the registered plan")
    for requirement in plan.evidence_requirements:
        expected_requirement = _resolve_requirement(
            REQUIREMENT_REGISTRY[requirement.requirement_id],
            opportunity,
            context,
            tuple(required_task_ids[requirement.requirement_id]),
        )
        if requirement != expected_requirement:
            raise InvestigationValidationError("Requirement does not match validated context evidence")
        unknown = set(requirement.evidence_refs) - known.keys()
        if unknown or any(known[ref].business_id != context.business_id for ref in requirement.evidence_refs):
            raise InvestigationValidationError("Requirement has unknown or cross-business evidence")
        if set(requirement.required_for_task_ids) - set(task_ids):
            raise InvestigationValidationError("Requirement references an unknown task")
    for task in plan.tasks:
        try:
            requirements = tuple(requirement_by_id[item] for item in task.required_requirement_ids)
        except KeyError:
            raise InvestigationValidationError("Task references an unknown requirement") from None
        expected_readiness = derive_task_readiness(requirements)
        if task.readiness is not expected_readiness:
            raise InvestigationValidationError("Task readiness does not match requirements")
        expected_missing = tuple(
            item.requirement_id for item in requirements
            if item.status is not EvidenceRequirementStatus.AVAILABLE
        )
        if task.missing_requirement_ids != expected_missing:
            raise InvestigationValidationError("Task missing requirements are inconsistent")
        expected_refs = tuple(dict.fromkeys(
            ref for item in requirements for ref in item.evidence_refs
        ))
        if task.available_evidence_refs != expected_refs:
            raise InvestigationValidationError("Task available evidence is inconsistent")
        if not task.strengthens_criteria and not task.weakens_criteria:
            if task.task_kind is not InvestigationTaskKind.RESOLVE_DATA_QUALITY:
                raise InvestigationValidationError("Task is not linked to criteria or coverage")
        if set(task.strengthens_criteria) - set(opportunity.confirmation_criteria):
            raise InvestigationValidationError("Task invents confirmation criteria")
        if set(task.weakens_criteria) - set(opportunity.refutation_criteria):
            raise InvestigationValidationError("Task invents refutation criteria")
        if _AUTONOMOUS_STEP.search(task.objective):
            raise InvestigationValidationError("Task contains an autonomous action")
    expected_unresolved = tuple(
        item.requirement_id for item in plan.evidence_requirements
        if item.status is not EvidenceRequirementStatus.AVAILABLE
    )
    if plan.unresolved_requirement_ids != expected_unresolved:
        raise InvestigationValidationError("Plan unresolved requirements are inconsistent")
    if plan.status is not _plan_status(plan.tasks):
        raise InvestigationValidationError("Plan status is inconsistent")
    if plan.recommended_start_task_id != _recommended_task(plan.tasks):
        raise InvestigationValidationError("Recommended task is inconsistent")
    generated = _plan_text(plan)
    if (
        _EMAIL.search(generated)
        or _PHONE.search(generated)
        or _PII_TERMS.search(generated)
        or _INVESTIGATION_PII_TERMS.search(generated)
    ):
        raise InvestigationValidationError("Plan contains PII-shaped or customer-level content")
    if (
        _has_unsupported_causal_wording(generated)
        or _UNSAFE_HYPOTHESIS.search(generated)
        or _INVESTIGATION_CAUSAL_ASSERTION.search(generated)
    ):
        raise InvestigationValidationError("Plan contains unsupported causal wording")
    if _AUTONOMOUS_ASSERTION.search(generated) or _FORBIDDEN_EXECUTION.search(generated):
        raise InvestigationValidationError("Plan contains an autonomous action")
    if context.economic_status.facts.get("economic_status") == "FX_REQUIRED":
        if _FINANCIAL_ASSERTION.search(generated) or _ECONOMIC_TOPIC.search(generated):
            raise InvestigationValidationError("Plan violates the FX_REQUIRED boundary")
    return plan


def _aggregate_gaps(plans: tuple[InvestigationPlan, ...]) -> tuple[EvidenceGap, ...]:
    occurrences: dict[str, list[tuple[InvestigationPlan, EvidenceRequirement]]] = {}
    for plan in plans:
        for requirement in plan.evidence_requirements:
            if requirement.status is EvidenceRequirementStatus.MISSING_FROM_CONTEXT:
                occurrences.setdefault(requirement.requirement_id, []).append((plan, requirement))
    gaps: list[EvidenceGap] = []
    for requirement_id, entries in occurrences.items():
        opportunity_ids = tuple(dict.fromkeys(plan.opportunity_id for plan, _ in entries))
        task_ids = tuple(dict.fromkeys(
            task_id for _, requirement in entries for task_id in requirement.required_for_task_ids
        ))
        priorities = tuple(dict.fromkeys(plan.opportunity_priority for plan, _ in entries))
        requirement = entries[0][1]
        gaps.append(EvidenceGap(
            gap_id=stable_gap_id(requirement_id),
            requirement_id=requirement_id,
            description=requirement.description,
            affected_opportunity_ids=opportunity_ids,
            affected_task_ids=task_ids,
            opportunity_priorities=priorities,
            status=EvidenceRequirementStatus.MISSING_FROM_CONTEXT,
            limitation=(
                "This gap means the requirement is not available in the current validated "
                "intelligence context; it does not establish that the evidence does not exist elsewhere."
            ),
        ))
    return tuple(gaps)


def validate_investigation_portfolio(
    portfolio: InvestigationPortfolio,
    evaluation: OpportunityEvaluation,
    context: IntelligenceContext,
) -> InvestigationPortfolio:
    if portfolio.business_id != context.business_id or portfolio.as_of_date != context.as_of_date:
        raise InvestigationValidationError("Portfolio crosses context identity")
    active_ids = tuple(item.opportunity_id for item in evaluation.opportunities)
    plan_ids = tuple(item.opportunity_id for item in portfolio.plans)
    if plan_ids != active_ids:
        raise InvestigationValidationError("Portfolio must contain plans for active opportunities only")
    tasks = tuple(task for plan in portfolio.plans for task in plan.tasks)
    counts = (
        sum(task.readiness is InvestigationReadiness.READY_NOW for task in tasks),
        sum(task.readiness is InvestigationReadiness.PARTIALLY_READY for task in tasks),
        sum(task.readiness in {
            InvestigationReadiness.BLOCKED_MISSING_EVIDENCE,
            InvestigationReadiness.BLOCKED_BOUNDARY,
        } for task in tasks),
    )
    if counts != (
        portfolio.ready_task_count,
        portfolio.partial_task_count,
        portfolio.blocked_task_count,
    ):
        raise InvestigationValidationError("Portfolio task counts are inconsistent")
    recommended = None
    for readiness in (
        InvestigationReadiness.READY_NOW,
        InvestigationReadiness.PARTIALLY_READY,
    ):
        recommended = next((task.task_id for task in tasks if task.readiness is readiness), None)
        if recommended is not None:
            break
    if portfolio.recommended_start_task_id != recommended:
        raise InvestigationValidationError("Portfolio recommended task is inconsistent")
    if portfolio.evidence_gaps != _aggregate_gaps(portfolio.plans):
        raise InvestigationValidationError("Portfolio evidence gaps are inconsistent")
    return portfolio


def build_investigation_portfolio(
    context: IntelligenceContext,
    evaluation: OpportunityEvaluation | None = None,
) -> InvestigationPortfolio:
    """Build a deterministic business portfolio over active opportunities only."""
    resolved = evaluation or evaluate_opportunities(context)
    if resolved.business_id != context.business_id or resolved.as_of_date != context.as_of_date:
        raise InvestigationValidationError("Opportunity evaluation crosses context identity")
    plans = tuple(_build_investigation_plan(item, context) for item in resolved.opportunities)
    plans = tuple(sorted(
        plans,
        key=lambda plan: (
            _PRIORITY_ORDER[plan.opportunity_priority],
            next(
                item.opportunity_order for item in resolved.opportunities
                if item.opportunity_id == plan.opportunity_id
            ),
        ),
    ))
    tasks = tuple(task for plan in plans for task in plan.tasks)
    recommended = None
    for readiness in (
        InvestigationReadiness.READY_NOW,
        InvestigationReadiness.PARTIALLY_READY,
    ):
        recommended = next((task.task_id for task in tasks if task.readiness is readiness), None)
        if recommended is not None:
            break
    portfolio = InvestigationPortfolio(
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        plans=plans,
        evidence_gaps=_aggregate_gaps(plans),
        recommended_start_task_id=recommended,
        ready_task_count=sum(
            task.readiness is InvestigationReadiness.READY_NOW for task in tasks
        ),
        partial_task_count=sum(
            task.readiness is InvestigationReadiness.PARTIALLY_READY for task in tasks
        ),
        blocked_task_count=sum(
            task.readiness in {
                InvestigationReadiness.BLOCKED_MISSING_EVIDENCE,
                InvestigationReadiness.BLOCKED_BOUNDARY,
            }
            for task in tasks
        ),
    )
    return validate_investigation_portfolio(portfolio, resolved, context)
