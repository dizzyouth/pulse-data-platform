"""Deterministic cross-domain opportunity rules over validated aggregate evidence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
import math
import re
from typing import Iterable

from src.intelligence.context import EvidenceItem, IntelligenceContext
from src.intelligence.narration import (
    _AUTONOMOUS_ASSERTION,
    _AUTONOMOUS_STEP,
    _EMAIL,
    _PHONE,
    _PII_TERMS,
    _has_unsupported_causal_wording,
)
from src.intelligence.opportunity_models import (
    HypothesisStatus,
    InvestigationOpportunity,
    OpportunityCandidate,
    OpportunityCategory,
    OpportunityConfidence,
    OpportunityEvaluation,
    OpportunityPriority,
    OpportunityScope,
    OpportunityType,
    OpportunityValidationError,
    SuppressedOpportunity,
    SuppressionReason,
    stable_opportunity_id,
)


RULE_ACQUISITION_FULFILLMENT = "OPP_001_ACQ_FULFILLMENT_MISALIGNMENT"
RULE_CONFIRMATION_LEAKAGE = "OPP_002_CONFIRMATION_LEAKAGE"
RULE_MEASUREMENT_RECONCILIATION = "OPP_003_MEASUREMENT_RECONCILIATION"
RULE_ACQUISITION_EFFICIENCY = "OPP_004_ACQUISITION_EFFICIENCY"

MATERIAL_SPEND_SHARE = 0.20
HIGH_SPEND_SHARE = 0.35
MATERIAL_LEAKAGE_COUNT = 10.0
MATERIAL_LEAKAGE_RATE = 0.05
MIN_BUSINESS_SAMPLE = 30.0
HIGH_IMPACT_COUNT = 100.0
MEDIUM_IMPACT_COUNT = 25.0
MATERIAL_COST_RATIO = 1.25
HIGH_COST_RELATIVE_GAP = 0.50
STRONG_DOWNSTREAM_DELTA = 0.10
IDENTITY_BLOCKER_RATE = 0.10


@dataclass(frozen=True, slots=True, kw_only=True)
class OpportunityRuleSpec:
    rule_id: str
    opportunity_type: OpportunityType
    required_evidence_types: tuple[str, ...]
    eligibility: str
    priority_logic: str
    confidence_logic: str
    hypothesis_design: str
    confirmation_refutation_design: str
    relevant_blockers: tuple[str, ...]
    contextual_blockers: tuple[str, ...]
    irrelevant_blockers: tuple[str, ...]
    suppression_conditions: tuple[SuppressionReason, ...]


class BlockerRelevance(StrEnum):
    SCORING = "SCORING"
    CONTEXTUAL = "CONTEXTUAL"
    IRRELEVANT = "IRRELEVANT"


@dataclass(frozen=True, slots=True, kw_only=True)
class BlockerAssessment:
    attached_refs: tuple[str, ...]
    scoring_refs: tuple[str, ...]
    contextual_refs: tuple[str, ...]


RULE_REGISTRY = (
    OpportunityRuleSpec(
        rule_id=RULE_ACQUISITION_FULFILLMENT,
        opportunity_type=OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
        required_evidence_types=("CAMPAIGN_DIAGNOSTIC", "SIGNAL"),
        eligibility=(
            "Campaign spend share is at least 20%, fulfillment sample is MEDIUM/HIGH, "
            "and an existing delivery-gap or return-pressure signal is active."
        ),
        priority_logic=(
            "HIGH requires at least 35% spend share, HIGH sample, and both downstream "
            "signals; otherwise eligible candidates are MEDIUM, with identity blockers "
            "reducing one band."
        ),
        confidence_logic=(
            "HIGH requires HIGH fulfillment sample and both downstream signals; MEDIUM "
            "otherwise, reduced one band by material identity-resolution blockers."
        ),
        hypothesis_design="Test acquisition/offer and downstream alignment without causal attribution.",
        confirmation_refutation_design=(
            "Use adequate additional cohorts, peer-normalization, stage concentration, "
            "measurement reconciliation, and known data-quality explanations."
        ),
        relevant_blockers=("IDENTITY_RESOLUTION_COVERAGE",),
        contextual_blockers=(),
        irrelevant_blockers=("FX_REQUIRED", "MISSING_INITIAL_CURRENCY", "PLATFORM_VS_OBSERVED"),
        suppression_conditions=(
            SuppressionReason.INSUFFICIENT_SAMPLE,
            SuppressionReason.INSUFFICIENT_MATERIALITY,
            SuppressionReason.CONTRADICTORY_EVIDENCE,
            SuppressionReason.MISSING_REQUIRED_DOMAIN,
        ),
    ),
    OpportunityRuleSpec(
        rule_id=RULE_CONFIRMATION_LEAKAGE,
        opportunity_type=OpportunityType.CONFIRMATION_LEAKAGE,
        required_evidence_types=("LEAKAGE", "SIGNAL"),
        eligibility="At least 30 matched orders, 10 unconfirmed orders, and a 5% gap.",
        priority_logic=(
            "HIGH at 100 affected orders; MEDIUM at 25 affected orders or a 20% gap; "
            "otherwise LOW. Identity uncertainty inside the denominator reduces one band."
        ),
        confidence_logic=(
            "Denominator bands are LOW below 30, MEDIUM below 100, otherwise HIGH; "
            "identity gaps outside a resolved denominator are contextual only."
        ),
        hypothesis_design="Test process, contact, offer, and upstream-quality factors.",
        confirmation_refutation_design="Use aggregate disposition and repeated stage evidence.",
        relevant_blockers=("UNCERTAIN_DENOMINATOR_IDENTITY_QUALITY",),
        contextual_blockers=("IDENTITY_RESOLUTION_COVERAGE_OUTSIDE_COHORT",),
        irrelevant_blockers=("FX_REQUIRED", "MISSING_INITIAL_CURRENCY", "PLATFORM_VS_OBSERVED"),
        suppression_conditions=(
            SuppressionReason.INSUFFICIENT_SAMPLE,
            SuppressionReason.INSUFFICIENT_MATERIALITY,
            SuppressionReason.MISSING_REQUIRED_DOMAIN,
        ),
    ),
    OpportunityRuleSpec(
        rule_id=RULE_MEASUREMENT_RECONCILIATION,
        opportunity_type=OpportunityType.MEASUREMENT_RECONCILIATION,
        required_evidence_types=("LEAKAGE",),
        eligibility="At least 10 observations and a 5% platform-versus-observed gap.",
        priority_logic="Always LOW because this is measurement reconciliation, not operational loss.",
        confidence_logic="Uses the observed-order denominator sample band.",
        hypothesis_design="Test attribution and event-definition differences.",
        confirmation_refutation_design="Reconcile documented aggregate event semantics.",
        relevant_blockers=(),
        contextual_blockers=("PLATFORM_VS_OBSERVED_DIRECT_CONTEXT",),
        irrelevant_blockers=("IDENTITY_RESOLUTION", "FX_REQUIRED", "MISSING_INITIAL_CURRENCY"),
        suppression_conditions=(
            SuppressionReason.INSUFFICIENT_SAMPLE,
            SuppressionReason.INSUFFICIENT_MATERIALITY,
            SuppressionReason.MISSING_REQUIRED_DOMAIN,
        ),
    ),
    OpportunityRuleSpec(
        rule_id=RULE_ACQUISITION_EFFICIENCY,
        opportunity_type=OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
        required_evidence_types=("CAMPAIGN_DIAGNOSTIC", "SIGNAL"),
        eligibility="Cost per observed Lightfunnels order is at least 1.25x peers with MEDIUM/HIGH sample.",
        priority_logic=(
            "MEDIUM only for HIGH sample and at least 50% relative cost gap; otherwise LOW. "
            "One strong downstream counter-signal forces LOW and two suppress the candidate."
        ),
        confidence_logic="Starts from acquisition sample band and drops one band for counter-evidence.",
        hypothesis_design="Test acquisition efficiency without CAC, ROAS, profit, or budget claims.",
        confirmation_refutation_design="Use repeated peer-adjusted cost and downstream-quality evidence.",
        relevant_blockers=(),
        contextual_blockers=(),
        irrelevant_blockers=(
            "IDENTITY_RESOLUTION", "FX_REQUIRED", "MISSING_INITIAL_CURRENCY",
            "PLATFORM_VS_OBSERVED",
        ),
        suppression_conditions=(
            SuppressionReason.INSUFFICIENT_SAMPLE,
            SuppressionReason.INSUFFICIENT_MATERIALITY,
            SuppressionReason.CONTRADICTORY_EVIDENCE,
            SuppressionReason.MISSING_REQUIRED_DOMAIN,
        ),
    ),
)


_RULE_ORDER = {rule.rule_id: index for index, rule in enumerate(RULE_REGISTRY)}
_PRIORITY_ORDER = {
    OpportunityPriority.HIGH: 0,
    OpportunityPriority.MEDIUM: 1,
    OpportunityPriority.LOW: 2,
}
_FINANCIAL_ASSERTION = re.compile(
    r"\b(?:profit|contribution|margin|mer|roas)\b[^.!?]{0,20}\$?\d|"
    r"\b(?:recovered revenue|revenue upside|dollar upside)\b",
    re.IGNORECASE,
)
_LIMITATION = re.compile(
    r"\b(?:does not establish|cannot establish|not causal|remains untested)\b",
    re.IGNORECASE,
)
_UNSAFE_HYPOTHESIS = re.compile(
    r"\bbad customers?\b|"
    r"\bcreative\s+(?:is\s+)?attract(?:s|ing|ed)\b|"
    r"\b(?:is|are|was|were)\s+the\s+(?:reason|driver)\b",
    re.IGNORECASE,
)


def _number(item: EvidenceItem, key: str) -> float | None:
    value = item.facts.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _text_fact(item: EvidenceItem, key: str) -> str:
    value = item.facts.get(key)
    return value if isinstance(value, str) else ""


def _dedupe(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


def _signal_items(
    context: IntelligenceContext, signal_type: str, scope_name: str | None = None
) -> tuple[EvidenceItem, ...]:
    return tuple(
        item for item in context.evidence_items
        if item.evidence_type == "SIGNAL"
        and item.facts.get("signal_type") == signal_type
        and (scope_name is None or item.scope_name == scope_name)
    )


def _leakage_item(context: IntelligenceContext, stage: str) -> EvidenceItem | None:
    return next(
        (item for item in context.leakage if item.facts.get("leakage_stage") == stage),
        None,
    )


def _normal_anomaly_refs(
    context: IntelligenceContext, metrics: tuple[str, ...]
) -> tuple[str, ...]:
    wanted = set(metrics)
    return tuple(
        item.evidence_id for item in context.time_anomalies
        if item.facts.get("metric_name") in wanted and item.facts.get("status") == "NORMAL"
    )


def _sample_confidence(sample_size: float) -> OpportunityConfidence:
    if sample_size < 30:
        return OpportunityConfidence.LOW
    if sample_size < 100:
        return OpportunityConfidence.MEDIUM
    return OpportunityConfidence.HIGH


def _lower_priority(priority: OpportunityPriority) -> OpportunityPriority:
    return {
        OpportunityPriority.HIGH: OpportunityPriority.MEDIUM,
        OpportunityPriority.MEDIUM: OpportunityPriority.LOW,
        OpportunityPriority.LOW: OpportunityPriority.LOW,
    }[priority]


def _lower_confidence(confidence: OpportunityConfidence) -> OpportunityConfidence:
    return {
        OpportunityConfidence.HIGH: OpportunityConfidence.MEDIUM,
        OpportunityConfidence.MEDIUM: OpportunityConfidence.LOW,
        OpportunityConfidence.LOW: OpportunityConfidence.LOW,
    }[confidence]


def _blocker_kind(item: EvidenceItem) -> str | None:
    if item.evidence_type == "LEAKAGE":
        stage = item.facts.get("leakage_stage")
        if stage == "IDENTITY_UNRESOLVED":
            return "IDENTITY_RESOLUTION"
        if stage == "PLATFORM_VS_OBSERVED":
            return "PLATFORM_VS_OBSERVED"
    if item.evidence_type == "ECONOMIC_STATUS":
        status = str(item.facts.get("economic_status", ""))
        if status == "FX_REQUIRED":
            return "FX_REQUIRED"
        if "MISSING" in status and "CURRENCY" in status:
            return "MISSING_INITIAL_CURRENCY"
    if item.evidence_type == "SIGNAL":
        signal_type = item.facts.get("signal_type")
        if signal_type == "IDENTITY_RESOLUTION_GAP":
            return "IDENTITY_RESOLUTION"
        if signal_type == "PLATFORM_OBSERVED_GAP":
            return "PLATFORM_VS_OBSERVED"
        if signal_type == "MISSING_INITIAL_CURRENCY":
            return "MISSING_INITIAL_CURRENCY"
    return None


def _known_blockers(context: IntelligenceContext) -> tuple[EvidenceItem, ...]:
    blockers: list[EvidenceItem] = []
    identity = _leakage_item(context, "IDENTITY_UNRESOLVED")
    if identity is not None and (_number(identity, "observed_rate") or 0) >= IDENTITY_BLOCKER_RATE:
        blockers.append(identity)
    elif identity is None:
        blockers.extend(_signal_items(context, "IDENTITY_RESOLUTION_GAP")[:1])

    blockers.extend(_signal_items(context, "MISSING_INITIAL_CURRENCY")[:1])
    if _blocker_kind(context.economic_status) is not None:
        blockers.append(context.economic_status)

    platform = _leakage_item(context, "PLATFORM_VS_OBSERVED")
    if platform is not None and (_number(platform, "observed_rate") or 0) >= MATERIAL_LEAKAGE_RATE:
        blockers.append(platform)
    return tuple({item.evidence_id: item for item in blockers}.values())


def classify_blocker_relevance(
    rule_id: str,
    opportunity_scope: OpportunityScope,
    blocker_evidence: EvidenceItem,
    supporting_evidence: tuple[EvidenceItem, ...],
) -> BlockerRelevance:
    """Classify whether one validated limitation constrains a specific rule."""
    kind = _blocker_kind(blocker_evidence)
    if kind is None:
        return BlockerRelevance.IRRELEVANT
    if rule_id == RULE_ACQUISITION_FULFILLMENT:
        if opportunity_scope is OpportunityScope.CAMPAIGN and kind == "IDENTITY_RESOLUTION":
            return BlockerRelevance.SCORING
        return BlockerRelevance.IRRELEVANT
    if rule_id == RULE_CONFIRMATION_LEAKAGE and kind == "IDENTITY_RESOLUTION":
        confirmation = next(
            (
                item for item in supporting_evidence
                if item.evidence_type == "LEAKAGE"
                and item.facts.get("leakage_stage") == "MATCHED_NOT_CONFIRMED"
            ),
            None,
        )
        denominator_quality = (
            str(confirmation.facts.get(
                "denominator_identity_quality", "HIGH_CONFIDENCE_RESOLVED"
            ))
            if confirmation is not None else "UNKNOWN"
        )
        if denominator_quality == "HIGH_CONFIDENCE_RESOLVED":
            return BlockerRelevance.CONTEXTUAL
        return BlockerRelevance.SCORING
    return BlockerRelevance.IRRELEVANT


def _assess_blockers(
    rule_id: str,
    opportunity_scope: OpportunityScope,
    context: IntelligenceContext,
    supporting_refs: tuple[str, ...],
) -> BlockerAssessment:
    supporting = tuple(context.evidence_by_id[ref] for ref in supporting_refs)
    attached: list[str] = []
    scoring: list[str] = []
    contextual: list[str] = []
    for blocker in _known_blockers(context):
        if blocker.evidence_id in supporting_refs:
            continue
        relevance = classify_blocker_relevance(
            rule_id, opportunity_scope, blocker, supporting
        )
        if relevance is BlockerRelevance.IRRELEVANT:
            continue
        attached.append(blocker.evidence_id)
        if relevance is BlockerRelevance.SCORING:
            scoring.append(blocker.evidence_id)
        else:
            contextual.append(blocker.evidence_id)
    return BlockerAssessment(
        attached_refs=tuple(attached),
        scoring_refs=tuple(scoring),
        contextual_refs=tuple(contextual),
    )


def _suppressed(
    *, rule_id: str, scope_type: OpportunityScope, scope_id: str | None,
    scope_name: str, reason: SuppressionReason, refs: Iterable[str], explanation: str,
) -> SuppressedOpportunity:
    return SuppressedOpportunity(
        rule_id=rule_id,
        scope_type=scope_type,
        scope_id=scope_id,
        scope_name=scope_name,
        reason_code=reason,
        evidence_refs=_dedupe(refs),
        explanation=explanation,
    )


def _campaign_scope_id(item: EvidenceItem) -> str:
    return str(item.facts.get("campaign_name", item.scope_name))


def _candidate_acquisition_fulfillment(
    context: IntelligenceContext,
) -> tuple[list[OpportunityCandidate], list[SuppressedOpportunity]]:
    candidates: list[OpportunityCandidate] = []
    suppressed: list[SuppressedOpportunity] = []
    campaigns = tuple(context.campaign_diagnostics)
    campaign_spends = tuple(_number(item, "spend_usd") for item in campaigns)
    complete_spend_domain = bool(campaign_spends) and all(
        value is not None for value in campaign_spends
    )
    total_spend = sum(value or 0 for value in campaign_spends)
    all_campaign_refs = tuple(item.evidence_id for item in campaigns)
    for campaign in campaigns:
        name = campaign.scope_name
        scope_id = _campaign_scope_id(campaign)
        spend = _number(campaign, "spend_usd")
        delivery = _signal_items(context, "CAMPAIGN_DELIVERY_GAP", name)
        returns = _signal_items(context, "CAMPAIGN_RETURN_PRESSURE", name)
        weak_refs = tuple(item.evidence_id for item in (*delivery, *returns))
        base_refs = _dedupe((campaign.evidence_id, *weak_refs))
        if spend is None or not complete_spend_domain or total_spend <= 0:
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_FULFILLMENT,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.MISSING_REQUIRED_DOMAIN,
                refs=base_refs,
                explanation="Campaign spend evidence is unavailable for materiality evaluation.",
            ))
            continue
        if not weak_refs:
            delivery_rate = _number(campaign, "delivery_rate")
            peer_delivery_rate = _number(campaign, "peer_delivery_rate")
            return_rate = _number(campaign, "return_rate")
            peer_return_rate = _number(campaign, "peer_return_rate")
            if None in (delivery_rate, peer_delivery_rate, return_rate, peer_return_rate):
                reason = SuppressionReason.MISSING_REQUIRED_DOMAIN
                explanation = "Comparable downstream campaign evidence is unavailable."
            elif (
                delivery_rate - peer_delivery_rate >= STRONG_DOWNSTREAM_DELTA
                or peer_return_rate - return_rate >= STRONG_DOWNSTREAM_DELTA
            ):
                reason = SuppressionReason.CONTRADICTORY_EVIDENCE
                explanation = "Validated downstream evidence is materially better than peers."
            else:
                reason = SuppressionReason.INSUFFICIENT_MATERIALITY
                explanation = "No material validated delivery-gap or return-pressure signal is active."
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_FULFILLMENT,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=reason,
                refs=(campaign.evidence_id,),
                explanation=explanation,
            ))
            continue
        sample_band = _text_fact(campaign, "fulfillment_sample_band")
        if sample_band == "LOW":
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_FULFILLMENT,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.INSUFFICIENT_SAMPLE,
                refs=base_refs,
                explanation="Fulfillment sample band is LOW.",
            ))
            continue
        spend_share = spend / total_spend
        if spend_share < MATERIAL_SPEND_SHARE:
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_FULFILLMENT,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.INSUFFICIENT_MATERIALITY,
                refs=_dedupe((*all_campaign_refs, *weak_refs)),
                explanation="Campaign spend share is below the explicit 20% materiality band.",
            ))
            continue
        both_outcomes = bool(delivery and returns)
        priority = (
            OpportunityPriority.HIGH
            if spend_share >= HIGH_SPEND_SHARE and sample_band == "HIGH" and both_outcomes
            else OpportunityPriority.MEDIUM
        )
        confidence = (
            OpportunityConfidence.HIGH
            if sample_band == "HIGH" and both_outcomes
            else OpportunityConfidence.MEDIUM
        )
        impacts = [
            (item, _number(item, "impact_order_count")) for item in (*delivery, *returns)
        ]
        impacts = [(item, value) for item, value in impacts if value is not None]
        impact_item, impact_value = max(
            impacts, key=lambda pair: pair[1], default=(None, None)
        )
        impact_name = None
        if impact_item is not None:
            impact_name = (
                "delivery_benchmark_gap_orders"
                if impact_item.facts.get("signal_type") == "CAMPAIGN_DELIVERY_GAP"
                else "excess_returns_vs_peer"
            )
        normal_refs = _normal_anomaly_refs(
            context, ("delivered_order_volume", "returned_order_volume")
        )
        anomaly_context = (
            " Latest relevant anomaly states are NORMAL; this does not remove the "
            "structural peer pattern."
            if normal_refs else ""
        )
        support_refs = _dedupe((*all_campaign_refs, *weak_refs, *normal_refs))
        blockers = _assess_blockers(
            RULE_ACQUISITION_FULFILLMENT,
            OpportunityScope.CAMPAIGN,
            context,
            support_refs,
        )
        if blockers.scoring_refs:
            priority = _lower_priority(priority)
            confidence = _lower_confidence(confidence)
        coverage_limitation = (
            " Observed campaign metrics remain valid for the resolved linked cohort; "
            "unresolved identity evidence limits coverage and representativeness beyond "
            "that cohort."
            if blockers.attached_refs else ""
        )
        opportunity = InvestigationOpportunity(
            opportunity_order=0,
            opportunity_id=stable_opportunity_id(
                OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
                OpportunityScope.CAMPAIGN,
                name,
            ),
            business_id=context.business_id,
            as_of_date=context.as_of_date,
            scope_type=OpportunityScope.CAMPAIGN,
            scope_id=scope_id,
            scope_name=name,
            opportunity_type=OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
            category=OpportunityCategory.CROSS_DOMAIN,
            title="Investigate acquisition and fulfillment alignment",
            observation_summary=(
                f"Campaign spend share is {spend_share * 100:.2f}% and occurs alongside "
                f"validated weak downstream peer evidence.{anomaly_context}"
            ),
            hypothesis_to_test=(
                "Acquisition or offer characteristics and downstream fulfillment outcomes "
                "may be misaligned for this campaign; current evidence does not identify the cause."
            ),
            hypothesis_status=HypothesisStatus.UNTESTED,
            priority=priority,
            confidence=confidence,
            impact_proxy_name=impact_name,
            impact_proxy_value=impact_value,
            impact_proxy_unit="ORDERS" if impact_value is not None else None,
            supporting_evidence_refs=support_refs,
            counter_evidence_refs=(),
            blocking_evidence_refs=blockers.attached_refs,
            investigation_steps=(
                "Review campaign acquisition and observed-order evidence beside delivery and return peer gaps.",
                "Compare aggregate confirmation and downstream stage evidence for the campaign.",
                "Assess linked-cohort coverage before considering any spend change.",
            ),
            confirmation_criteria=(
                "The downstream peer pattern remains with additional adequately sized campaign cohorts.",
                "The observed gap remains after measurement and identity reconciliation.",
                "A specific aggregate downstream stage shows repeated concentration for the campaign.",
            ),
            refutation_criteria=(
                "Peer-adjusted downstream outcomes normalize with additional adequate sample.",
                "The apparent gap disappears after measurement reconciliation.",
                "The pattern is shown to be a known aggregate data-quality artifact.",
            ),
            decision_unlocked=(
                "Determine whether acquisition, offer, or fulfillment evidence should be "
                "investigated first before scaling or changing spend."
            ),
            missing_evidence=(
                "Aggregate offer and quantity mix by campaign cohort.",
                "Adequately sized creative or ad-group downstream cohort evidence.",
            ),
            limitation=(
                "This untested hypothesis does not establish a causal reason, forecast an "
                "outcome, value financial upside, or authorize a budget action."
                f"{coverage_limitation}"
            ),
        )
        candidates.append(OpportunityCandidate(
            rule_id=RULE_ACQUISITION_FULFILLMENT, opportunity=opportunity
        ))
    return candidates, suppressed


def _candidate_confirmation(
    context: IntelligenceContext,
) -> tuple[list[OpportunityCandidate], list[SuppressedOpportunity]]:
    item = _leakage_item(context, "MATCHED_NOT_CONFIRMED")
    signal = _signal_items(context, "CONFIRMATION_LEAKAGE")
    refs = _dedupe((item.evidence_id,) if item else ())
    if item is None:
        return [], [_suppressed(
            rule_id=RULE_CONFIRMATION_LEAKAGE,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.MISSING_REQUIRED_DOMAIN,
            refs=(),
            explanation="Matched-not-confirmed leakage evidence is unavailable.",
        )]
    observed = _number(item, "observed_count") or 0
    denominator = _number(item, "denominator_count") or 0
    rate = _number(item, "observed_rate") or 0
    if denominator < MIN_BUSINESS_SAMPLE:
        return [], [_suppressed(
            rule_id=RULE_CONFIRMATION_LEAKAGE,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.INSUFFICIENT_SAMPLE,
            refs=refs,
            explanation="Matched-order denominator is below the explicit sample floor of 30.",
        )]
    if observed < MATERIAL_LEAKAGE_COUNT or rate < MATERIAL_LEAKAGE_RATE:
        return [], [_suppressed(
            rule_id=RULE_CONFIRMATION_LEAKAGE,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.INSUFFICIENT_MATERIALITY,
            refs=refs,
            explanation="Confirmation leakage is below the explicit count or rate materiality band.",
        )]
    if not signal:
        return [], [_suppressed(
            rule_id=RULE_CONFIRMATION_LEAKAGE,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.MISSING_REQUIRED_DOMAIN,
            refs=refs,
            explanation="The validated confirmation-leakage signal is not active.",
        )]
    priority = (
        OpportunityPriority.HIGH if observed >= HIGH_IMPACT_COUNT
        else OpportunityPriority.MEDIUM
        if observed >= MEDIUM_IMPACT_COUNT or rate >= 0.20
        else OpportunityPriority.LOW
    )
    confidence = _sample_confidence(denominator)
    normal_refs = _normal_anomaly_refs(context, ("confirmed_order_volume",))
    anomaly_context = (
        " The latest confirmation-volume anomaly state is NORMAL; this does not remove "
        "the structural stage gap."
        if normal_refs else ""
    )
    support_refs = _dedupe(
        (item.evidence_id, *(x.evidence_id for x in signal), *normal_refs)
    )
    blockers = _assess_blockers(
        RULE_CONFIRMATION_LEAKAGE,
        OpportunityScope.BUSINESS,
        context,
        support_refs,
    )
    if blockers.scoring_refs:
        priority = _lower_priority(priority)
        confidence = _lower_confidence(confidence)
    if blockers.scoring_refs:
        identity_limitation = (
            " Identity-quality uncertainty within the analyzed denominator limits "
            "confidence in the observed cohort rate."
        )
    elif blockers.contextual_refs:
        identity_limitation = (
            " The opportunity applies to the resolved high-confidence cohort; unresolved "
            "identity records limit coverage beyond that cohort."
        )
    else:
        identity_limitation = ""
    missing_evidence = [
        "Aggregate confirmation disposition categories.",
        "Aggregate contact-attempt and offer-mix cohorts.",
    ]
    if blockers.contextual_refs:
        missing_evidence.append("Coverage of unresolved identities outside the analyzed cohort.")
    opportunity = InvestigationOpportunity(
        opportunity_order=0,
        opportunity_id=stable_opportunity_id(
            OpportunityType.CONFIRMATION_LEAKAGE,
            OpportunityScope.BUSINESS,
            context.business_id,
        ),
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        scope_type=OpportunityScope.BUSINESS,
        scope_id=context.business_id,
        scope_name=context.business_id,
        opportunity_type=OpportunityType.CONFIRMATION_LEAKAGE,
        category=OpportunityCategory.CONFIRMATION,
        title="Investigate matched-order confirmation leakage",
        observation_summary=(
            f"{observed:g} of {denominator:g} high-confidence matched orders are not "
            f"observed as confirmed ({rate * 100:.2f}%).{anomaly_context}"
        ),
        hypothesis_to_test=(
            "Process, contact, offer, or upstream order-quality factors may be associated "
            "with the observed confirmation gap; current evidence does not identify the cause."
        ),
        hypothesis_status=HypothesisStatus.UNTESTED,
        priority=priority,
        confidence=confidence,
        impact_proxy_name="matched_not_confirmed_orders",
        impact_proxy_value=observed,
        impact_proxy_unit="ORDERS",
        supporting_evidence_refs=support_refs,
        counter_evidence_refs=(),
        blocking_evidence_refs=blockers.attached_refs,
        investigation_steps=(
            "Review aggregate confirmation operations and matched-order disposition evidence.",
            "Compare confirmation-stage evidence with aggregate offer and upstream order-quality cohorts.",
        ),
        confirmation_criteria=(
            "The gap remains material across additional adequately sized matched-order cohorts.",
            "Aggregate disposition evidence identifies repeated concentration at confirmation.",
        ),
        refutation_criteria=(
            "The gap normalizes with additional adequately sized cohorts.",
            "Measurement or identity reconciliation removes the apparent confirmation gap.",
        ),
        decision_unlocked=(
            "Determine whether the next intervention should focus on confirmation operations, "
            "offer quality, or upstream lead and order quality."
        ),
        missing_evidence=tuple(missing_evidence),
        limitation=(
            "The observed stage gap does not establish why orders were not confirmed and "
            "does not authorize customer contact or an operational change."
            f"{identity_limitation}"
        ),
    )
    return [OpportunityCandidate(rule_id=RULE_CONFIRMATION_LEAKAGE, opportunity=opportunity)], []


def _candidate_measurement(
    context: IntelligenceContext,
) -> tuple[list[OpportunityCandidate], list[SuppressedOpportunity]]:
    item = _leakage_item(context, "PLATFORM_VS_OBSERVED")
    signal = _signal_items(context, "PLATFORM_OBSERVED_GAP")
    if item is None or item.facts.get("is_measurement_gap") is not True:
        return [], [_suppressed(
            rule_id=RULE_MEASUREMENT_RECONCILIATION,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.MISSING_REQUIRED_DOMAIN,
            refs=(() if item is None else (item.evidence_id,)),
            explanation="Validated measurement-gap evidence is unavailable.",
        )]
    observed = _number(item, "observed_count") or 0
    denominator = _number(item, "denominator_count") or 0
    rate = _number(item, "observed_rate") or 0
    refs = (item.evidence_id,)
    if denominator < MIN_BUSINESS_SAMPLE:
        return [], [_suppressed(
            rule_id=RULE_MEASUREMENT_RECONCILIATION,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.INSUFFICIENT_SAMPLE,
            refs=refs,
            explanation="Measurement denominator is below the explicit sample floor of 30.",
        )]
    if observed < MATERIAL_LEAKAGE_COUNT or rate < MATERIAL_LEAKAGE_RATE:
        return [], [_suppressed(
            rule_id=RULE_MEASUREMENT_RECONCILIATION,
            scope_type=OpportunityScope.BUSINESS,
            scope_id=context.business_id,
            scope_name=context.business_id,
            reason=SuppressionReason.INSUFFICIENT_MATERIALITY,
            refs=refs,
            explanation="Platform-versus-observed difference is below the materiality band.",
        )]
    opportunity = InvestigationOpportunity(
        opportunity_order=0,
        opportunity_id=stable_opportunity_id(
            OpportunityType.MEASUREMENT_RECONCILIATION,
            OpportunityScope.BUSINESS,
            context.business_id,
        ),
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        scope_type=OpportunityScope.BUSINESS,
        scope_id=context.business_id,
        scope_name=context.business_id,
        opportunity_type=OpportunityType.MEASUREMENT_RECONCILIATION,
        category=OpportunityCategory.MEASUREMENT,
        title="Reconcile platform and observed-order measurement",
        observation_summary=(
            f"Platform-reported conversions differ from observed Lightfunnels orders by "
            f"{observed:g} of {denominator:g} aggregate observations under their current "
            "measurement semantics."
        ),
        hypothesis_to_test=(
            "Attribution windows or event definitions may be associated with the observed "
            "measurement difference; current evidence does not establish the cause."
        ),
        hypothesis_status=HypothesisStatus.UNTESTED,
        priority=OpportunityPriority.LOW,
        confidence=_sample_confidence(denominator),
        impact_proxy_name="platform_vs_observed_count_difference",
        impact_proxy_value=observed,
        impact_proxy_unit="OBSERVATIONS",
        supporting_evidence_refs=_dedupe((item.evidence_id, *(x.evidence_id for x in signal))),
        counter_evidence_refs=(),
        blocking_evidence_refs=(),
        investigation_steps=(
            "Reconcile aggregate platform conversion and Lightfunnels order event definitions.",
            "Compare attribution windows and aggregate event inclusion rules.",
        ),
        confirmation_criteria=(
            "Documented event definitions account for a repeatable portion of the aggregate difference.",
            "The difference remains visible under aligned comparison windows.",
        ),
        refutation_criteria=(
            "Aligned event definitions and windows remove the aggregate difference.",
            "The apparent difference is shown to be a reporting-timing artifact.",
        ),
        decision_unlocked=(
            "Determine whether attribution and event semantics need reconciliation before "
            "platform conversion evidence is used for further decisions."
        ),
        missing_evidence=(
            "Documented platform conversion event definition and attribution window.",
            "Documented Lightfunnels observed-order inclusion rules.",
        ),
        limitation=(
            "This measurement opportunity does not establish lost orders, fraud, tracking "
            "failure, operational loss, or financial upside."
        ),
    )
    return [OpportunityCandidate(rule_id=RULE_MEASUREMENT_RECONCILIATION, opportunity=opportunity)], []


def _candidate_acquisition_efficiency(
    context: IntelligenceContext,
) -> tuple[list[OpportunityCandidate], list[SuppressedOpportunity]]:
    candidates: list[OpportunityCandidate] = []
    suppressed: list[SuppressedOpportunity] = []
    for campaign in context.campaign_diagnostics:
        name = campaign.scope_name
        scope_id = _campaign_scope_id(campaign)
        cost = _number(campaign, "cost_per_lightfunnels_order_usd")
        peer_cost = _number(campaign, "peer_cost_per_lightfunnels_order_usd")
        signal = _signal_items(context, "CAMPAIGN_ACQUISITION_COST_GAP", name)
        refs = _dedupe((campaign.evidence_id, *(item.evidence_id for item in signal)))
        if cost is None or peer_cost is None or peer_cost <= 0:
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_EFFICIENCY,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.MISSING_REQUIRED_DOMAIN,
                refs=refs,
                explanation="Comparable campaign cost-per-observed-order evidence is unavailable.",
            ))
            continue
        if cost < peer_cost * MATERIAL_COST_RATIO:
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_EFFICIENCY,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.INSUFFICIENT_MATERIALITY,
                refs=(campaign.evidence_id,),
                explanation="Cost per observed order is below the explicit 1.25x peer band.",
            ))
            continue
        sample_band = _text_fact(campaign, "acquisition_sample_band")
        if sample_band == "LOW":
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_EFFICIENCY,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.INSUFFICIENT_SAMPLE,
                refs=(campaign.evidence_id,),
                explanation="Acquisition sample band is LOW.",
            ))
            continue
        if not signal:
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_EFFICIENCY,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.MISSING_REQUIRED_DOMAIN,
                refs=(campaign.evidence_id,),
                explanation="The validated acquisition-cost-gap signal is not active.",
            ))
            continue
        delivery = _number(campaign, "delivery_rate")
        peer_delivery = _number(campaign, "peer_delivery_rate")
        returns = _number(campaign, "return_rate")
        peer_returns = _number(campaign, "peer_return_rate")
        strong_delivery = bool(
            delivery is not None and peer_delivery is not None
            and delivery - peer_delivery >= STRONG_DOWNSTREAM_DELTA
        )
        strong_returns = bool(
            returns is not None and peer_returns is not None
            and peer_returns - returns >= STRONG_DOWNSTREAM_DELTA
        )
        counter_count = int(strong_delivery) + int(strong_returns)
        if counter_count == 2:
            suppressed.append(_suppressed(
                rule_id=RULE_ACQUISITION_EFFICIENCY,
                scope_type=OpportunityScope.CAMPAIGN,
                scope_id=scope_id,
                scope_name=name,
                reason=SuppressionReason.CONTRADICTORY_EVIDENCE,
                refs=refs,
                explanation=(
                    "Both delivery and return outcomes are materially better than peers, "
                    "contradicting a standalone acquisition-efficiency concern."
                ),
            ))
            continue
        signal_item = signal[0]
        relative_gap = _number(signal_item, "relative_gap") or 0
        priority = (
            OpportunityPriority.MEDIUM
            if sample_band == "HIGH" and relative_gap >= HIGH_COST_RELATIVE_GAP
            else OpportunityPriority.LOW
        )
        confidence = (
            OpportunityConfidence.HIGH if sample_band == "HIGH"
            else OpportunityConfidence.MEDIUM
        )
        counter_refs: tuple[str, ...] = ()
        if counter_count == 1:
            priority = OpportunityPriority.LOW
            confidence = _lower_confidence(confidence)
            counter_refs = (campaign.evidence_id,)
        support_refs = tuple(item.evidence_id for item in signal)
        blockers = _assess_blockers(
            RULE_ACQUISITION_EFFICIENCY,
            OpportunityScope.CAMPAIGN,
            context,
            support_refs,
        )
        opportunity = InvestigationOpportunity(
            opportunity_order=0,
            opportunity_id=stable_opportunity_id(
                OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
                OpportunityScope.CAMPAIGN,
                name,
            ),
            business_id=context.business_id,
            as_of_date=context.as_of_date,
            scope_type=OpportunityScope.CAMPAIGN,
            scope_id=scope_id,
            scope_name=name,
            opportunity_type=OpportunityType.ACQUISITION_EFFICIENCY_REVIEW,
            category=OpportunityCategory.ACQUISITION,
            title="Review acquisition cost efficiency",
            observation_summary=(
                "Validated USD cost per observed Lightfunnels order is materially above "
                "the leave-one-out campaign peer benchmark."
            ),
            hypothesis_to_test=(
                "Acquisition mix or measurement characteristics may be associated with the "
                "observed cost gap; current evidence does not identify the cause."
            ),
            hypothesis_status=HypothesisStatus.UNTESTED,
            priority=priority,
            confidence=confidence,
            impact_proxy_name="cost_per_lightfunnels_order_gap_usd",
            impact_proxy_value=_number(signal_item, "absolute_gap"),
            impact_proxy_unit="USD_PER_OBSERVED_ORDER",
            supporting_evidence_refs=support_refs,
            counter_evidence_refs=counter_refs,
            blocking_evidence_refs=blockers.attached_refs,
            investigation_steps=(
                "Review aggregate campaign acquisition cost and observed-order measurement inputs.",
                "Compare downstream quality evidence before interpreting acquisition cost in isolation.",
            ),
            confirmation_criteria=(
                "The peer-adjusted cost gap remains with additional adequate observed-order sample.",
                "Downstream quality evidence does not offset the acquisition-cost concern.",
            ),
            refutation_criteria=(
                "The cost gap normalizes with additional adequate sample or measurement reconciliation.",
                "Materially stronger downstream outcomes make the isolated cost gap non-decisive.",
            ),
            decision_unlocked=(
                "Determine whether acquisition efficiency deserves further testing before any "
                "campaign or spend decision is considered."
            ),
            missing_evidence=(
                "Adequately sized acquisition cohort evidence below campaign level.",
            ),
            limitation=(
                "This peer comparison does not establish causal inefficiency, CAC, profit, "
                "contribution, MER, ROAS, or authorize a budget action."
            ),
        )
        candidates.append(OpportunityCandidate(
            rule_id=RULE_ACQUISITION_EFFICIENCY, opportunity=opportunity
        ))
    return candidates, suppressed


def _opportunity_text(opportunity: InvestigationOpportunity) -> str:
    return "\n".join((
        opportunity.business_id,
        opportunity.scope_id or "",
        opportunity.scope_name,
        opportunity.title,
        opportunity.observation_summary,
        opportunity.hypothesis_to_test,
        *opportunity.investigation_steps,
        *opportunity.confirmation_criteria,
        *opportunity.refutation_criteria,
        opportunity.decision_unlocked,
        *opportunity.missing_evidence,
        opportunity.limitation,
    ))


def _impact_is_traceable(
    opportunity: InvestigationOpportunity, context: IntelligenceContext
) -> bool:
    if opportunity.impact_proxy_value is None:
        return True
    known = context.evidence_by_id
    support = [known[ref] for ref in opportunity.supporting_evidence_refs if ref in known]
    target = float(opportunity.impact_proxy_value)
    for item in support:
        for value in item.facts.values():
            if (
                not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isclose(float(value), target, rel_tol=1e-9, abs_tol=1e-9)
            ):
                return True
    if opportunity.impact_proxy_name == "campaign_spend_share":
        campaign_items = [item for item in support if item.evidence_type == "CAMPAIGN_DIAGNOSTIC"]
        total = sum((_number(item, "spend_usd") or 0) for item in campaign_items)
        scoped = next((item for item in campaign_items if item.scope_name == opportunity.scope_name), None)
        if scoped is not None and total > 0:
            return math.isclose((_number(scoped, "spend_usd") or 0) / total, target, rel_tol=1e-9)
    return False


def validate_opportunity(
    opportunity: InvestigationOpportunity,
    context: IntelligenceContext,
    *, require_final_order: bool = True,
) -> InvestigationOpportunity:
    """Fail closed unless identity, evidence, language, and impact are supported."""
    if not isinstance(opportunity, InvestigationOpportunity):
        raise OpportunityValidationError("Opportunity must use the strict model")
    if require_final_order and opportunity.opportunity_order < 1:
        raise OpportunityValidationError("Final opportunity order must be positive")
    if opportunity.business_id != context.business_id or opportunity.as_of_date != context.as_of_date:
        raise OpportunityValidationError("Opportunity crosses context identity or date")
    refs = (
        *opportunity.supporting_evidence_refs,
        *opportunity.counter_evidence_refs,
        *opportunity.blocking_evidence_refs,
    )
    unknown = set(refs) - context.evidence_by_id.keys()
    if unknown:
        raise OpportunityValidationError("Opportunity references unknown evidence")
    if any(context.evidence_by_id[ref].business_id != context.business_id for ref in refs):
        raise OpportunityValidationError("Opportunity evidence crosses business boundaries")
    if not _LIMITATION.search(opportunity.limitation):
        raise OpportunityValidationError("Opportunity limitation must preserve hypothesis uncertainty")
    generated = _opportunity_text(opportunity)
    if _EMAIL.search(generated) or _PHONE.search(generated) or _PII_TERMS.search(generated):
        raise OpportunityValidationError("Opportunity contains PII-shaped or customer-level content")
    if _has_unsupported_causal_wording(generated) or _UNSAFE_HYPOTHESIS.search(generated):
        raise OpportunityValidationError("Opportunity contains unsupported causal wording")
    if _AUTONOMOUS_ASSERTION.search(generated) or any(
        _AUTONOMOUS_STEP.search(step) for step in opportunity.investigation_steps
    ):
        raise OpportunityValidationError("Opportunity contains an autonomous action")
    if context.economic_status.facts.get("economic_status") == "FX_REQUIRED":
        if _FINANCIAL_ASSERTION.search(generated):
            raise OpportunityValidationError("Opportunity violates the FX_REQUIRED boundary")
        forbidden_impact = (opportunity.impact_proxy_name or "").lower()
        if any(term in forbidden_impact for term in (
            "profit", "contribution", "margin", "mer", "roas", "revenue", "upside"
        )):
            raise OpportunityValidationError("Financial impact proxy is forbidden under FX_REQUIRED")
    if not _impact_is_traceable(opportunity, context):
        raise OpportunityValidationError("Opportunity impact proxy is not traceable to evidence")
    return opportunity


def validate_suppressed(
    suppressed: SuppressedOpportunity, context: IntelligenceContext
) -> SuppressedOpportunity:
    unknown = set(suppressed.evidence_refs) - context.evidence_by_id.keys()
    if unknown:
        raise OpportunityValidationError("Suppressed candidate references unknown evidence")
    generated = "\n".join((
        suppressed.scope_id or "", suppressed.scope_name, suppressed.explanation,
    ))
    if _EMAIL.search(generated) or _PHONE.search(generated) or _PII_TERMS.search(generated):
        raise OpportunityValidationError("Suppressed candidate contains PII-shaped content")
    return suppressed


def evaluate_opportunities(context: IntelligenceContext) -> OpportunityEvaluation:
    """Evaluate all registered rules and return stable ranked validated output."""
    if not isinstance(context, IntelligenceContext):
        raise OpportunityValidationError("A validated IntelligenceContext is required")
    candidates: list[OpportunityCandidate] = []
    suppressed: list[SuppressedOpportunity] = []
    for evaluator in (
        _candidate_acquisition_fulfillment,
        _candidate_confirmation,
        _candidate_measurement,
        _candidate_acquisition_efficiency,
    ):
        rule_candidates, rule_suppressed = evaluator(context)
        candidates.extend(rule_candidates)
        suppressed.extend(rule_suppressed)
    for candidate in candidates:
        validate_opportunity(candidate.opportunity, context, require_final_order=False)
    candidates.sort(key=lambda candidate: (
        _PRIORITY_ORDER[candidate.opportunity.priority],
        _RULE_ORDER[candidate.rule_id],
        -(candidate.opportunity.impact_proxy_value or 0),
        candidate.opportunity.scope_name.lower(),
        candidate.opportunity.opportunity_id,
    ))
    opportunities = tuple(
        validate_opportunity(
            replace(candidate.opportunity, opportunity_order=index), context
        )
        for index, candidate in enumerate(candidates, 1)
    )
    suppressed.sort(key=lambda item: (
        _RULE_ORDER[item.rule_id], item.scope_name.lower(), item.reason_code.value,
    ))
    validated_suppressed = tuple(validate_suppressed(item, context) for item in suppressed)
    return OpportunityEvaluation(
        business_id=context.business_id,
        as_of_date=context.as_of_date,
        opportunities=opportunities,
        suppressed_candidates=validated_suppressed,
    )
