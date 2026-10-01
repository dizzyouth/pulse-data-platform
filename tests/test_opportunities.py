"""Phase 6.7A deterministic cross-domain opportunity contracts."""

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timezone
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from src.intelligence.context import build_context
from src.intelligence.opportunities import (
    RULE_ACQUISITION_EFFICIENCY,
    RULE_ACQUISITION_FULFILLMENT,
    RULE_CONFIRMATION_LEAKAGE,
    RULE_MEASUREMENT_RECONCILIATION,
    RULE_REGISTRY,
    evaluate_opportunities,
    validate_opportunity,
)
from src.intelligence.opportunity_cli import main as opportunity_main
from src.intelligence.opportunity_models import (
    OpportunityPriority,
    OpportunityType,
    OpportunityValidationError,
    SuppressionReason,
    stable_opportunity_id,
)


BUSINESS = "synthetic_business"


def _signal(
    order: int,
    signal_type: str,
    *,
    scope_type: str = "BUSINESS",
    scope_name: str = BUSINESS,
    category: str,
    observed: float,
    baseline: float,
    sample: int,
    impact: float | None,
    confidence: str = "HIGH",
    priority: str = "MEDIUM",
) -> dict:
    return {
        "signal_order": order,
        "signal_id": f"{BUSINESS}|{scope_type}|{signal_type}|{scope_name}",
        "business_id": BUSINESS,
        "as_of_date": date(2026, 8, 31),
        "scope_type": scope_type,
        "scope_id": scope_name,
        "scope_name": scope_name,
        "signal_type": signal_type,
        "signal_category": category,
        "priority": priority,
        "confidence": confidence,
        "metric_name": signal_type.lower(),
        "observed_value": observed,
        "baseline_value": baseline,
        "absolute_gap": abs(observed - baseline),
        "relative_gap": abs(observed - baseline) / baseline if baseline else None,
        "sample_size": sample,
        "impact_order_count": impact,
        "evidence_summary": "Synthetic aggregate observation.",
        "why_it_matters": "The aggregate difference warrants investigation.",
        "recommended_next_step": "Review aggregate evidence.",
        "limitation": "Current aggregate evidence cannot establish cause.",
        "causal_claim": False,
    }


class SyntheticRepository:
    def __init__(
        self,
        *,
        campaign_name: str = "Orbit-Blue",
        campaign_spend: float = 700,
        peer_spend: float = 300,
        weak_delivery: bool = True,
        weak_returns: bool = True,
        fulfillment_band: str = "HIGH",
        acquisition_band: str = "HIGH",
        matched_gap: float = 100,
        matched_denominator: float = 500,
        measurement_gap: float = 100,
        measurement_denominator: float = 1000,
        cost: float = 15,
        peer_cost: float = 10,
        cost_signal: bool = True,
        strong_delivery: bool = False,
        strong_returns: bool = False,
        identity_rate: float = 0,
        missing_initial_currency: bool = False,
        anomaly_status: str = "NORMAL",
        delivery_impact: float = 80,
        return_impact: float = 60,
    ) -> None:
        self.campaign_name = campaign_name
        self.signals: list[dict] = []
        order = 1
        if weak_delivery:
            self.signals.append(_signal(
                order, "CAMPAIGN_DELIVERY_GAP", scope_type="CAMPAIGN",
                scope_name=campaign_name, category="FULFILLMENT", observed=0.30,
                baseline=0.55, sample=140, impact=delivery_impact,
                confidence=fulfillment_band,
            ))
            order += 1
        if weak_returns:
            self.signals.append(_signal(
                order, "CAMPAIGN_RETURN_PRESSURE", scope_type="CAMPAIGN",
                scope_name=campaign_name, category="FULFILLMENT", observed=0.50,
                baseline=0.25, sample=140, impact=return_impact,
                confidence=fulfillment_band,
            ))
            order += 1
        confirmation_rate = matched_gap / matched_denominator if matched_denominator else 0
        if matched_gap > 0:
            self.signals.append(_signal(
                order, "CONFIRMATION_LEAKAGE", category="CONFIRMATION",
                observed=matched_gap, baseline=0, sample=int(matched_denominator),
                impact=matched_gap,
            ))
            order += 1
        if measurement_gap > 0:
            self.signals.append(_signal(
                order, "PLATFORM_OBSERVED_GAP", category="MEASUREMENT",
                observed=measurement_gap + measurement_denominator,
                baseline=measurement_denominator, sample=int(measurement_denominator),
                impact=measurement_gap, priority="LOW",
            ))
            order += 1
        if missing_initial_currency:
            self.signals.append(_signal(
                order, "MISSING_INITIAL_CURRENCY", category="ECONOMICS",
                observed=1, baseline=0, sample=1, impact=None, priority="LOW",
            ))
            order += 1
        if cost_signal:
            self.signals.append(_signal(
                order, "CAMPAIGN_ACQUISITION_COST_GAP", scope_type="CAMPAIGN",
                scope_name=campaign_name, category="ACQUISITION", observed=cost,
                baseline=peer_cost, sample=160, impact=None,
                confidence=acquisition_band, priority="LOW",
            ))

        delivery_rate = 0.70 if strong_delivery else 0.30
        peer_delivery_rate = 0.50 if strong_delivery else 0.55
        return_rate = 0.10 if strong_returns else 0.50
        peer_return_rate = 0.30 if strong_returns else 0.25
        self.campaigns = [
            self._campaign(
                campaign_name, campaign_spend, delivery_rate, peer_delivery_rate,
                return_rate, peer_return_rate, cost, peer_cost,
                fulfillment_band, acquisition_band, delivery_impact, return_impact,
            ),
            self._campaign(
                "Peer-Green", peer_spend, 0.55, 0.30, 0.25, 0.50,
                9, 16, "HIGH", "HIGH", 0, 0,
            ),
        ]
        self.leakage = [
            self._leakage(
                "PLATFORM_VS_OBSERVED", measurement_gap, measurement_denominator,
                True, False, 1,
            ),
            self._leakage(
                "IDENTITY_UNRESOLVED", identity_rate * 1000, 1000,
                False, False, 2,
            ),
            self._leakage(
                "MATCHED_NOT_CONFIRMED", matched_gap, matched_denominator,
                False, True, 3,
            ),
        ]
        self.anomalies = [
            {
                "metric_name": metric,
                "status": anomaly_status,
                "severity": "INFO",
                "observed_at_utc": datetime(2026, 8, 31, tzinfo=timezone.utc),
                "observed_value": 10.0,
                "expected_value": 10.0,
                "lower_bound": 5.0,
                "upper_bound": 15.0,
                "baseline_strategy": "robust_history",
                "confidence": "HIGH",
                "explanation": "Synthetic aggregate anomaly state.",
            }
            for metric in (
                "daily_target_spend", "lightfunnels_order_volume",
                "confirmed_order_volume", "delivered_order_volume",
                "returned_order_volume",
            )
        ]
        self.confirmation_rate = confirmation_rate

    @staticmethod
    def _campaign(
        name, spend, delivery, peer_delivery, returns, peer_returns,
        cost, peer_cost, fulfillment_band, acquisition_band,
        delivery_impact, return_impact,
    ) -> dict:
        return {
            "business_id": BUSINESS,
            "campaign_name": name,
            "spend_usd": spend,
            "lightfunnels_orders": 160,
            "matched_orders": 150,
            "confirmed_orders": 120,
            "shipped_orders": 140,
            "delivered_orders": 70,
            "returned_orders": 50,
            "confirmation_rate": 0.80,
            "peer_confirmation_rate": 0.82,
            "delivery_rate": delivery,
            "peer_delivery_rate": peer_delivery,
            "return_rate": returns,
            "peer_return_rate": peer_returns,
            "cost_per_lightfunnels_order_usd": cost,
            "peer_cost_per_lightfunnels_order_usd": peer_cost,
            "cost_per_delivered_order_usd": 30,
            "peer_cost_per_delivered_order_usd": 20,
            "delivery_benchmark_gap_orders": delivery_impact,
            "excess_returns_vs_peer": return_impact,
            "confirmation_sample_band": "HIGH",
            "fulfillment_sample_band": fulfillment_band,
            "acquisition_sample_band": acquisition_band,
            "cost_delivered_sample_band": "HIGH",
            "sample_band": fulfillment_band,
            "benchmark_interpretation": "Synthetic observational peer benchmark.",
        }

    @staticmethod
    def _leakage(stage, observed, denominator, measurement, operational, order) -> dict:
        return {
            "business_id": BUSINESS,
            "as_of_date": date(2026, 8, 31),
            "leakage_stage": stage,
            "category": "MEASUREMENT" if measurement else "CONFIRMATION",
            "observed_count": observed,
            "denominator_count": denominator,
            "observed_rate": observed / denominator if denominator else 0,
            "interpretation": "Synthetic aggregate stage difference.",
            "is_measurement_gap": measurement,
            "is_operational_gap": operational,
            "stage_order": order,
        }

    def fetch_as_of(self, business_id):
        return {"business_id": business_id, "as_of_date": date(2026, 8, 31)}

    def fetch_signals(self, business_id):
        return self.signals

    def fetch_leakage(self, business_id):
        return self.leakage

    def fetch_campaigns(self, business_id):
        return self.campaigns

    def fetch_anomalies(self, business_id):
        return self.anomalies

    def fetch_economics(self, business_id):
        return [{"business_id": business_id, "economic_status": "FX_REQUIRED", "currency": "SAR"}]


def synthetic_context(**changes):
    return build_context(BUSINESS, SyntheticRepository(**changes))


def with_confirmation_identity_quality(context, quality: str):
    evidence_id = "leakage:matched_not_confirmed"
    original = context.evidence_by_id[evidence_id]
    changed = replace(
        original,
        facts={**original.facts, "denominator_identity_quality": quality},
    )
    return replace(
        context,
        leakage=tuple(changed if item.evidence_id == evidence_id else item
                      for item in context.leakage),
        evidence_items=tuple(changed if item.evidence_id == evidence_id else item
                             for item in context.evidence_items),
    )


def opportunity(evaluation, kind: OpportunityType):
    return next(item for item in evaluation.opportunities if item.opportunity_type is kind)


def suppressed(evaluation, rule_id: str, scope_name: str | None = None):
    return [
        item for item in evaluation.suppressed_candidates
        if item.rule_id == rule_id and (scope_name is None or item.scope_name == scope_name)
    ]


class OpportunityModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.result = evaluate_opportunities(self.context)
        self.item = self.result.opportunities[0]

    def test_stable_id_is_normalized_and_repeatable(self) -> None:
        first = stable_opportunity_id(
            OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
            "CAMPAIGN",
            "Orbit Blue / Test",
        )
        second = stable_opportunity_id(
            OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT,
            "CAMPAIGN",
            "Orbit Blue / Test",
        )
        self.assertEqual(first, second)
        self.assertEqual(
            first,
            "opportunity:acquisition_fulfillment_misalignment:orbit-blue-test",
        )
        self.assertEqual(
            stable_opportunity_id(
                OpportunityType.CONFIRMATION_LEAKAGE, "BUSINESS", BUSINESS
            ),
            "opportunity:confirmation_leakage:synthetic_business",
        )

    def test_model_is_immutable_and_typed(self) -> None:
        with self.assertRaises(FrozenInstanceError):
            self.item.priority = OpportunityPriority.LOW
        with self.assertRaises(OpportunityValidationError):
            replace(self.item, priority="URGENT")

    def test_causal_claim_cannot_be_true(self) -> None:
        with self.assertRaisesRegex(OpportunityValidationError, "causal"):
            replace(self.item, causal_claim=True)

    def test_rule_registry_has_stable_explicit_ids(self) -> None:
        self.assertEqual(
            [rule.rule_id for rule in RULE_REGISTRY],
            [
                RULE_ACQUISITION_FULFILLMENT,
                RULE_CONFIRMATION_LEAKAGE,
                RULE_MEASUREMENT_RECONCILIATION,
                RULE_ACQUISITION_EFFICIENCY,
            ],
        )
        self.assertTrue(all(rule.eligibility and rule.priority_logic for rule in RULE_REGISTRY))


class CrossDomainRuleTests(unittest.TestCase):
    def test_material_spend_and_weak_fulfillment_create_high_opportunity(self) -> None:
        result = evaluate_opportunities(synthetic_context())
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertEqual(item.scope_name, "Orbit-Blue")
        self.assertEqual(item.priority.value, "HIGH")
        self.assertEqual(item.confidence.value, "HIGH")
        self.assertIn("70.00%", item.observation_summary)
        self.assertIn("signal:campaign_delivery_gap:orbit-blue", item.supporting_evidence_refs)
        self.assertIn("signal:campaign_return_pressure:orbit-blue", item.supporting_evidence_refs)

    def test_strong_fulfillment_creates_no_false_candidate(self) -> None:
        result = evaluate_opportunities(synthetic_context(
            weak_delivery=False, weak_returns=False, cost_signal=False,
            strong_delivery=True, strong_returns=True,
        ))
        self.assertFalse(any(
            item.opportunity_type is OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT
            for item in result.opportunities
        ))
        self.assertEqual(
            suppressed(result, RULE_ACQUISITION_FULFILLMENT, "Orbit-Blue")[0].reason_code,
            SuppressionReason.CONTRADICTORY_EVIDENCE,
        )

    def test_insignificant_spend_is_suppressed(self) -> None:
        result = evaluate_opportunities(synthetic_context(campaign_spend=50, peer_spend=950))
        item = suppressed(result, RULE_ACQUISITION_FULFILLMENT, "Orbit-Blue")[0]
        self.assertEqual(item.reason_code, SuppressionReason.INSUFFICIENT_MATERIALITY)

    def test_low_fulfillment_sample_is_suppressed(self) -> None:
        result = evaluate_opportunities(synthetic_context(fulfillment_band="LOW"))
        item = suppressed(result, RULE_ACQUISITION_FULFILLMENT, "Orbit-Blue")[0]
        self.assertEqual(item.reason_code, SuppressionReason.INSUFFICIENT_SAMPLE)

    def test_alternate_campaign_name_and_values_drive_output(self) -> None:
        result = evaluate_opportunities(synthetic_context(
            campaign_name="Nova-Delta", delivery_impact=23, return_impact=17,
        ))
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertEqual(item.scope_name, "Nova-Delta")
        self.assertEqual(
            item.opportunity_id,
            "opportunity:acquisition_fulfillment_misalignment:nova-delta",
        )
        self.assertEqual(item.impact_proxy_value, 23)

    def test_spend_share_is_calculated_from_supplied_campaign_evidence(self) -> None:
        result = evaluate_opportunities(synthetic_context(
            campaign_spend=400, peer_spend=600,
        ))
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertIn("40.00%", item.observation_summary)

    def test_confirmation_leakage_is_active_when_material(self) -> None:
        result = evaluate_opportunities(synthetic_context())
        item = opportunity(result, OpportunityType.CONFIRMATION_LEAKAGE)
        self.assertEqual(item.impact_proxy_value, 100)
        self.assertEqual(item.priority.value, "HIGH")
        self.assertIn("100 of 500", item.observation_summary)
        self.assertIn("20.00%", item.observation_summary)
        self.assertIn("leakage:matched_not_confirmed", item.supporting_evidence_refs)

    def test_tiny_confirmation_gap_is_suppressed(self) -> None:
        result = evaluate_opportunities(synthetic_context(matched_gap=2))
        item = suppressed(result, RULE_CONFIRMATION_LEAKAGE)[0]
        self.assertEqual(item.reason_code, SuppressionReason.INSUFFICIENT_MATERIALITY)

    def test_measurement_gap_is_low_priority_and_not_operational_loss(self) -> None:
        result = evaluate_opportunities(synthetic_context())
        item = opportunity(result, OpportunityType.MEASUREMENT_RECONCILIATION)
        self.assertEqual(item.priority.value, "LOW")
        rendered = json.dumps(item.to_dict()).lower()
        self.assertIn("measurement", rendered)
        self.assertIn("does not establish lost orders", rendered)
        self.assertNotIn("operational opportunity", rendered)

    def test_sufficient_acquisition_sample_and_cost_gap_create_review(self) -> None:
        result = evaluate_opportunities(synthetic_context())
        item = opportunity(result, OpportunityType.ACQUISITION_EFFICIENCY_REVIEW)
        self.assertEqual(item.scope_name, "Orbit-Blue")
        self.assertEqual(item.priority.value, "MEDIUM")
        self.assertEqual(item.impact_proxy_unit, "USD_PER_OBSERVED_ORDER")

    def test_low_acquisition_sample_is_suppressed(self) -> None:
        result = evaluate_opportunities(synthetic_context(acquisition_band="LOW"))
        item = suppressed(result, RULE_ACQUISITION_EFFICIENCY, "Orbit-Blue")[0]
        self.assertEqual(item.reason_code, SuppressionReason.INSUFFICIENT_SAMPLE)

    def test_one_strong_downstream_outcome_is_counter_evidence_and_downranks(self) -> None:
        result = evaluate_opportunities(synthetic_context(
            weak_delivery=False, weak_returns=False, strong_delivery=True,
        ))
        item = opportunity(result, OpportunityType.ACQUISITION_EFFICIENCY_REVIEW)
        self.assertEqual(item.priority.value, "LOW")
        self.assertEqual(item.confidence.value, "MEDIUM")
        self.assertEqual(item.counter_evidence_refs, ("campaign:orbit-blue",))

    def test_two_strong_downstream_outcomes_suppress_cost_candidate(self) -> None:
        result = evaluate_opportunities(synthetic_context(
            weak_delivery=False, weak_returns=False,
            strong_delivery=True, strong_returns=True,
        ))
        item = suppressed(result, RULE_ACQUISITION_EFFICIENCY, "Orbit-Blue")[0]
        self.assertEqual(item.reason_code, SuppressionReason.CONTRADICTORY_EVIDENCE)

    def test_normal_anomaly_does_not_remove_structural_opportunity(self) -> None:
        result = evaluate_opportunities(synthetic_context(anomaly_status="NORMAL"))
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertIn("structural peer pattern", item.observation_summary)
        self.assertIn("anomaly:delivered_order_volume", item.supporting_evidence_refs)

    def test_identity_blocker_reduces_priority_and_confidence(self) -> None:
        result = evaluate_opportunities(synthetic_context(identity_rate=0.20))
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertEqual((item.priority.value, item.confidence.value), ("MEDIUM", "MEDIUM"))
        self.assertIn("leakage:identity_unresolved", item.blocking_evidence_refs)
        self.assertIn("limits coverage and representativeness", item.limitation)

    def test_fulfillment_without_identity_limitation_is_not_downgraded(self) -> None:
        result = evaluate_opportunities(synthetic_context(identity_rate=0))
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertEqual((item.priority.value, item.confidence.value), ("HIGH", "HIGH"))
        self.assertNotIn("leakage:identity_unresolved", item.blocking_evidence_refs)

    def test_resolved_confirmation_cohort_keeps_identity_gap_contextual(self) -> None:
        result = evaluate_opportunities(synthetic_context(identity_rate=0.20))
        item = opportunity(result, OpportunityType.CONFIRMATION_LEAKAGE)
        self.assertEqual((item.priority.value, item.confidence.value), ("HIGH", "HIGH"))
        self.assertIn("leakage:identity_unresolved", item.blocking_evidence_refs)
        self.assertIn("resolved high-confidence cohort", item.limitation)
        self.assertIn("limit coverage beyond that cohort", item.limitation)

    def test_uncertain_confirmation_denominator_can_be_downgraded(self) -> None:
        context = with_confirmation_identity_quality(
            synthetic_context(identity_rate=0.20), "UNCERTAIN"
        )
        result = evaluate_opportunities(context)
        item = opportunity(result, OpportunityType.CONFIRMATION_LEAKAGE)
        self.assertEqual((item.priority.value, item.confidence.value), ("MEDIUM", "MEDIUM"))
        self.assertIn("leakage:identity_unresolved", item.blocking_evidence_refs)
        self.assertIn("within the analyzed denominator", item.limitation)

    def test_fx_required_does_not_downrank_non_economic_opportunities(self) -> None:
        result = evaluate_opportunities(synthetic_context())
        items = {
            item.opportunity_type: item for item in result.opportunities
        }
        self.assertEqual(
            (items[OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT].priority.value,
             items[OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT].confidence.value),
            ("HIGH", "HIGH"),
        )
        self.assertEqual(
            (items[OpportunityType.CONFIRMATION_LEAKAGE].priority.value,
             items[OpportunityType.CONFIRMATION_LEAKAGE].confidence.value),
            ("HIGH", "HIGH"),
        )
        self.assertTrue(all(
            "economics:fx_required" not in item.blocking_evidence_refs
            for item in result.opportunities
        ))

    def test_missing_currency_does_not_downrank_non_economic_opportunities(self) -> None:
        result = evaluate_opportunities(synthetic_context(missing_initial_currency=True))
        fulfillment = opportunity(
            result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT
        )
        confirmation = opportunity(result, OpportunityType.CONFIRMATION_LEAKAGE)
        self.assertEqual((fulfillment.priority.value, fulfillment.confidence.value), ("HIGH", "HIGH"))
        self.assertEqual((confirmation.priority.value, confirmation.confidence.value), ("HIGH", "HIGH"))
        self.assertTrue(all(
            "signal:missing_initial_currency" not in item.blocking_evidence_refs
            for item in result.opportunities
        ))

    def test_platform_gap_does_not_block_fulfillment_opportunity(self) -> None:
        result = evaluate_opportunities(synthetic_context(measurement_gap=100))
        item = opportunity(result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertEqual((item.priority.value, item.confidence.value), ("HIGH", "HIGH"))
        self.assertNotIn("leakage:platform_vs_observed", item.blocking_evidence_refs)
        self.assertNotIn("signal:platform_observed_gap", item.blocking_evidence_refs)

    def test_measurement_gap_is_direct_support_for_reconciliation(self) -> None:
        result = evaluate_opportunities(synthetic_context(measurement_gap=100))
        item = opportunity(result, OpportunityType.MEASUREMENT_RECONCILIATION)
        self.assertIn("leakage:platform_vs_observed", item.supporting_evidence_refs)
        self.assertIn("signal:platform_observed_gap", item.supporting_evidence_refs)
        self.assertEqual(item.blocking_evidence_refs, ())

    def test_unrelated_blockers_are_not_attached_merely_because_they_exist(self) -> None:
        result = evaluate_opportunities(synthetic_context(
            identity_rate=0.20, missing_initial_currency=True,
        ))
        confirmation = opportunity(result, OpportunityType.CONFIRMATION_LEAKAGE)
        fulfillment = opportunity(
            result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT
        )
        efficiency = opportunity(result, OpportunityType.ACQUISITION_EFFICIENCY_REVIEW)
        self.assertEqual(confirmation.blocking_evidence_refs, ("leakage:identity_unresolved",))
        self.assertEqual(fulfillment.blocking_evidence_refs, ("leakage:identity_unresolved",))
        self.assertEqual(efficiency.blocking_evidence_refs, ())


class OpportunitySafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.result = evaluate_opportunities(self.context)
        self.item = opportunity(
            self.result, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT
        )

    def test_all_evidence_refs_resolve_and_buckets_are_disjoint(self) -> None:
        known = self.context.evidence_by_id
        for item in self.result.opportunities:
            buckets = (
                set(item.supporting_evidence_refs), set(item.counter_evidence_refs),
                set(item.blocking_evidence_refs),
            )
            self.assertTrue(set.union(*buckets) <= known.keys())
            self.assertFalse(buckets[0] & buckets[1])
            self.assertFalse(buckets[0] & buckets[2])
            self.assertFalse(buckets[1] & buckets[2])

    def test_unknown_reference_is_rejected(self) -> None:
        changed = replace(self.item, supporting_evidence_refs=("signal:unknown",))
        with self.assertRaisesRegex(OpportunityValidationError, "unknown"):
            validate_opportunity(changed, self.context)

    def test_duplicate_and_empty_support_are_rejected(self) -> None:
        ref = self.item.supporting_evidence_refs[0]
        with self.assertRaisesRegex(OpportunityValidationError, "duplicates"):
            replace(self.item, supporting_evidence_refs=(ref, ref))
        with self.assertRaisesRegex(OpportunityValidationError, "cannot be empty"):
            replace(self.item, supporting_evidence_refs=())
        with self.assertRaisesRegex(OpportunityValidationError, "disjoint"):
            replace(self.item, counter_evidence_refs=(ref,))

    def test_impact_proxy_must_be_traceable(self) -> None:
        changed = replace(self.item, impact_proxy_value=987654321.0)
        with self.assertRaisesRegex(OpportunityValidationError, "traceable"):
            validate_opportunity(changed, self.context)

    def test_testable_possibility_language_is_allowed(self) -> None:
        self.assertIn("may be misaligned", self.item.hypothesis_to_test)
        self.assertIs(validate_opportunity(self.item, self.context), self.item)

    def test_affirmative_causal_language_is_rejected(self) -> None:
        unsafe = (
            "Targeting caused the observed returns.",
            "The creative is attracting bad customers.",
            "Fulfillment is the reason returns are high.",
        )
        for statement in unsafe:
            with self.subTest(statement=statement):
                changed = replace(self.item, hypothesis_to_test=statement)
                with self.assertRaisesRegex(OpportunityValidationError, "causal wording"):
                    validate_opportunity(changed, self.context)

    def test_autonomous_action_is_rejected(self) -> None:
        changed = replace(self.item, investigation_steps=("Pause Orbit-Blue.",))
        with self.assertRaisesRegex(OpportunityValidationError, "autonomous"):
            validate_opportunity(changed, self.context)

    def test_pii_shaped_content_is_rejected(self) -> None:
        changed = replace(self.item, missing_evidence=("Contact owner@example.com.",))
        with self.assertRaisesRegex(OpportunityValidationError, "PII"):
            validate_opportunity(changed, self.context)
        with self.assertRaisesRegex(OpportunityValidationError, "PII"):
            evaluate_opportunities(synthetic_context(campaign_name="owner@example.com"))

    def test_fx_required_blocks_financial_claim_and_valuation(self) -> None:
        serialized = json.dumps(self.result.to_dict(include_suppressed=True)).lower()
        for forbidden in ("recovered revenue", "revenue upside", "profit is", "roas is"):
            self.assertNotIn(forbidden, serialized)
        self.assertTrue(all(
            item.impact_proxy_unit in {
                "ORDERS", "OBSERVATIONS", "USD_PER_OBSERVED_ORDER"
            }
            for item in self.result.opportunities
        ))
        changed = replace(self.item, observation_summary="Profit is $10.")
        with self.assertRaisesRegex(OpportunityValidationError, "FX_REQUIRED"):
            validate_opportunity(changed, self.context)
        changed = replace(self.item, impact_proxy_name="revenue_upside")
        with self.assertRaisesRegex(OpportunityValidationError, "Financial impact"):
            validate_opportunity(changed, self.context)


class DeterminismAndCliTests(unittest.TestCase):
    def test_same_context_produces_byte_stable_json_and_order(self) -> None:
        first = evaluate_opportunities(synthetic_context())
        second = evaluate_opportunities(synthetic_context())
        first_json = json.dumps(
            first.to_dict(include_suppressed=True), sort_keys=True, separators=(",", ":")
        )
        second_json = json.dumps(
            second.to_dict(include_suppressed=True), sort_keys=True, separators=(",", ":")
        )
        self.assertEqual(first_json, second_json)
        self.assertEqual(
            [item.opportunity_order for item in first.opportunities],
            list(range(1, len(first.opportunities) + 1)),
        )

    def test_context_values_change_output_without_business_metric_constants(self) -> None:
        first = evaluate_opportunities(synthetic_context(delivery_impact=41))
        second = evaluate_opportunities(synthetic_context(delivery_impact=83))
        first_item = opportunity(first, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        second_item = opportunity(second, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        self.assertEqual(first_item.impact_proxy_value, 60)
        self.assertEqual(second_item.impact_proxy_value, 83)
        source = Path("src/intelligence/opportunities.py").read_text(encoding="utf-8")
        self.assertNotIn("Sama-NewUM", source)
        self.assertNotIn("sama_cod_pilot", source)
        for forbidden in (
            "5616.01", "6647.92", "59.79", "42.86", "40.03", "52.38", "97.71", "71.24",
        ):
            self.assertNotIn(forbidden, source)

    def test_cli_json_is_offline_aggregate_and_can_show_suppressed(self) -> None:
        output = io.StringIO()
        with patch(
            "src.intelligence.opportunity_cli.build_context",
            return_value=synthetic_context(),
        ), redirect_stdout(output):
            code = opportunity_main([
                "list", "--business-id", BUSINESS, "--format", "json",
                "--show-suppressed",
            ])
        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertTrue(payload["opportunities"])
        self.assertIn("suppressed_candidates", payload)
        rendered = output.getvalue().lower()
        for forbidden in ("customer_id", "phone_number", "tracking_number", "openai"):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()
