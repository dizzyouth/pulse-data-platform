"""Phase 6.7C deterministic investigation-planning contracts."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import FrozenInstanceError, replace
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from src.intelligence.context import EvidenceItem
from src.intelligence.investigation_cli import main as investigation_main
from src.intelligence.investigation_models import (
    EvidenceRequirement,
    EvidenceRequirementStatus,
    InvestigationReadiness,
    InvestigationValidationError,
    stable_gap_id,
    stable_plan_id,
    stable_task_id,
)
from src.intelligence.investigations import (
    build_investigation_plan,
    build_investigation_portfolio,
    derive_task_readiness,
    validate_investigation_plan,
)
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import OpportunityType
from tests.test_opportunities import BUSINESS, opportunity, synthetic_context


def _requirement(
    status: EvidenceRequirementStatus,
    *,
    evidence_refs: tuple[str, ...] = (),
) -> EvidenceRequirement:
    return EvidenceRequirement(
        requirement_id="requirement:test_evidence",
        name="Test aggregate evidence",
        description="Validated aggregate test evidence.",
        status=status,
        evidence_refs=evidence_refs,
        missing_reason=None if status is EvidenceRequirementStatus.AVAILABLE else "Unavailable.",
        collection_hint="Add validated aggregate evidence to the intelligence context.",
        source_scope="BUSINESS_AGGREGATE",
        required_for_task_ids=("investigation-task:test:scope:task",),
    )


def _add_dimension(context, dimension: str, *, scope_name: str):
    item = EvidenceItem(
        evidence_id=f"dimension:{dimension.lower()}:{scope_name.lower()}",
        evidence_type="TIME_ANOMALY",
        business_id=context.business_id,
        scope_type="CAMPAIGN" if scope_name != context.business_id else "BUSINESS",
        scope_name=scope_name,
        title=f"Validated {dimension}",
        facts={"evidence_dimension": dimension},
        limitation="Aggregate descriptive evidence only.",
        source_relation="monitoring_views.anomaly_baseline_history",
    )
    return replace(
        context,
        time_anomalies=(*context.time_anomalies, item),
        evidence_items=(*context.evidence_items, item),
    )


class InvestigationModelAndReadinessTests(unittest.TestCase):
    def test_models_are_immutable_and_ids_are_stable(self) -> None:
        requirement = _requirement(
            EvidenceRequirementStatus.AVAILABLE,
            evidence_refs=("signal:aggregate",),
        )
        with self.assertRaises(FrozenInstanceError):
            requirement.name = "changed"
        self.assertEqual(
            stable_task_id("TYPE A", "Orbit Blue", "Validate Pattern"),
            "investigation-task:type-a:orbit-blue:validate-pattern",
        )
        self.assertEqual(stable_plan_id("TYPE A", "Orbit Blue"), "investigation-plan:type-a:orbit-blue")
        self.assertEqual(stable_gap_id("requirement:offer_mix"), "evidence-gap:offer_mix")

    def test_strict_status_and_explicit_readiness_rules(self) -> None:
        available = _requirement(
            EvidenceRequirementStatus.AVAILABLE,
            evidence_refs=("signal:aggregate",),
        )
        partial = _requirement(
            EvidenceRequirementStatus.PARTIAL,
            evidence_refs=("signal:aggregate",),
        )
        missing = _requirement(EvidenceRequirementStatus.MISSING_FROM_CONTEXT)
        boundary = _requirement(
            EvidenceRequirementStatus.BLOCKED_BY_BOUNDARY,
            evidence_refs=("economics:status",),
        )
        self.assertIs(derive_task_readiness((available,)), InvestigationReadiness.READY_NOW)
        self.assertIs(derive_task_readiness((available, partial)), InvestigationReadiness.PARTIALLY_READY)
        self.assertIs(
            derive_task_readiness((available, missing)),
            InvestigationReadiness.BLOCKED_MISSING_EVIDENCE,
        )
        self.assertIs(
            derive_task_readiness((available, boundary)),
            InvestigationReadiness.BLOCKED_BOUNDARY,
        )
        with self.assertRaises(InvestigationValidationError):
            replace(available, status="UNKNOWN")


class InvestigationPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.portfolio = build_investigation_portfolio(self.context, self.evaluation)

    def _plan(self, kind: OpportunityType):
        item = opportunity(self.evaluation, kind)
        return next(plan for plan in self.portfolio.plans if plan.opportunity_id == item.opportunity_id)

    def test_confirmation_plan_has_ready_baseline_and_truthful_gaps(self) -> None:
        plan = self._plan(OpportunityType.CONFIRMATION_LEAKAGE)
        baseline = next(task for task in plan.tasks if "baseline" in task.task_id)
        self.assertIs(baseline.readiness, InvestigationReadiness.READY_NOW)
        self.assertIn("leakage:matched_not_confirmed", baseline.available_evidence_refs)
        statuses = {item.requirement_id: item.status for item in plan.evidence_requirements}
        self.assertIs(
            statuses["requirement:confirmation_dispositions"],
            EvidenceRequirementStatus.MISSING_FROM_CONTEXT,
        )
        self.assertIs(
            statuses["requirement:campaign_offer_mix"],
            EvidenceRequirementStatus.MISSING_FROM_CONTEXT,
        )

    def test_acquisition_fulfillment_plan_does_not_infer_finer_grain_evidence(self) -> None:
        plan = self._plan(OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        peer = next(task for task in plan.tasks if "validate-peer-pattern" in task.task_id)
        offer = next(task for task in plan.tasks if "compare-offer-mix" in task.task_id)
        adgroup = next(task for task in plan.tasks if "compare-adgroup" in task.task_id)
        self.assertIs(peer.readiness, InvestigationReadiness.READY_NOW)
        self.assertIs(offer.readiness, InvestigationReadiness.BLOCKED_MISSING_EVIDENCE)
        self.assertIs(adgroup.readiness, InvestigationReadiness.BLOCKED_MISSING_EVIDENCE)
        self.assertNotIn("campaign:orbit-blue", offer.available_evidence_refs)
        self.assertNotIn("campaign:orbit-blue", adgroup.available_evidence_refs)

    def test_identity_task_uses_opportunity_relevant_blocker_evidence(self) -> None:
        context = synthetic_context(identity_rate=0.20)
        evaluation = evaluate_opportunities(context)
        plan = build_investigation_plan(
            opportunity(evaluation, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT),
            context,
            evaluation,
        )
        task = next(item for item in plan.tasks if "assess-linkage" in item.task_id)
        self.assertIs(task.readiness, InvestigationReadiness.READY_NOW)
        self.assertEqual(task.available_evidence_refs, ("leakage:identity_unresolved",))

    def test_measurement_plan_is_bounded_and_semantics_are_missing(self) -> None:
        plan = self._plan(OpportunityType.MEASUREMENT_RECONCILIATION)
        current = next(task for task in plan.tasks if "validate-measurement" in task.task_id)
        self.assertIs(current.readiness, InvestigationReadiness.READY_NOW)
        missing = set(plan.unresolved_requirement_ids)
        self.assertIn("requirement:platform_event_definition", missing)
        self.assertIn("requirement:lightfunnels_inclusion_rules", missing)
        current_text = " ".join((current.objective, current.expected_output)).lower()
        self.assertNotIn("lost orders", current_text)
        self.assertNotIn("fraud", current_text)
        self.assertIn("does not establish lost orders", current.limitation.lower())

    def test_efficiency_plan_exists_only_for_active_opportunity(self) -> None:
        active = opportunity(self.evaluation, OpportunityType.ACQUISITION_EFFICIENCY_REVIEW)
        self.assertEqual(build_investigation_plan(active, self.context, self.evaluation).opportunity_id, active.opportunity_id)
        suppressed_context = synthetic_context(acquisition_band="LOW")
        suppressed_evaluation = evaluate_opportunities(suppressed_context)
        with self.assertRaisesRegex(InvestigationValidationError, "active opportunity"):
            build_investigation_plan(active, suppressed_context, suppressed_evaluation)

    def test_available_requirements_use_context_evidence_only(self) -> None:
        for plan in self.portfolio.plans:
            for requirement in plan.evidence_requirements:
                if requirement.status is EvidenceRequirementStatus.AVAILABLE:
                    self.assertTrue(requirement.evidence_refs)
                    for evidence_ref in requirement.evidence_refs:
                        self.assertIn(evidence_ref, self.context.evidence_by_id)
                        self.assertFalse(evidence_ref.startswith((
                            "opportunity:", "investigation-task:", "investigation-plan:"
                        )))

    def test_cross_business_evidence_is_rejected_fail_closed(self) -> None:
        context = synthetic_context()
        evaluation = evaluate_opportunities(context)
        plan = build_investigation_portfolio(context, evaluation).plans[0]
        opportunity_item = next(
            item for item in evaluation.opportunities
            if item.opportunity_id == plan.opportunity_id
        )
        evidence = context.evidence_by_id[plan.tasks[0].available_evidence_refs[0]]
        object.__setattr__(evidence, "business_id", "different_business")
        with self.assertRaisesRegex(ValueError, "crosses business"):
            validate_investigation_plan(plan, opportunity_item, context)

    def test_exact_dimension_can_make_offer_requirement_available(self) -> None:
        context = _add_dimension(
            synthetic_context(), "CAMPAIGN_OFFER_MIX", scope_name="Orbit-Blue"
        )
        evaluation = evaluate_opportunities(context)
        plan = build_investigation_plan(
            opportunity(evaluation, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT),
            context,
            evaluation,
        )
        requirement = next(
            item for item in plan.evidence_requirements
            if item.requirement_id == "requirement:campaign_offer_mix"
        )
        self.assertIs(requirement.status, EvidenceRequirementStatus.AVAILABLE)
        self.assertEqual(requirement.evidence_refs, ("dimension:campaign_offer_mix:orbit-blue",))

    def test_validator_rejects_unstable_ids_and_false_availability(self) -> None:
        opportunity_item = opportunity(
            self.evaluation, OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT
        )
        plan = self._plan(OpportunityType.ACQUISITION_FULFILLMENT_MISALIGNMENT)
        changed_task = replace(plan.tasks[0], task_id="investigation-task:other:scope:task")
        with self.assertRaises(InvestigationValidationError):
            validate_investigation_plan(replace(plan, tasks=(changed_task, *plan.tasks[1:])), opportunity_item, self.context)
        missing_index = next(
            index for index, item in enumerate(plan.evidence_requirements)
            if item.status is EvidenceRequirementStatus.MISSING_FROM_CONTEXT
        )
        false_available = replace(
            plan.evidence_requirements[missing_index],
            status=EvidenceRequirementStatus.AVAILABLE,
            evidence_refs=("campaign:orbit-blue",),
            missing_reason=None,
        )
        requirements = list(plan.evidence_requirements)
        requirements[missing_index] = false_available
        with self.assertRaisesRegex(InvestigationValidationError, "validated context evidence"):
            validate_investigation_plan(replace(plan, evidence_requirements=tuple(requirements)), opportunity_item, self.context)

    def test_validator_rejects_duplicate_order_readiness_and_recommendation(self) -> None:
        opportunity_item = opportunity(self.evaluation, OpportunityType.CONFIRMATION_LEAKAGE)
        plan = self._plan(OpportunityType.CONFIRMATION_LEAKAGE)
        invalid_plans = (
            replace(
                plan,
                tasks=(replace(plan.tasks[0], task_id=plan.tasks[1].task_id), *plan.tasks[1:]),
            ),
            replace(
                plan,
                tasks=(replace(plan.tasks[0], task_order=2), *plan.tasks[1:]),
            ),
            replace(
                plan,
                tasks=(
                    replace(
                        plan.tasks[0],
                        readiness=InvestigationReadiness.BLOCKED_MISSING_EVIDENCE,
                    ),
                    *plan.tasks[1:],
                ),
            ),
            replace(
                plan,
                recommended_start_task_id="investigation-task:other:scope:task",
            ),
        )
        for invalid in invalid_plans:
            with self.subTest(plan=invalid):
                with self.assertRaises(InvestigationValidationError):
                    validate_investigation_plan(invalid, opportunity_item, self.context)

    def test_validator_rejects_causal_autonomous_financial_and_pii_text(self) -> None:
        opportunity_item = opportunity(self.evaluation, OpportunityType.CONFIRMATION_LEAKAGE)
        plan = self._plan(OpportunityType.CONFIRMATION_LEAKAGE)
        bad_values = (
            "The reason is targeting.",
            "Pause campaign automatically.",
            "Automatically execute SQL and trigger ETL.",
            "Expected profit will increase.",
            "Contact customer" + "@" + "example.invalid.",
            "Inspect each lead ID in a raw row.",
        )
        for value in bad_values:
            with self.subTest(value=value):
                changed = replace(plan.tasks[0], objective=value)
                with self.assertRaises(InvestigationValidationError):
                    validate_investigation_plan(
                        replace(plan, tasks=(changed, *plan.tasks[1:])),
                        opportunity_item,
                        self.context,
                    )


class PortfolioAndCliTests(unittest.TestCase):
    def test_portfolio_orders_active_opportunities_and_recommends_first_ready(self) -> None:
        context = synthetic_context()
        evaluation = evaluate_opportunities(context)
        portfolio = build_investigation_portfolio(context, evaluation)
        self.assertEqual(
            [plan.opportunity_id for plan in portfolio.plans],
            [item.opportunity_id for item in evaluation.opportunities],
        )
        first_ready = next(
            task.task_id
            for plan in portfolio.plans
            for task in plan.tasks
            if task.readiness is InvestigationReadiness.READY_NOW
        )
        self.assertEqual(portfolio.recommended_start_task_id, first_ready)

    def test_shared_missing_gap_is_deduplicated_deterministically(self) -> None:
        portfolio = build_investigation_portfolio(synthetic_context())
        offer = next(
            gap for gap in portfolio.evidence_gaps
            if gap.requirement_id == "requirement:campaign_offer_mix"
        )
        self.assertEqual(len(offer.affected_opportunity_ids), 2)
        self.assertEqual(len(offer.affected_task_ids), 2)
        self.assertEqual(offer.gap_id, "evidence-gap:campaign_offer_mix")

    def test_missing_evidence_wording_does_not_invent_a_source(self) -> None:
        portfolio = build_investigation_portfolio(synthetic_context())
        forbidden = ("api endpoint", "connector", "table", "file", "data owner")
        for plan in portfolio.plans:
            for requirement in plan.evidence_requirements:
                if requirement.status is EvidenceRequirementStatus.MISSING_FROM_CONTEXT:
                    self.assertEqual(
                        requirement.missing_reason,
                        "Not available in the current validated intelligence context.",
                    )
                    text = f"{requirement.missing_reason} {requirement.collection_hint}".lower()
                    self.assertFalse(any(item in text for item in forbidden))

    def test_same_context_produces_byte_stable_json(self) -> None:
        first = build_investigation_portfolio(synthetic_context())
        second = build_investigation_portfolio(synthetic_context())
        encode = lambda value: json.dumps(value.to_dict(), sort_keys=True, separators=(",", ":"))
        self.assertEqual(encode(first), encode(second))

    def test_alternate_names_work_without_business_specific_engine_constants(self) -> None:
        context = synthetic_context(campaign_name="Nova-Alternate")
        portfolio = build_investigation_portfolio(context)
        self.assertTrue(any("nova-alternate" in plan.plan_id for plan in portfolio.plans))
        source = Path("src/intelligence/investigations.py").read_text(encoding="utf-8").lower()
        self.assertNotIn("sama", source)
        self.assertNotIn("orbit-blue", source)
        self.assertNotIn("synthetic_business", source)

    def test_cli_json_is_offline_aggregate_and_filters_active_opportunity(self) -> None:
        context = synthetic_context()
        active = evaluate_opportunities(context).opportunities[0]
        output = io.StringIO()
        with patch(
            "src.intelligence.investigation_cli.build_context", return_value=context
        ), redirect_stdout(output):
            code = investigation_main([
                "plan", "--business-id", BUSINESS, "--format", "json",
                "--opportunity-id", active.opportunity_id,
            ])
        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(len(payload["plans"]), 1)
        self.assertEqual(payload["plans"][0]["opportunity_id"], active.opportunity_id)
        rendered = output.getvalue().lower()
        for forbidden in ("customer_id", "phone_number", "tracking_number", "openai"):
            self.assertNotIn(forbidden, rendered)

    def test_cli_rejects_unknown_or_suppressed_opportunity(self) -> None:
        error = io.StringIO()
        with patch(
            "src.intelligence.investigation_cli.build_context",
            return_value=synthetic_context(),
        ), redirect_stderr(error):
            code = investigation_main([
                "plan", "--business-id", BUSINESS,
                "--opportunity-id", "opportunity:unknown:scope",
            ])
        self.assertEqual(code, 2)
        self.assertIn("not active", error.getvalue())


if __name__ == "__main__":
    unittest.main()
