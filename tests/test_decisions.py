"""Phase 6.8A deterministic decision-readiness contracts."""

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError, replace
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from src.intelligence.context import EvidenceItem
from src.intelligence.decision_cli import main as decision_main
from src.intelligence.decision_models import (
    DecisionClass,
    DecisionReadiness,
    DecisionType,
    DecisionValidationError,
    ReadinessReasonCode,
    stable_decision_id,
)
from src.intelligence.decisions import (
    DECISION_FRAME_REGISTRY,
    FINANCIAL_BOUNDARY_FRAME,
    assess_decision_frame,
    build_decision_readiness_portfolio,
    validate_decision_assessment,
    validate_decision_readiness_portfolio,
)
from src.intelligence.investigation_models import (
    EvidenceRequirement,
    EvidenceRequirementStatus,
    InvestigationReadiness,
    InvestigationTask,
    InvestigationTaskKind,
)
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import OpportunityType
from tests.test_opportunities import BUSINESS, opportunity, synthetic_context


def _add_dimension(context, evidence_id: str, dimension: str, scope_name: str):
    item = EvidenceItem(
        evidence_id=evidence_id,
        evidence_type="AGGREGATE_DOCUMENTATION",
        business_id=context.business_id,
        scope_type="BUSINESS" if scope_name == context.business_id else "CAMPAIGN",
        scope_name=scope_name,
        title=f"Synthetic {dimension.lower()}",
        facts={"evidence_dimension": dimension},
        limitation="Synthetic aggregate evidence cannot establish cause.",
        source_relation="marts.sama_pilot_business_leakage",
    )
    return replace(
        context,
        leakage=(*context.leakage, item),
        evidence_items=(*context.evidence_items, item),
    )


def _pipeline(context):
    evaluation = evaluate_opportunities(context)
    investigations = build_investigation_portfolio(context, evaluation)
    decisions = build_decision_readiness_portfolio(
        context, evaluation, investigations
    )
    return evaluation, investigations, decisions


def _assessment(portfolio, decision_type: DecisionType):
    return next(
        item for item in portfolio.assessments if item.decision_type is decision_type
    )


class DecisionModelAndRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation, self.investigations, self.portfolio = _pipeline(self.context)
        self.item = self.portfolio.assessments[0]

    def test_models_are_immutable_strict_and_preserve_human_agency(self) -> None:
        with self.assertRaises(FrozenInstanceError):
            self.item.readiness = DecisionReadiness.READY_FOR_HUMAN_REVIEW
        with self.assertRaises(DecisionValidationError):
            replace(self.item, readiness="APPROVED")
        with self.assertRaises(DecisionValidationError):
            replace(self.item, human_review_required=False)
        with self.assertRaises(DecisionValidationError):
            replace(self.item, autonomous_action_allowed=True)
        self.assertTrue(all(
            item.human_review_required and not item.autonomous_action_allowed
            for item in self.portfolio.assessments
        ))

    def test_stable_ids_and_serialization_are_deterministic(self) -> None:
        expected = stable_decision_id(
            "CONFIRMATION_LEAKAGE", BUSINESS, "confirmation-investigation-review"
        )
        self.assertEqual(
            expected,
            "decision-readiness:confirmation_leakage:synthetic_business:confirmation-investigation-review",
        )
        second = _pipeline(synthetic_context())[2]
        encode = lambda value: json.dumps(
            value.to_dict(), sort_keys=True, separators=(",", ":")
        )
        self.assertEqual(encode(self.portfolio), encode(second))

    def test_registry_explicitly_defines_evidence_and_boundary_behavior(self) -> None:
        frames = [
            frame for values in DECISION_FRAME_REGISTRY.values() for frame in values
        ]
        self.assertEqual(len(frames), 8)
        self.assertTrue(all(
            frame.frame_id
            and frame.required_requirement_ids
            and frame.relevant_task_keys
            and frame.rationale_design
            and frame.decision_boundary
            and frame.limitation
            for frame in frames
        ))
        self.assertIs(
            FINANCIAL_BOUNDARY_FRAME.decision_class, DecisionClass.FINANCIAL_DECISION
        )

    def test_portfolio_order_is_priority_then_opportunity_then_frame(self) -> None:
        priority = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        opportunities = {
            item.opportunity_id: item for item in self.evaluation.opportunities
        }
        keys = [
            (
                priority[opportunities[item.originating_opportunity_id].priority.value],
                opportunities[item.originating_opportunity_id].opportunity_order,
                DECISION_FRAME_REGISTRY[
                    opportunities[item.originating_opportunity_id].opportunity_type
                ][0].frame_order
                if item.decision_type is DECISION_FRAME_REGISTRY[
                    opportunities[item.originating_opportunity_id].opportunity_type
                ][0].decision_type else 2,
            )
            for item in self.portfolio.assessments
        ]
        self.assertEqual(keys, sorted(keys))
        ready = next(
            item.decision_id for item in self.portfolio.assessments
            if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        )
        self.assertEqual(self.portfolio.first_reviewable_decision_id, ready)


class DecisionReadinessTests(unittest.TestCase):
    def test_all_hard_requirements_available_can_be_ready(self) -> None:
        context = synthetic_context()
        for evidence_id, dimension in (
            ("synthetic:platform-definition", "PLATFORM_EVENT_DEFINITION"),
            ("synthetic:observed-rules", "LIGHTFUNNELS_INCLUSION_RULES"),
        ):
            context = _add_dimension(context, evidence_id, dimension, BUSINESS)
        item = _assessment(_pipeline(context)[2], DecisionType.MEASUREMENT_USE_READINESS)
        self.assertIs(item.readiness, DecisionReadiness.READY_FOR_HUMAN_REVIEW)
        self.assertEqual(item.unresolved_requirement_ids, ())

    def test_high_priority_high_confidence_does_not_satisfy_hard_requirements(self) -> None:
        evaluation, _, portfolio = _pipeline(synthetic_context())
        source = opportunity(evaluation, OpportunityType.CONFIRMATION_LEAKAGE)
        self.assertEqual((source.priority.value, source.confidence.value), ("HIGH", "HIGH"))
        bounded = _assessment(
            portfolio, DecisionType.CONFIRMATION_INVESTIGATION_REVIEW
        )
        operational = _assessment(
            portfolio, DecisionType.CONFIRMATION_OPERATIONAL_CHANGE_CONSIDERATION
        )
        self.assertIs(
            bounded.readiness, DecisionReadiness.READY_FOR_HUMAN_REVIEW
        )
        self.assertIs(
            operational.readiness, DecisionReadiness.NEEDS_MORE_EVIDENCE
        )
        self.assertIn(
            ReadinessReasonCode.CAUSAL_UNCERTAINTY,
            operational.readiness_reason_codes,
        )

    def test_low_priority_measurement_decision_may_be_reviewable(self) -> None:
        evaluation, _, portfolio = _pipeline(synthetic_context())
        source = opportunity(evaluation, OpportunityType.MEASUREMENT_RECONCILIATION)
        item = _assessment(
            portfolio, DecisionType.MEASUREMENT_RECONCILIATION_REVIEW
        )
        self.assertEqual(source.priority.value, "LOW")
        self.assertIs(item.readiness, DecisionReadiness.READY_FOR_HUMAN_REVIEW)

    def test_contextual_identity_blocker_does_not_block_bounded_confirmation_review(self) -> None:
        _, _, portfolio = _pipeline(synthetic_context(identity_rate=0.20))
        item = _assessment(
            portfolio, DecisionType.CONFIRMATION_INVESTIGATION_REVIEW
        )
        self.assertTrue(item.blocking_evidence_refs)
        self.assertIn(
            ReadinessReasonCode.DATA_QUALITY_LIMITATION,
            item.readiness_reason_codes,
        )
        self.assertIs(item.readiness, DecisionReadiness.READY_FOR_HUMAN_REVIEW)

    def test_ready_now_task_is_not_treated_as_completed_output(self) -> None:
        _, investigations, portfolio = _pipeline(synthetic_context())
        plan = next(
            item for item in investigations.plans
            if item.opportunity_id.startswith("opportunity:confirmation_leakage:")
        )
        self.assertTrue(any(
            task.readiness is InvestigationReadiness.READY_NOW for task in plan.tasks
        ))
        item = _assessment(
            portfolio, DecisionType.CONFIRMATION_OPERATIONAL_CHANGE_CONSIDERATION
        )
        self.assertIs(item.readiness, DecisionReadiness.NEEDS_MORE_EVIDENCE)
        self.assertIn("requirement:confirmation_dispositions", item.unresolved_requirement_ids)
        self.assertTrue(item.next_evidence_task_ids)

    def test_counter_evidence_is_allowed_for_investigation_but_blocks_change(self) -> None:
        context = synthetic_context(
            weak_delivery=False,
            weak_returns=False,
            strong_delivery=True,
        )
        context = _add_dimension(
            context,
            "synthetic:below-campaign-acquisition",
            "BELOW_CAMPAIGN_ACQUISITION_COHORTS",
            "Orbit-Blue",
        )
        _, _, portfolio = _pipeline(context)
        review = _assessment(
            portfolio, DecisionType.ACQUISITION_EFFICIENCY_INVESTIGATION_REVIEW
        )
        change = _assessment(
            portfolio,
            DecisionType.ACQUISITION_EFFICIENCY_CAMPAIGN_CHANGE_CONSIDERATION,
        )
        self.assertTrue(review.counter_evidence_refs)
        self.assertIs(review.readiness, DecisionReadiness.READY_FOR_HUMAN_REVIEW)
        self.assertIs(change.readiness, DecisionReadiness.NEEDS_MORE_EVIDENCE)
        self.assertIn(
            ReadinessReasonCode.COUNTER_EVIDENCE_PRESENT,
            change.readiness_reason_codes,
        )

    def test_normal_anomaly_does_not_make_business_change_ready(self) -> None:
        _, _, portfolio = _pipeline(synthetic_context(anomaly_status="NORMAL"))
        item = _assessment(portfolio, DecisionType.CAMPAIGN_CHANGE_CONSIDERATION)
        self.assertIs(item.readiness, DecisionReadiness.NEEDS_MORE_EVIDENCE)
        self.assertTrue(any(ref.startswith("anomaly:") for ref in item.supporting_evidence_refs))

    def test_suppressed_opportunity_has_no_plan_or_decision(self) -> None:
        evaluation, investigations, portfolio = _pipeline(
            synthetic_context(matched_gap=2)
        )
        self.assertFalse(any(
            item.opportunity_type is OpportunityType.CONFIRMATION_LEAKAGE
            for item in evaluation.opportunities
        ))
        self.assertFalse(any(
            "confirmation_leakage" in item.opportunity_id
            for item in investigations.plans
        ))
        self.assertFalse(any(
            item.decision_type in {
                DecisionType.CONFIRMATION_INVESTIGATION_REVIEW,
                DecisionType.CONFIRMATION_OPERATIONAL_CHANGE_CONSIDERATION,
            }
            for item in portfolio.assessments
        ))

    def test_financial_frame_under_fx_required_is_blocked_not_missing(self) -> None:
        context = synthetic_context()
        evaluation = evaluate_opportunities(context)
        investigations = build_investigation_portfolio(context, evaluation)
        source = opportunity(evaluation, OpportunityType.ACQUISITION_EFFICIENCY_REVIEW)
        plan = next(item for item in investigations.plans if item.opportunity_id == source.opportunity_id)
        task_id = (
            "investigation-task:acquisition_efficiency_review:"
            "orbit-blue:validate-trusted-economics"
        )
        requirement = EvidenceRequirement(
            requirement_id="requirement:trusted_cross_currency_economics",
            name="Trusted cross-currency economics",
            description="Trusted economic evidence for a cross-currency financial frame.",
            status=EvidenceRequirementStatus.BLOCKED_BY_BOUNDARY,
            evidence_refs=(context.economic_status.evidence_id,),
            missing_reason="FX_REQUIRED prevents trusted cross-currency evaluation.",
            collection_hint="Add trusted cross-currency economics.",
            source_scope="BUSINESS_ECONOMICS",
            required_for_task_ids=(task_id,),
        )
        task = InvestigationTask(
            task_order=len(plan.tasks) + 1,
            task_id=task_id,
            opportunity_id=source.opportunity_id,
            task_kind=InvestigationTaskKind.COLLECT_MISSING_EVIDENCE,
            title="Validate trusted economics boundary",
            objective="Assess whether trusted cross-currency evidence is available.",
            readiness=InvestigationReadiness.BLOCKED_BOUNDARY,
            required_requirement_ids=(requirement.requirement_id,),
            available_evidence_refs=(context.economic_status.evidence_id,),
            missing_requirement_ids=(requirement.requirement_id,),
            expected_output="A bounded economic-status assessment.",
            strengthens_criteria=source.confirmation_criteria,
            weakens_criteria=source.refutation_criteria,
            completion_criteria=("Complete when trusted economics are validated.",),
            limitation="This task does not calculate a financial outcome.",
            autonomous_action=False,
        )
        synthetic_plan = replace(
            plan,
            tasks=(*plan.tasks, task),
            evidence_requirements=(*plan.evidence_requirements, requirement),
            unresolved_requirement_ids=(*plan.unresolved_requirement_ids, requirement.requirement_id),
        )
        item = assess_decision_frame(
            context, source, synthetic_plan, FINANCIAL_BOUNDARY_FRAME
        )
        self.assertIs(item.readiness, DecisionReadiness.BLOCKED_BY_BOUNDARY)
        self.assertIn(ReadinessReasonCode.ECONOMIC_BOUNDARY, item.readiness_reason_codes)
        self.assertIn("FX_REQUIRED", item.decision_boundary)


class DecisionTraceabilityAndSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation, self.investigations, self.portfolio = _pipeline(self.context)
        self.item = self.portfolio.assessments[0]
        self.opportunity = next(
            item for item in self.evaluation.opportunities
            if item.opportunity_id == self.item.originating_opportunity_id
        )
        self.plan = next(
            item for item in self.investigations.plans
            if item.plan_id == self.item.investigation_plan_id
        )
        self.frame = next(
            frame
            for frame in DECISION_FRAME_REGISTRY[self.opportunity.opportunity_type]
            if frame.decision_type is self.item.decision_type
        )

    def test_all_evidence_requirements_and_tasks_resolve_locally(self) -> None:
        known = self.context.evidence_by_id
        forbidden = (
            "opportunity:", "investigation-plan:", "investigation-task:",
            "requirement:", "evidence-gap:", "decision-readiness:",
        )
        for item in self.portfolio.assessments:
            refs = (
                *item.supporting_evidence_refs,
                *item.counter_evidence_refs,
                *item.blocking_evidence_refs,
            )
            self.assertTrue(refs)
            self.assertTrue(set(refs) <= known.keys())
            self.assertTrue(all(known[ref].business_id == BUSINESS for ref in refs))
            self.assertFalse(any(ref.startswith(forbidden) for ref in refs))
            plan = next(
                plan for plan in self.investigations.plans
                if plan.plan_id == item.investigation_plan_id
            )
            self.assertTrue(
                set(item.required_requirement_ids)
                <= {req.requirement_id for req in plan.evidence_requirements}
            )
            self.assertTrue(
                set(item.relevant_investigation_task_ids)
                <= {task.task_id for task in plan.tasks}
            )

    def test_validator_rejects_identity_evidence_and_readiness_tampering(self) -> None:
        invalid = (
            replace(self.item, originating_opportunity_id="opportunity:unknown:scope"),
            replace(self.item, investigation_plan_id="investigation-plan:unknown:scope"),
            replace(self.item, decision_id="decision-readiness:wrong:scope:frame"),
            replace(self.item, readiness=DecisionReadiness.READY_FOR_HUMAN_REVIEW),
            replace(self.item, supporting_evidence_refs=("opportunity:unknown:scope",)),
        )
        for changed in invalid:
            with self.subTest(changed=changed):
                with self.assertRaises(DecisionValidationError):
                    validate_decision_assessment(
                        changed,
                        self.context,
                        self.opportunity,
                        self.plan,
                        self.frame,
                    )

    def test_validator_rejects_duplicate_order_and_wrong_counts(self) -> None:
        duplicate = replace(
            self.portfolio,
            assessments=(
                self.portfolio.assessments[0],
                replace(self.portfolio.assessments[1], decision_order=1),
                *self.portfolio.assessments[2:],
            ),
        )
        with self.assertRaises(DecisionValidationError):
            validate_decision_readiness_portfolio(
                duplicate, self.context, self.evaluation, self.investigations
            )
        wrong_count = replace(
            self.portfolio,
            ready_for_human_review_count=self.portfolio.ready_for_human_review_count + 1,
        )
        with self.assertRaises(DecisionValidationError):
            validate_decision_readiness_portfolio(
                wrong_count, self.context, self.evaluation, self.investigations
            )

    def test_validator_rejects_pii_causal_action_and_financial_claims(self) -> None:
        questions = (
            "Is owner" + "@" + "example.invalid ready for review?",
            "Targeting caused the observed gap.",
            "Pause the campaign.",
            "ROI is 50 percent.",
        )
        for question in questions:
            with self.subTest(question=question):
                frame = replace(self.frame, decision_question=question)
                with self.assertRaises(DecisionValidationError):
                    assess_decision_frame(
                        self.context, self.opportunity, self.plan, frame
                    )

    def test_no_recommendation_or_outcome_fields_exist(self) -> None:
        rendered = json.dumps(self.portfolio.to_dict()).lower()
        for forbidden in (
            "recommended_option", "selected_action", "best_action",
            "expected_return", "predicted_outcome",
        ):
            self.assertNotIn(forbidden, rendered)


class DecisionCliAndHardcodingTests(unittest.TestCase):
    def test_cli_is_offline_aggregate_and_filters_active_opportunity(self) -> None:
        context = synthetic_context()
        active = evaluate_opportunities(context).opportunities[0]
        output = io.StringIO()
        with patch(
            "src.intelligence.decision_cli.build_context", return_value=context
        ), redirect_stdout(output):
            code = decision_main([
                "readiness",
                "--business-id", BUSINESS,
                "--format", "json",
                "--opportunity-id", active.opportunity_id,
            ])
        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertTrue(payload["assessments"])
        self.assertTrue(all(
            item["originating_opportunity_id"] == active.opportunity_id
            for item in payload["assessments"]
        ))
        rendered = output.getvalue().lower()
        for forbidden in ("customer_id", "phone_number", "tracking_number", "openai"):
            self.assertNotIn(forbidden, rendered)

    def test_cli_rejects_unknown_or_suppressed_opportunity(self) -> None:
        error = io.StringIO()
        with patch(
            "src.intelligence.decision_cli.build_context",
            return_value=synthetic_context(),
        ), patch("sys.stderr", error):
            code = decision_main([
                "readiness", "--business-id", BUSINESS,
                "--opportunity-id", "opportunity:unknown:scope",
            ])
        self.assertEqual(code, 2)
        self.assertIn("not active", error.getvalue())

    def test_engine_has_no_real_pilot_constants(self) -> None:
        source = "\n".join(
            Path(path).read_text(encoding="utf-8")
            for path in (
                "src/intelligence/decision_models.py",
                "src/intelligence/decisions.py",
            )
        )
        for forbidden in (
            "Sama-NewUM", "sama_cod_pilot", "267", "951", "84.48", "97.71", "85",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
