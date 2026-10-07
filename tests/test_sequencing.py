"""Phase 6.9A evidence-leverage and investigation-sequencing contracts."""

from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import FrozenInstanceError, fields, replace
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from src.intelligence.decision_models import (
    DecisionReadiness,
    DecisionReadinessPortfolio,
)
from src.intelligence.decisions import build_decision_readiness_portfolio
from src.intelligence.investigation_models import (
    EvidenceRequirementStatus,
    InvestigationPortfolio,
    InvestigationReadiness,
)
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import OpportunityEvaluation, OpportunityPriority
from src.intelligence.sequencing import (
    build_investigation_sequencing_portfolio,
    validate_investigation_sequencing_portfolio,
)
from src.intelligence.sequencing_cli import main as sequencing_main
from src.intelligence.sequencing_models import (
    EvidenceLeverageItem,
    InvestigationSequenceItem,
    SequencingPortfolioState,
    SequencingValidationError,
)
from tests.test_opportunities import synthetic_context


PRIORITY_ORDER = {
    OpportunityPriority.HIGH: 0,
    OpportunityPriority.MEDIUM: 1,
    OpportunityPriority.LOW: 2,
}


def pipeline(context=None):
    resolved = context or synthetic_context()
    opportunities = evaluate_opportunities(resolved)
    investigations = build_investigation_portfolio(resolved, opportunities)
    decisions = build_decision_readiness_portfolio(
        resolved, opportunities, investigations
    )
    sequencing = build_investigation_sequencing_portfolio(
        resolved, opportunities, investigations, decisions
    )
    return resolved, opportunities, investigations, decisions, sequencing


def decision_portfolio(
    source: DecisionReadinessPortfolio,
    assessments,
) -> DecisionReadinessPortfolio:
    ordered = tuple(
        replace(item, decision_order=index)
        for index, item in enumerate(assessments, 1)
    )
    return replace(
        source,
        assessments=ordered,
        ready_for_human_review_count=sum(
            item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
            for item in ordered
        ),
        needs_more_evidence_count=sum(
            item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
            for item in ordered
        ),
        blocked_by_boundary_count=sum(
            item.readiness is DecisionReadiness.BLOCKED_BY_BOUNDARY
            for item in ordered
        ),
        first_reviewable_decision_id=next(
            (
                item.decision_id
                for item in ordered
                if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
            ),
            None,
        ),
    )


def replace_task_readiness(
    portfolio: InvestigationPortfolio,
    task_id: str,
    readiness: InvestigationReadiness,
) -> InvestigationPortfolio:
    plans = []
    for plan in portfolio.plans:
        tasks = tuple(
            replace(task, readiness=readiness) if task.task_id == task_id else task
            for task in plan.tasks
        )
        plans.append(replace(plan, tasks=tasks))
    all_tasks = tuple(task for plan in plans for task in plan.tasks)
    return replace(
        portfolio,
        plans=tuple(plans),
        ready_task_count=sum(
            item.readiness is InvestigationReadiness.READY_NOW for item in all_tasks
        ),
        partial_task_count=sum(
            item.readiness is InvestigationReadiness.PARTIALLY_READY
            for item in all_tasks
        ),
        blocked_task_count=sum(
            item.readiness in {
                InvestigationReadiness.BLOCKED_MISSING_EVIDENCE,
                InvestigationReadiness.BLOCKED_BOUNDARY,
            }
            for item in all_tasks
        ),
    )


class SequencingLeverageTests(unittest.TestCase):
    def setUp(self) -> None:
        (
            self.context,
            self.opportunities,
            self.investigations,
            self.decisions,
            self.portfolio,
        ) = pipeline()

    def test_shared_requirement_is_deduplicated_and_ranks_by_breadth(self) -> None:
        offer = self.portfolio.evidence_leverage_items[0]
        self.assertEqual(offer.requirement_id, "requirement:campaign_offer_mix")
        self.assertEqual(offer.affected_decision_count, 3)
        self.assertEqual(offer.affected_opportunity_count, 2)
        self.assertTrue(offer.shared_across_decisions)
        self.assertTrue(offer.shared_across_opportunities)
        self.assertEqual(
            sum(
                item.requirement_id == offer.requirement_id
                for item in self.portfolio.evidence_leverage_items
            ),
            1,
        )
        self.assertGreater(
            offer.affected_decision_count,
            self.portfolio.evidence_leverage_items[-1].affected_decision_count,
        )

    def test_counts_priority_gap_identity_and_ready_exclusion_are_exact(self) -> None:
        opportunities = {
            item.opportunity_id: item for item in self.opportunities.opportunities
        }
        targeted = {
            item.decision_id: item
            for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
        }
        ready_ids = {
            item.decision_id
            for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        }
        gaps = {
            item.requirement_id: item.gap_id
            for item in self.investigations.evidence_gaps
        }
        for item in self.portfolio.evidence_leverage_items:
            self.assertEqual(item.affected_decision_count, len(item.affected_decision_ids))
            self.assertEqual(
                item.affected_opportunity_count, len(item.affected_opportunity_ids)
            )
            self.assertFalse(set(item.affected_decision_ids) & ready_ids)
            self.assertTrue(all(
                item.requirement_id in targeted[decision_id].unresolved_requirement_ids
                for decision_id in item.affected_decision_ids
            ))
            expected_priority = min(
                (
                    opportunities[opportunity_id].priority
                    for opportunity_id in item.affected_opportunity_ids
                ),
                key=lambda value: PRIORITY_ORDER[value],
            )
            self.assertIs(item.highest_opportunity_priority, expected_priority)
            self.assertEqual(item.existing_gap_id, gaps.get(item.requirement_id))

    def test_partial_requirement_can_be_unresolved_without_evidence_gap(self) -> None:
        requirement_id = "requirement:platform_event_definition"
        plans = []
        for plan in self.investigations.plans:
            requirements = tuple(
                replace(
                    item,
                    status=EvidenceRequirementStatus.PARTIAL,
                    evidence_refs=("leakage:platform_vs_observed",),
                    missing_reason=(
                        "Only part of this requirement is available in the current "
                        "validated intelligence context."
                    ),
                )
                if item.requirement_id == requirement_id
                else item
                for item in plan.evidence_requirements
            )
            plans.append(replace(plan, evidence_requirements=requirements))
        investigations = replace(
            self.investigations,
            plans=tuple(plans),
            evidence_gaps=tuple(
                item
                for item in self.investigations.evidence_gaps
                if item.requirement_id != requirement_id
            ),
        )
        portfolio = build_investigation_sequencing_portfolio(
            self.context, self.opportunities, investigations, self.decisions
        )
        item = next(
            value
            for value in portfolio.evidence_leverage_items
            if value.requirement_id == requirement_id
        )
        self.assertIs(item.requirement_status, EvidenceRequirementStatus.PARTIAL)
        self.assertIsNone(item.existing_gap_id)

    def test_conflicting_requirement_definition_or_status_fails_closed(self) -> None:
        target_id = "requirement:campaign_offer_mix"
        occurrence = 0
        plans = []
        for plan in self.investigations.plans:
            requirements = []
            for requirement in plan.evidence_requirements:
                if requirement.requirement_id == target_id:
                    occurrence += 1
                    if occurrence == 2:
                        requirement = replace(requirement, name="Conflicting definition")
                requirements.append(requirement)
            plans.append(replace(plan, evidence_requirements=tuple(requirements)))
        with self.assertRaisesRegex(
            SequencingValidationError, "Conflicting semantic definitions"
        ):
            build_investigation_sequencing_portfolio(
                self.context,
                self.opportunities,
                replace(self.investigations, plans=tuple(plans)),
                self.decisions,
            )

        occurrence = 0
        plans = []
        for plan in self.investigations.plans:
            requirements = []
            for requirement in plan.evidence_requirements:
                if requirement.requirement_id == target_id:
                    occurrence += 1
                    if occurrence == 2:
                        requirement = replace(
                            requirement, status=EvidenceRequirementStatus.PARTIAL
                        )
                requirements.append(requirement)
            plans.append(replace(plan, evidence_requirements=tuple(requirements)))
        with self.assertRaisesRegex(
            SequencingValidationError, "Conflicting current statuses"
        ):
            build_investigation_sequencing_portfolio(
                self.context,
                self.opportunities,
                replace(self.investigations, plans=tuple(plans)),
                self.decisions,
            )

    def test_leverage_order_is_exact_and_breadth_precedes_priority(self) -> None:
        changed_opportunities = tuple(
            replace(
                item,
                priority=(
                    OpportunityPriority.LOW
                    if item.opportunity_id in self.portfolio.evidence_leverage_items[0].affected_opportunity_ids
                    else OpportunityPriority.HIGH
                ),
            )
            for item in self.opportunities.opportunities
        )
        priorities = {
            item.opportunity_id: item.priority for item in changed_opportunities
        }
        changed_plans = tuple(
            replace(plan, opportunity_priority=priorities[plan.opportunity_id])
            for plan in self.investigations.plans
        )
        opportunities = replace(
            self.opportunities, opportunities=changed_opportunities
        )
        investigations = replace(self.investigations, plans=changed_plans)
        portfolio = build_investigation_sequencing_portfolio(
            self.context, opportunities, investigations, self.decisions
        )
        self.assertEqual(
            portfolio.evidence_leverage_items[0].requirement_id,
            "requirement:campaign_offer_mix",
        )
        self.assertIs(
            portfolio.evidence_leverage_items[0].highest_opportunity_priority,
            OpportunityPriority.LOW,
        )
        first_appearance = {}
        for assessment in self.decisions.assessments:
            if assessment.readiness is not DecisionReadiness.NEEDS_MORE_EVIDENCE:
                continue
            for requirement_id in assessment.unresolved_requirement_ids:
                first_appearance.setdefault(requirement_id, len(first_appearance))
        keys = [
            (
                -item.affected_decision_count,
                PRIORITY_ORDER[item.highest_opportunity_priority],
                -item.affected_opportunity_count,
                first_appearance[item.requirement_id],
                item.requirement_id,
            )
            for item in portfolio.evidence_leverage_items
        ]
        self.assertEqual(keys, sorted(keys))


class SequencingTaskAndStateTests(unittest.TestCase):
    def setUp(self) -> None:
        (
            self.context,
            self.opportunities,
            self.investigations,
            self.decisions,
            self.portfolio,
        ) = pipeline()

    def test_only_authoritative_next_evidence_tasks_are_sequenced(self) -> None:
        authoritative = {
            task_id
            for assessment in self.decisions.assessments
            if assessment.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
            for task_id in assessment.next_evidence_task_ids
        }
        self.assertEqual(
            {item.task_id for item in self.portfolio.sequence_items}, authoritative
        )
        all_tasks = {
            task.task_id for plan in self.investigations.plans for task in plan.tasks
        }
        self.assertTrue(all_tasks - authoritative)
        self.assertFalse(
            (all_tasks - authoritative)
            & {item.task_id for item in self.portfolio.sequence_items}
        )

    def test_ready_now_derives_can_begin_and_becomes_recommendation(self) -> None:
        task_id = self.portfolio.sequence_items[-1].task_id
        investigations = replace_task_readiness(
            self.investigations, task_id, InvestigationReadiness.READY_NOW
        )
        portfolio = build_investigation_sequencing_portfolio(
            self.context, self.opportunities, investigations, self.decisions
        )
        selected = next(item for item in portfolio.sequence_items if item.task_id == task_id)
        self.assertTrue(selected.can_begin_now)
        self.assertEqual(portfolio.recommended_next_task_id, task_id)
        self.assertIs(portfolio.state, SequencingPortfolioState.READY_TASK_AVAILABLE)
        self.assertEqual(portfolio.startable_task_count, 1)

    def test_blocked_high_leverage_task_is_not_forced_as_recommendation(self) -> None:
        self.assertTrue(self.portfolio.evidence_leverage_items)
        self.assertTrue(self.portfolio.sequence_items)
        self.assertTrue(all(not item.can_begin_now for item in self.portfolio.sequence_items))
        self.assertIsNone(self.portfolio.recommended_next_task_id)
        self.assertIs(self.portfolio.state, SequencingPortfolioState.EVIDENCE_GAP_FIRST)

    def test_ready_task_does_not_complete_work_or_resolve_requirements(self) -> None:
        task_id = self.portfolio.sequence_items[0].task_id
        before = json.dumps(self.decisions.to_dict(), sort_keys=True)
        investigations = replace_task_readiness(
            self.investigations, task_id, InvestigationReadiness.READY_NOW
        )
        portfolio = build_investigation_sequencing_portfolio(
            self.context, self.opportunities, investigations, self.decisions
        )
        self.assertEqual(before, json.dumps(self.decisions.to_dict(), sort_keys=True))
        selected = next(item for item in portfolio.sequence_items if item.task_id == task_id)
        self.assertTrue(selected.addressed_requirement_ids)
        self.assertTrue(any(
            set(selected.addressed_requirement_ids) <= set(item.unresolved_requirement_ids)
            for item in self.decisions.assessments
        ))
        rendered = json.dumps(portfolio.to_dict()).lower()
        self.assertIn("task readiness is not completion", rendered)
        self.assertNotIn("task completed", rendered)

    def test_boundary_only_and_all_ready_states_are_explicit(self) -> None:
        source = self.decisions.assessments[0]
        boundary_item = replace(
            source,
            readiness=DecisionReadiness.BLOCKED_BY_BOUNDARY,
            unresolved_requirement_ids=(),
            next_evidence_task_ids=(),
        )
        boundary_decisions = decision_portfolio(self.decisions, (boundary_item,))
        boundary = build_investigation_sequencing_portfolio(
            self.context,
            self.opportunities,
            self.investigations,
            boundary_decisions,
        )
        self.assertIs(boundary.state, SequencingPortfolioState.BOUNDARY_ONLY)
        self.assertEqual(boundary.boundary_blocked_decision_ids, (source.decision_id,))
        self.assertEqual(boundary.evidence_leverage_items, ())
        self.assertEqual(boundary.sequence_items, ())

        ready_items = tuple(
            replace(
                item,
                readiness=DecisionReadiness.READY_FOR_HUMAN_REVIEW,
                unresolved_requirement_ids=(),
                next_evidence_task_ids=(),
            )
            for item in self.decisions.assessments
        )
        ready_decisions = decision_portfolio(self.decisions, ready_items)
        ready = build_investigation_sequencing_portfolio(
            self.context, self.opportunities, self.investigations, ready_decisions
        )
        self.assertIs(
            ready.state, SequencingPortfolioState.NO_OPEN_READINESS_GAPS
        )
        self.assertIsNone(ready.top_evidence_focus_requirement_id)
        self.assertIsNone(ready.recommended_next_task_id)

    def test_boundary_decisions_do_not_inflate_normal_leverage(self) -> None:
        needs = next(
            item
            for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
        )
        blocked = replace(needs, readiness=DecisionReadiness.BLOCKED_BY_BOUNDARY)
        decisions = decision_portfolio(self.decisions, (blocked,))
        portfolio = build_investigation_sequencing_portfolio(
            self.context, self.opportunities, self.investigations, decisions
        )
        self.assertEqual(portfolio.evidence_leverage_items, ())
        self.assertEqual(portfolio.targeted_needs_more_evidence_decision_ids, ())
        self.assertEqual(portfolio.boundary_blocked_decision_ids, (blocked.decision_id,))

    def test_task_order_uses_only_the_six_explicit_keys(self) -> None:
        tasks = {
            task.task_id: task
            for plan in self.investigations.plans
            for task in plan.tasks
        }
        keys = [
            (
                not item.can_begin_now,
                PRIORITY_ORDER[item.opportunity_priority],
                -item.affected_decision_count,
                item.opportunity_order,
                tasks[item.task_id].task_order,
                item.task_id,
            )
            for item in self.portfolio.sequence_items
        ]
        self.assertEqual(keys, sorted(keys))

    def test_task_with_multiple_decisions_never_promises_an_unlock(self) -> None:
        shared = next(
            item for item in self.portfolio.sequence_items
            if item.affected_decision_count > 1
        )
        self.assertIn("could produce evidence relevant", shared.sequencing_reason)
        rendered = json.dumps(self.portfolio.to_dict()).lower()
        for forbidden in (
            "will unlock", "guaranteed to unlock", "will make decisions ready",
            "automatically make", "expected roi", "revenue upside",
        ):
            self.assertNotIn(forbidden, rendered)
        self.assertIn("does not guarantee that any decision will become ready", rendered)


class SequencingValidationAndCliTests(unittest.TestCase):
    def setUp(self) -> None:
        (
            self.context,
            self.opportunities,
            self.investigations,
            self.decisions,
            self.portfolio,
        ) = pipeline()

    def test_models_are_immutable_and_have_no_score_or_completion_fields(self) -> None:
        with self.assertRaises(FrozenInstanceError):
            self.portfolio.state = SequencingPortfolioState.BOUNDARY_ONLY
        model_fields = {
            item.name
            for model in (
                EvidenceLeverageItem,
                InvestigationSequenceItem,
                type(self.portfolio),
            )
            for item in fields(model)
        }
        for forbidden in (
            "score", "weighted_score", "expected_value", "roi", "probability",
            "completed", "owner", "due_date", "execution_state",
        ):
            self.assertNotIn(forbidden, model_fields)

    def test_unknown_and_cross_layer_references_fail_closed(self) -> None:
        source = next(
            item
            for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
        )
        unknown_task = "investigation-task:unknown:scope:task"
        bad_task = replace(
            source,
            relevant_investigation_task_ids=(
                *source.relevant_investigation_task_ids, unknown_task
            ),
            next_evidence_task_ids=(*source.next_evidence_task_ids, unknown_task),
        )
        unknown_requirement = "requirement:unknown"
        bad_requirement = replace(
            source,
            required_requirement_ids=(
                *source.required_requirement_ids, unknown_requirement
            ),
            unresolved_requirement_ids=(
                *source.unresolved_requirement_ids, unknown_requirement
            ),
        )
        unrelated_task = next(
            task.task_id
            for plan in self.investigations.plans
            if plan.plan_id != source.investigation_plan_id
            for task in plan.tasks
        )
        bad_plan_link = replace(
            source,
            relevant_investigation_task_ids=(
                *source.relevant_investigation_task_ids, unrelated_task
            ),
            next_evidence_task_ids=(
                *source.next_evidence_task_ids, unrelated_task
            ),
        )
        cases = (
            (bad_task, "unknown"),
            (bad_requirement, "unknown"),
            (bad_plan_link, "unknown or unrelated"),
            (
                replace(
                    source,
                    investigation_plan_id="investigation-plan:unknown:scope",
                ),
                "unknown plan",
            ),
            (
                replace(
                    source,
                    originating_opportunity_id="opportunity:unknown:scope",
                ),
                "unknown opportunity",
            ),
            (replace(source, business_id="other_business"), "crosses business"),
        )
        for assessment, message in cases:
            with self.subTest(message=message):
                decisions = decision_portfolio(self.decisions, (assessment,))
                with self.assertRaisesRegex(SequencingValidationError, message):
                    build_investigation_sequencing_portfolio(
                        self.context,
                        self.opportunities,
                        self.investigations,
                        decisions,
                    )

    def test_portfolio_model_rejects_invalid_focus_recommendation_and_duplicates(self) -> None:
        with self.assertRaisesRegex(SequencingValidationError, "top evidence focus"):
            replace(
                self.portfolio,
                top_evidence_focus_requirement_id="requirement:unknown",
            )
        blocked_task_id = self.portfolio.sequence_items[0].task_id
        with self.assertRaisesRegex(SequencingValidationError, "startable"):
            replace(self.portfolio, recommended_next_task_id=blocked_task_id)
        duplicate_order = replace(self.portfolio.sequence_items[1], sequence_order=1)
        with self.assertRaisesRegex(SequencingValidationError, "sequence orders"):
            replace(
                self.portfolio,
                sequence_items=(
                    self.portfolio.sequence_items[0],
                    duplicate_order,
                    *self.portfolio.sequence_items[2:],
                ),
            )
        with self.assertRaisesRegex(SequencingValidationError, "cannot duplicate"):
            replace(
                self.portfolio,
                evidence_leverage_items=(
                    self.portfolio.evidence_leverage_items[0],
                    self.portfolio.evidence_leverage_items[0],
                ),
            )

    def test_output_validator_rejects_false_linkage_order_and_unsafe_text(self) -> None:
        sequence = list(self.portfolio.sequence_items)
        ready_id = next(
            item.decision_id
            for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        )
        false_link = replace(
            sequence[0], affected_decision_ids=(ready_id,), affected_decision_count=1
        )
        false_portfolio = replace(
            self.portfolio, sequence_items=(false_link, *sequence[1:])
        )
        with self.assertRaisesRegex(SequencingValidationError, "not present"):
            validate_investigation_sequencing_portfolio(
                false_portfolio,
                self.context,
                self.opportunities,
                self.investigations,
                self.decisions,
            )

        reordered = tuple(
            replace(item, sequence_order=index)
            for index, item in enumerate(reversed(sequence), 1)
        )
        with self.assertRaisesRegex(SequencingValidationError, "deterministic"):
            validate_investigation_sequencing_portfolio(
                replace(self.portfolio, sequence_items=reordered),
                self.context,
                self.opportunities,
                self.investigations,
                self.decisions,
            )
        for value, message in (
            ("Pause campaign automatically.", "execution"),
            ("Contact customer" + "@" + "example.invalid.", "PII"),
            ("Targeting is the cause.", "causal"),
            ("Expected ROI will increase.", "financial"),
            ("This task will unlock decisions.", "promises"),
        ):
            changed = replace(sequence[0], sequencing_reason=value)
            items = (changed, *sequence[1:])
            with self.subTest(value=value):
                with self.assertRaisesRegex(SequencingValidationError, message):
                    validate_investigation_sequencing_portfolio(
                        replace(self.portfolio, sequence_items=items),
                        self.context,
                        self.opportunities,
                        self.investigations,
                        self.decisions,
                    )

    def test_same_inputs_produce_byte_stable_json(self) -> None:
        second = build_investigation_sequencing_portfolio(
            self.context,
            self.opportunities,
            self.investigations,
            self.decisions,
        )
        encode = lambda value: json.dumps(
            value.to_dict(), sort_keys=True, separators=(",", ":")
        )
        self.assertEqual(encode(self.portfolio), encode(second))

    def test_cli_builds_each_layer_once_and_is_provider_free(self) -> None:
        output = io.StringIO()
        with patch(
            "src.intelligence.sequencing_cli.build_context",
            return_value=self.context,
        ) as build_context, patch(
            "src.intelligence.sequencing_cli.evaluate_opportunities",
            return_value=self.opportunities,
        ) as evaluate, patch(
            "src.intelligence.sequencing_cli.build_investigation_portfolio",
            return_value=self.investigations,
        ) as investigate, patch(
            "src.intelligence.sequencing_cli.build_decision_readiness_portfolio",
            return_value=self.decisions,
        ) as decide, patch(
            "src.intelligence.sequencing_cli.build_investigation_sequencing_portfolio",
            return_value=self.portfolio,
        ) as sequence, redirect_stdout(output):
            code = sequencing_main([
                "sequence", "--business-id", "synthetic_business", "--format", "json"
            ])
        self.assertEqual(code, 0)
        for operation in (build_context, evaluate, investigate, decide, sequence):
            self.assertEqual(operation.call_count, 1)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["state"], self.portfolio.state.value)
        rendered = output.getvalue().lower()
        for forbidden in (
            "openai", "customer_id", "phone_number", "tracking_number", "raw row",
        ):
            self.assertNotIn(forbidden, rendered)

    def test_engine_has_no_real_pilot_constants_or_hidden_score(self) -> None:
        source = "\n".join(
            Path(path).read_text(encoding="utf-8")
            for path in (
                "src/intelligence/sequencing_models.py",
                "src/intelligence/sequencing.py",
            )
        )
        for forbidden in (
            "Sama-NewUM", "sama_cod_pilot", "267", "951", "84.48", "97.71",
            "2 ready", "4 missing", "leverage_score", "weighted_score",
            "importance_score",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
