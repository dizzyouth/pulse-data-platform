"""Phase 6.9B sequencing API, deterministic answers, and UI contracts."""

from __future__ import annotations

from dataclasses import replace
from itertools import combinations
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import STATIC_DIR, create_app
from src.api.service import AnalystService
from src.intelligence.decisions import build_decision_readiness_portfolio
from src.intelligence.investigation_models import (
    InvestigationPortfolio,
    InvestigationReadiness,
)
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.narration import validate_answer
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.providers import OfflineFakeProvider
from src.intelligence.sequencing import build_investigation_sequencing_portfolio
from src.intelligence.sequencing_answers import (
    SequencingQuestionIntent,
    answer_sequencing_question,
    classify_sequencing_question,
)
from src.intelligence.sequencing_models import (
    SequencingPortfolioState,
    SequencingValidationError,
)
from tests.test_opportunities import BUSINESS, synthetic_context


def replace_task_readiness(
    portfolio: InvestigationPortfolio,
    task_id: str,
    readiness: InvestigationReadiness,
) -> InvestigationPortfolio:
    plans = tuple(
        replace(
            plan,
            tasks=tuple(
                replace(task, readiness=readiness)
                if task.task_id == task_id else task
                for task in plan.tasks
            ),
        )
        for plan in portfolio.plans
    )
    tasks = tuple(task for plan in plans for task in plan.tasks)
    return replace(
        portfolio,
        plans=plans,
        ready_task_count=sum(
            item.readiness is InvestigationReadiness.READY_NOW for item in tasks
        ),
        partial_task_count=sum(
            item.readiness is InvestigationReadiness.PARTIALLY_READY for item in tasks
        ),
        blocked_task_count=sum(
            item.readiness in {
                InvestigationReadiness.BLOCKED_MISSING_EVIDENCE,
                InvestigationReadiness.BLOCKED_BOUNDARY,
            }
            for item in tasks
        ),
    )


class SequencingApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.investigations = build_investigation_portfolio(
            self.context, self.evaluation
        )
        self.decisions = build_decision_readiness_portfolio(
            self.context, self.evaluation, self.investigations
        )
        self.sequencing = build_investigation_sequencing_portfolio(
            self.context, self.evaluation, self.investigations, self.decisions
        )
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.context_builder = MagicMock(return_value=self.context)
        self.client = TestClient(create_app(AnalystService(
            context_builder=self.context_builder,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )))

    def test_portfolio_endpoint_returns_engine_contract_verbatim(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/sequencing", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload, self.sequencing.to_dict())
        self.assertEqual(payload["state"], self.sequencing.state.value)
        self.assertEqual(
            payload["top_evidence_focus_requirement_id"],
            self.sequencing.top_evidence_focus_requirement_id,
        )
        self.assertEqual(
            payload["recommended_next_task_id"],
            self.sequencing.recommended_next_task_id,
        )
        self.provider_factory.assert_not_called()

    def test_engine_order_counts_and_boundary_partition_are_preserved(self) -> None:
        payload = self.client.get(
            "/api/v1/analyst/sequencing", params={"business_id": BUSINESS}
        ).json()
        self.assertEqual(
            [item["requirement_id"] for item in payload["evidence_leverage_items"]],
            [item.requirement_id for item in self.sequencing.evidence_leverage_items],
        )
        self.assertEqual(
            [item["task_id"] for item in payload["sequence_items"]],
            [item.task_id for item in self.sequencing.sequence_items],
        )
        self.assertEqual(
            [item["sequence_order"] for item in payload["sequence_items"]],
            list(range(1, len(self.sequencing.sequence_items) + 1)),
        )
        for actual, expected in zip(
            payload["evidence_leverage_items"],
            self.sequencing.evidence_leverage_items,
            strict=True,
        ):
            self.assertEqual(actual["affected_decision_count"], expected.affected_decision_count)
            self.assertEqual(actual["affected_opportunity_count"], expected.affected_opportunity_count)
        self.assertEqual(
            payload["boundary_blocked_decision_ids"],
            list(self.sequencing.boundary_blocked_decision_ids),
        )
        self.assertFalse(
            set(payload["targeted_needs_more_evidence_decision_ids"])
            & set(payload["boundary_blocked_decision_ids"])
        )

    def test_evidence_gap_first_without_recommendation_is_valid(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/sequencing", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["state"], "EVIDENCE_GAP_FIRST")
        self.assertIsNone(payload["recommended_next_task_id"])
        self.assertEqual(payload["startable_task_count"], 0)
        self.assertTrue(payload["sequence_items"])
        self.assertTrue(all(not item["can_begin_now"] for item in payload["sequence_items"]))

    def test_response_excludes_private_raw_execution_and_score_fields(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/sequencing", params={"business_id": BUSINESS}
        )
        lowered = response.text.lower()
        for forbidden in (
            "raw_context", "source_relation", "select *", " from marts.",
            "customer_id", "order_id", "lead_id", "phone_number",
            "email_address", "tracking_number", '"score":', '"probability":',
            '"roi":', '"owner":', '"due_date":', '"completion":',
        ):
            self.assertNotIn(forbidden, lowered)

    def test_endpoint_builds_every_layer_once_without_provider(self) -> None:
        self.context_builder.reset_mock()
        with patch(
            "src.api.service.evaluate_opportunities", wraps=evaluate_opportunities
        ) as evaluate, patch(
            "src.api.service.build_investigation_portfolio",
            wraps=build_investigation_portfolio,
        ) as investigate, patch(
            "src.api.service.build_decision_readiness_portfolio",
            wraps=build_decision_readiness_portfolio,
        ) as decide, patch(
            "src.api.service.build_investigation_sequencing_portfolio",
            wraps=build_investigation_sequencing_portfolio,
        ) as sequence:
            response = self.client.get(
                "/api/v1/analyst/sequencing", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.context_builder.call_count, 1)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(investigate.call_count, 1)
        self.assertEqual(decide.call_count, 1)
        self.assertEqual(sequence.call_count, 1)
        self.provider_factory.assert_not_called()

    def test_validation_failure_is_controlled(self) -> None:
        with patch(
            "src.api.service.build_investigation_sequencing_portfolio",
            side_effect=SequencingValidationError("private sequencing detail"),
        ):
            response = self.client.get(
                "/api/v1/analyst/sequencing", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["error"]["code"], "SEQUENCING_VALIDATION_FAILED"
        )
        self.assertNotIn("private sequencing detail", response.text)

    def test_capabilities_describe_learning_without_execution(self) -> None:
        response = self.client.get("/api/v1/analyst/capabilities")
        rendered = response.text.lower()
        self.assertEqual(response.status_code, 200)
        self.assertIn("evidence shared across multiple decision-readiness gaps", rendered)
        self.assertIn("could produce relevant evidence", rendered)
        self.assertIn("can begin now", rendered)
        self.assertIn("no automatic evidence collection or task execution", rendered)
        self.assertIn("no guaranteed decision unlock", rendered)


class SequencingAskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.investigations = build_investigation_portfolio(
            self.context, self.evaluation
        )
        self.decisions = build_decision_readiness_portfolio(
            self.context, self.evaluation, self.investigations
        )
        self.sequencing = build_investigation_sequencing_portfolio(
            self.context, self.evaluation, self.investigations, self.decisions
        )
        self.top = next(
            item for item in self.sequencing.evidence_leverage_items
            if item.requirement_id == self.sequencing.top_evidence_focus_requirement_id
        )
        self.task = self.sequencing.sequence_items[0]
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.context_builder = MagicMock(return_value=self.context)
        self.client = TestClient(create_app(AnalystService(
            context_builder=self.context_builder,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )))

    def ask_requirement(self, question: str, requirement_id: str | None = None):
        return self.client.post("/api/v1/analyst/ask", json={
            "business_id": BUSINESS,
            "question": question,
            "sequencing_requirement_id": requirement_id or self.top.requirement_id,
        })

    def ask_task(self, question: str, task_id: str | None = None):
        return self.client.post("/api/v1/analyst/ask", json={
            "business_id": BUSINESS,
            "question": question,
            "sequencing_task_id": task_id or self.task.task_id,
        })

    def test_what_to_learn_does_not_fabricate_blocked_task(self) -> None:
        response = self.ask_requirement("What should we learn next?")
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["question_category"], "SEQUENCING_WHAT_LEARN_NEXT")
        self.assertIn("No readiness-raising investigation", payload["answer"]["answer_summary"])
        self.assertIn(self.top.requirement_name, response.text)
        for item in self.sequencing.sequence_items:
            self.assertNotEqual(payload["answer"]["answer_summary"], item.task_title)
        self.assertNotIn("Next startable investigation:", payload["answer"]["answer_summary"])

    def test_startable_portfolio_returns_exact_recommended_task(self) -> None:
        task_id = self.sequencing.sequence_items[-1].task_id
        investigations = replace_task_readiness(
            self.investigations, task_id, InvestigationReadiness.READY_NOW
        )
        sequencing = build_investigation_sequencing_portfolio(
            self.context, self.evaluation, investigations, self.decisions
        )
        answer = answer_sequencing_question(
            "What should we learn next?",
            sequencing,
            self.context,
            self.evaluation,
            investigations,
            self.decisions,
            leverage_item=sequencing.evidence_leverage_items[0],
        )
        recommended = next(
            item for item in sequencing.sequence_items
            if item.task_id == sequencing.recommended_next_task_id
        )
        self.assertIn(recommended.task_title, answer.answer_summary)
        self.assertIn("Next startable investigation", answer.answer_summary)
        self.assertIs(validate_answer(answer, answer.question, self.context), answer)

    def test_focus_answers_use_counts_ordering_and_value_boundary(self) -> None:
        response = self.ask_requirement("Why is this the top evidence focus?")
        self.assertEqual(response.status_code, 200, response.text)
        rendered = response.text.lower()
        self.assertEqual(
            response.json()["question_category"], "SEQUENCING_WHY_THIS_EVIDENCE"
        )
        self.assertIn("non-ready decisions", rendered)
        self.assertIn("opportunities", rendered)
        self.assertIn("ordering first compares", rendered)
        self.assertIn("not an expected-value or roi score", rendered)

    def test_affected_decisions_are_exact_and_never_promised_an_unlock(self) -> None:
        response = self.ask_requirement("Which decisions depend on this evidence?")
        self.assertEqual(response.status_code, 200, response.text)
        rendered = response.text
        by_id = {item.decision_id: item for item in self.decisions.assessments}
        for decision_id in self.top.affected_decision_ids:
            self.assertIn(by_id[decision_id].decision_question, rendered)
        for decision in self.decisions.assessments:
            if decision.decision_id not in self.top.affected_decision_ids:
                self.assertNotIn(decision.decision_question, rendered)
        self.assertNotIn("will unlock", rendered.lower())
        self.assertNotIn("will make ready", rendered.lower())

    def test_guarantee_boundary_is_explicit(self) -> None:
        response = self.ask_requirement("Will this evidence guarantee readiness?")
        self.assertEqual(response.status_code, 200, response.text)
        rendered = response.text.lower()
        self.assertIn("no guarantee", rendered)
        self.assertIn("must be recalculated", rendered)
        self.assertIn("task readiness is not completion", rendered)
        self.assertNotIn("will unlock", rendered)

    def test_no_startable_and_blocked_task_answers_preserve_readiness(self) -> None:
        no_start = self.ask_requirement(
            "Why is there no next startable investigation?"
        )
        self.assertEqual(no_start.status_code, 200, no_start.text)
        self.assertIn("current decision-readiness gaps", no_start.text)
        self.assertIn("blocked by missing evidence", no_start.text)
        self.assertIn("no task is presented", no_start.text)

        task = self.ask_task("Can this investigation begin now?")
        self.assertEqual(task.status_code, 200, task.text)
        self.assertIn("Can this investigation begin now? No.", task.text)
        self.assertIn(self.task.task_readiness.value, task.text)
        self.assertIn(self.task.limitation, task.text)
        self.assertNotIn("Pulse will", task.text)

    def test_task_order_and_help_answers_use_selected_sequence_item(self) -> None:
        ordered = self.ask_task("Why is this ordered here?")
        helpful = self.ask_task("What could this investigation help clarify?")
        self.assertEqual(ordered.status_code, 200, ordered.text)
        self.assertEqual(helpful.status_code, 200, helpful.text)
        self.assertIn(self.task.task_title, ordered.text)
        self.assertIn(self.task.sequencing_reason.split(" relevant to ")[0], ordered.text)
        requirement_names = {
            requirement.requirement_id: requirement.name
            for plan in self.investigations.plans
            for requirement in plan.evidence_requirements
        }
        for requirement_id in self.task.addressed_requirement_ids:
            self.assertIn(requirement_names[requirement_id], helpful.text)

    def test_provider_metadata_and_evidence_refs_are_strict(self) -> None:
        response = self.ask_requirement("Why is this the top evidence focus?")
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["meta"]["provider"], "deterministic")
        self.assertEqual(payload["meta"]["model"], "sequencing-engine-v1")
        self.assertEqual(
            payload["meta"]["answer_source"], "deterministic_evidence_sequencing"
        )
        self.assertEqual(payload["meta"]["provider_call_count"], 0)
        self.assertFalse(payload["meta"]["repair_attempted"])
        self.assertFalse(payload["meta"]["deterministic_fallback_used"])
        for evidence_ref in payload["answer"]["evidence_refs"]:
            self.assertIn(evidence_ref, self.context.evidence_by_id)
            self.assertFalse(evidence_ref.startswith((
                "requirement:", "evidence-gap:", "investigation-task:",
                "investigation-plan:", "decision-readiness:", "opportunity:",
            )))
        self.provider_factory.assert_not_called()

    def test_requirement_and_task_ids_fail_closed(self) -> None:
        invalid_requirement = self.ask_requirement("Explain this.", "not-a-requirement")
        missing_requirement = self.ask_requirement(
            "Explain this.", "requirement:missing_current_scope"
        )
        invalid_task = self.ask_task("Explain this.", "not-a-task")
        missing_task = self.ask_task(
            "Explain this.", "investigation-task:missing:scope:task"
        )
        self.assertEqual(invalid_requirement.status_code, 422)
        self.assertEqual(
            invalid_requirement.json()["error"]["code"],
            "INVALID_SEQUENCING_REQUIREMENT_ID",
        )
        self.assertEqual(missing_requirement.status_code, 404)
        self.assertEqual(
            missing_requirement.json()["error"]["code"],
            "SEQUENCING_REQUIREMENT_NOT_FOUND",
        )
        self.assertEqual(invalid_task.status_code, 422)
        self.assertEqual(
            invalid_task.json()["error"]["code"], "INVALID_SEQUENCING_TASK_ID"
        )
        self.assertEqual(missing_task.status_code, 404)
        self.assertEqual(
            missing_task.json()["error"]["code"], "SEQUENCING_TASK_NOT_FOUND"
        )

    def test_every_mixed_context_id_pair_and_extra_field_is_rejected(self) -> None:
        opportunity = self.evaluation.opportunities[0]
        investigation_task = self.investigations.plans[0].tasks[0]
        decision = self.decisions.assessments[0]
        fields = {
            "opportunity_id": opportunity.opportunity_id,
            "investigation_task_id": investigation_task.task_id,
            "decision_id": decision.decision_id,
            "sequencing_requirement_id": self.top.requirement_id,
            "sequencing_task_id": self.task.task_id,
        }
        for pair in combinations(fields, 2):
            with self.subTest(pair=pair):
                response = self.client.post("/api/v1/analyst/ask", json={
                    "business_id": BUSINESS,
                    "question": "Explain this.",
                    pair[0]: fields[pair[0]],
                    pair[1]: fields[pair[1]],
                })
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")
        extra = self.client.post("/api/v1/analyst/ask", json={
            "business_id": BUSINESS,
            "question": "Explain this.",
            "sequencing_requirement_id": self.top.requirement_id,
            "unexpected": True,
        })
        self.assertEqual(extra.status_code, 422)

    def test_all_existing_ask_modes_remain_separate(self) -> None:
        opportunity = self.evaluation.opportunities[0]
        task = self.investigations.plans[0].tasks[0]
        decision = self.decisions.assessments[0]
        cases = (
            ("opportunity_id", opportunity.opportunity_id, "Why is this an opportunity?", "deterministic_opportunity"),
            ("investigation_task_id", task.task_id, "What is this investigation?", "deterministic_investigation"),
            ("decision_id", decision.decision_id, "What does this decision mean?", "deterministic_decision_readiness"),
        )
        for field, value, question, source in cases:
            with self.subTest(field=field):
                response = self.client.post("/api/v1/analyst/ask", json={
                    "business_id": BUSINESS, "question": question, field: value,
                })
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["meta"]["answer_source"], source)
        provider = OfflineFakeProvider()
        ordinary = TestClient(create_app(AnalystService(
            context_builder=lambda _business_id: self.context,
            provider_factory=lambda _name: provider,
            warehouse_checker=lambda: True,
        ))).post("/api/v1/analyst/ask", json={
            "business_id": BUSINESS, "question": "Why are returns high?",
        })
        self.assertEqual(ordinary.status_code, 200, ordinary.text)
        self.assertEqual(ordinary.json()["meta"]["answer_source"], "grounded_analyst")

    def test_sequencing_ask_builds_every_layer_once(self) -> None:
        self.context_builder.reset_mock()
        with patch(
            "src.api.service.evaluate_opportunities", wraps=evaluate_opportunities
        ) as evaluate, patch(
            "src.api.service.build_investigation_portfolio",
            wraps=build_investigation_portfolio,
        ) as investigate, patch(
            "src.api.service.build_decision_readiness_portfolio",
            wraps=build_decision_readiness_portfolio,
        ) as decide, patch(
            "src.api.service.build_investigation_sequencing_portfolio",
            wraps=build_investigation_sequencing_portfolio,
        ) as sequence:
            response = self.ask_requirement("Why is this the top evidence focus?")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.context_builder.call_count, 1)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(investigate.call_count, 1)
        self.assertEqual(decide.call_count, 1)
        self.assertEqual(sequence.call_count, 1)
        self.provider_factory.assert_not_called()

    def test_classifier_and_direct_answers_pass_existing_validator(self) -> None:
        cases = {
            "Explain this sequence.": SequencingQuestionIntent.SEQUENCING_OVERVIEW,
            "What should we learn next?": SequencingQuestionIntent.SEQUENCING_WHAT_LEARN_NEXT,
            "What is the top evidence focus?": SequencingQuestionIntent.SEQUENCING_TOP_EVIDENCE_FOCUS,
            "Why is this the top evidence focus?": SequencingQuestionIntent.SEQUENCING_WHY_THIS_EVIDENCE,
            "Which decisions depend on this evidence?": SequencingQuestionIntent.SEQUENCING_AFFECTED_DECISIONS,
            "What investigation can start next?": SequencingQuestionIntent.SEQUENCING_NEXT_STARTABLE_TASK,
            "Why is there no next startable investigation?": SequencingQuestionIntent.SEQUENCING_WHY_NO_STARTABLE_TASK,
            "Why is this ordered here?": SequencingQuestionIntent.SEQUENCING_TASK_WHY_ORDERED,
            "Can this investigation begin now?": SequencingQuestionIntent.SEQUENCING_TASK_CAN_BEGIN,
            "What could this investigation help clarify?": SequencingQuestionIntent.SEQUENCING_TASK_WHAT_COULD_HELP,
            "Will this evidence unlock the decisions?": SequencingQuestionIntent.SEQUENCING_GUARANTEE_BOUNDARY,
        }
        task_intents = {
            SequencingQuestionIntent.SEQUENCING_TASK_WHY_ORDERED,
            SequencingQuestionIntent.SEQUENCING_TASK_CAN_BEGIN,
            SequencingQuestionIntent.SEQUENCING_TASK_WHAT_COULD_HELP,
        }
        for question, intent in cases.items():
            with self.subTest(question=question):
                self.assertIs(classify_sequencing_question(question), intent)
                answer = answer_sequencing_question(
                    question,
                    self.sequencing,
                    self.context,
                    self.evaluation,
                    self.investigations,
                    self.decisions,
                    sequence_item=self.task if intent in task_intents else None,
                    leverage_item=self.top if intent not in task_intents else None,
                )
                self.assertIs(validate_answer(answer, question, self.context), answer)


class SequencingUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
        cls.styles = (STATIC_DIR / "styles.css").read_text(encoding="utf-8")

    def test_section_is_after_decisions_and_initial_load_fetches_once(self) -> None:
        self.assertGreater(
            self.html.index('id="sequencing-section"'),
            self.html.index('id="decision-readiness-section"'),
        )
        self.assertEqual(self.javascript.count("fetch(`${SEQUENCING_ENDPOINT}"), 1)
        self.assertIn("Promise.all", self.javascript)
        self.assertNotIn("fetch(`${SEQUENCING_ENDPOINT}/${", self.javascript)

    def test_summary_uses_authoritative_api_fields_without_fallback_task(self) -> None:
        for value in (
            "sequencing.state",
            "sequencing.top_evidence_focus_requirement_id",
            "sequencing.recommended_next_task_id",
            "sequencing.startable_task_count",
            "top.affected_decision_count",
            "top.affected_opportunity_count",
        ):
            self.assertIn(value, self.javascript)
        self.assertIn('sequencing.recommended_next_task_id === null', self.javascript)
        self.assertNotIn("sequencing.sequence_items[0]", self.javascript)
        self.assertNotIn("sequencing.evidence_leverage_items[0]", self.javascript)
        self.assertIn("Investigation plan starting point:", self.javascript)
        self.assertNotIn("Recommended next investigation:", self.javascript)

    def test_cards_preserve_api_order_and_can_begin_field(self) -> None:
        self.assertIn(
            "for (const item of sequencing.evidence_leverage_items)", self.javascript
        )
        self.assertIn("for (const item of sequencing.sequence_items)", self.javascript)
        self.assertNotIn("sequencing.sequence_items.sort", self.javascript)
        self.assertNotIn("sequencing.evidence_leverage_items.sort", self.javascript)
        self.assertIn('item.can_begin_now ? "Yes" : "No"', self.javascript)
        self.assertIn("item.affected_decision_count", self.javascript)
        self.assertIn("item.affected_opportunity_count", self.javascript)
        self.assertIn("item.sequencing_reason", self.javascript)
        self.assertIn(
            "`${item.highest_opportunity_priority} opportunity priority`",
            self.javascript,
        )

    def test_associations_are_id_based_and_technical_ids_are_secondary(self) -> None:
        for source in (
            "decisionsById.get(id)", "sequenceById.get(id)",
            "leverageById.get(id)", "plansById.get(item.investigation_plan_id)",
            "opportunitiesById.get(item.opportunity_id)",
        ):
            self.assertIn(source, self.javascript)
        self.assertIn("Technical associations", self.javascript)
        self.assertNotIn("title === item", self.javascript)

    def test_sequencing_actions_send_only_their_context_id(self) -> None:
        self.assertIn(
            "requestBody.sequencing_requirement_id = sequencingRequirementId",
            self.javascript,
        )
        self.assertIn(
            "requestBody.sequencing_task_id = sequencingTaskId", self.javascript
        )
        self.assertIn("else if (decisionId)", self.javascript)
        self.assertIn("else if (investigationTaskId)", self.javascript)
        self.assertIn("else if (opportunityId)", self.javascript)
        self.assertIn("Answer about evidence focus:", self.javascript)
        self.assertIn("Answer about investigation sequence:", self.javascript)

    def test_bounded_labels_responsive_layout_and_no_persistence(self) -> None:
        for label in (
            "Startable investigation available", "Evidence gap first",
            "Boundary-limited", "No open readiness gaps", "CAN BEGIN NOW",
            "BLOCKED BY CURRENT EVIDENCE", "None currently available",
            "No readiness-raising investigation can begin now",
        ):
            self.assertIn(label, self.html + self.javascript)
        self.assertIn("overflow-wrap: anywhere", self.styles)
        self.assertIn("@media (max-width: 760px)", self.styles)
        self.assertIn(".sequencing-summary", self.styles)
        self.assertNotIn("localStorage", self.javascript)
        self.assertNotIn("sessionStorage", self.javascript)

    def test_no_business_execution_controls_or_hardcoded_acceptance_values(self) -> None:
        rendered = self.html + self.javascript
        for forbidden_button in (
            ">Pause campaign<", ">Change budget<", ">Approve<", ">Execute<",
            ">Collect data<", ">Start task<", ">Trigger investigation<",
            ">Run ETL<", ">Apply recommendation<", ">Assign owner<",
        ):
            self.assertNotIn(forbidden_button, rendered)
        for forbidden_value in (
            "3 non-ready decisions across 2 opportunities", "sequence items: 6",
            "campaign_offer_mix", "84.48", "97.71", "267", "951",
        ):
            self.assertNotIn(forbidden_value, rendered)

    def test_no_guaranteed_unlock_or_client_persistence_language(self) -> None:
        rendered = (self.html + self.javascript).lower()
        for forbidden in (
            "will unlock", "will make ready", "guarantees readiness",
            "fixes these decisions", "task completed", "mark complete",
        ):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()
