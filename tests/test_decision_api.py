"""Phase 6.8B decision-readiness API, deterministic answers, and UI contracts."""

from __future__ import annotations

from dataclasses import replace
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import STATIC_DIR, create_app
from src.api.service import AnalystService
from src.intelligence.decision_answers import (
    DecisionQuestionIntent,
    answer_decision_question,
    classify_decision_question,
)
from src.intelligence.decision_models import (
    DecisionReadiness,
    DecisionReadinessPortfolio,
    DecisionValidationError,
    ReadinessReasonCode,
)
from src.intelligence.decisions import build_decision_readiness_portfolio
from src.intelligence.investigation_models import InvestigationReadiness
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.narration import validate_answer
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.providers import OfflineFakeProvider
from tests.test_opportunities import BUSINESS, synthetic_context


class DecisionApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.investigations = build_investigation_portfolio(
            self.context, self.evaluation
        )
        self.decisions = build_decision_readiness_portfolio(
            self.context, self.evaluation, self.investigations
        )
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.context_builder = MagicMock(return_value=self.context)
        self.service = AnalystService(
            context_builder=self.context_builder,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )
        self.client = TestClient(create_app(self.service))
        self.ready = next(
            item for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        )
        self.not_ready = next(
            item for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
            and item.unresolved_requirement_ids
        )

    def test_portfolio_endpoint_returns_engine_contract_verbatim(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/decisions", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload, self.decisions.to_dict())
        self.assertEqual(
            [item["decision_id"] for item in payload["assessments"]],
            [item.decision_id for item in self.decisions.assessments],
        )
        self.assertEqual(
            payload["first_reviewable_decision_id"],
            self.decisions.first_reviewable_decision_id,
        )
        self.assertEqual(
            (
                payload["ready_for_human_review_count"],
                payload["needs_more_evidence_count"],
                payload["blocked_by_boundary_count"],
            ),
            (
                self.decisions.ready_for_human_review_count,
                self.decisions.needs_more_evidence_count,
                self.decisions.blocked_by_boundary_count,
            ),
        )
        self.provider_factory.assert_not_called()

    def test_known_unknown_invalid_and_cross_business_detail_are_safe(self) -> None:
        known = self.client.get(
            f"/api/v1/analyst/decisions/{self.ready.decision_id}",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(known.status_code, 200, known.text)
        self.assertEqual(known.json()["assessment"], self.ready.to_dict())

        unknown = self.client.get(
            "/api/v1/analyst/decisions/decision-readiness:confirmation_leakage:missing:confirmation-investigation-review",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.json()["error"]["code"], "DECISION_NOT_FOUND")

        invalid = self.client.get(
            "/api/v1/analyst/decisions/not-a-decision",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(invalid.json()["error"]["code"], "INVALID_DECISION_ID")

        mismatch = self.client.get(
            f"/api/v1/analyst/decisions/{self.ready.decision_id}",
            params={"business_id": "different_business"},
        )
        self.assertEqual(mismatch.status_code, 404)
        self.assertIn(
            mismatch.json()["error"]["code"],
            {"BUSINESS_NOT_FOUND", "DECISION_NOT_FOUND"},
        )

    def test_suppressed_opportunity_produces_no_decisions(self) -> None:
        context = synthetic_context(matched_gap=2)
        evaluation = evaluate_opportunities(context)
        client = TestClient(create_app(AnalystService(
            context_builder=lambda _business_id: context,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )))
        response = client.get(
            "/api/v1/analyst/decisions", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        active_ids = {item.opportunity_id for item in evaluation.opportunities}
        self.assertTrue(all(
            item["originating_opportunity_id"] in active_ids
            for item in response.json()["assessments"]
        ))
        self.assertFalse(any(
            item["decision_type"].startswith("CONFIRMATION_")
            for item in response.json()["assessments"]
        ))

    def test_decision_endpoint_builds_each_layer_once(self) -> None:
        self.context_builder.reset_mock()
        with patch(
            "src.api.service.evaluate_opportunities", wraps=evaluate_opportunities
        ) as evaluate, patch(
            "src.api.service.build_investigation_portfolio",
            wraps=build_investigation_portfolio,
        ) as investigate, patch(
            "src.api.service.build_decision_readiness_portfolio",
            wraps=build_decision_readiness_portfolio,
        ) as decide:
            response = self.client.get(
                "/api/v1/analyst/decisions", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.context_builder.call_count, 1)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(investigate.call_count, 1)
        self.assertEqual(decide.call_count, 1)
        self.provider_factory.assert_not_called()

    def test_validation_failure_is_controlled(self) -> None:
        with patch(
            "src.api.service.build_decision_readiness_portfolio",
            side_effect=DecisionValidationError("private decision detail"),
        ):
            response = self.client.get(
                "/api/v1/analyst/decisions", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["error"]["code"], "DECISION_VALIDATION_FAILED"
        )
        self.assertNotIn("private decision detail", response.text)

    def test_response_excludes_private_raw_and_suppressed_fields(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/decisions", params={"business_id": BUSINESS}
        )
        lowered = response.text.lower()
        for forbidden in (
            "suppressed_candidates", "raw_context", "source_relation", "select *",
            " from marts.",
            "customer_id", "order_id", "lead_id", "phone_number", "email_address",
            "tracking_number", "recommended_option", "selected_action", '"roi":',
        ):
            self.assertNotIn(forbidden, lowered)

    def test_capabilities_include_decision_support_without_execution(self) -> None:
        response = self.client.get("/api/v1/analyst/capabilities")
        self.assertEqual(response.status_code, 200)
        rendered = response.text.lower()
        self.assertIn("bounded human decision review", rendered)
        self.assertIn("why a decision is or is not ready", rendered)
        self.assertIn("could raise readiness", rendered)
        self.assertIn("no autonomous decisions", rendered)


class DecisionAskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.investigations = build_investigation_portfolio(
            self.context, self.evaluation
        )
        self.decisions = build_decision_readiness_portfolio(
            self.context, self.evaluation, self.investigations
        )
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.context_builder = MagicMock(return_value=self.context)
        self.client = TestClient(create_app(AnalystService(
            context_builder=self.context_builder,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )))
        self.ready = next(
            item for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.READY_FOR_HUMAN_REVIEW
        )
        self.not_ready = next(
            item for item in self.decisions.assessments
            if item.readiness is DecisionReadiness.NEEDS_MORE_EVIDENCE
            and item.unresolved_requirement_ids
        )

    def ask(self, question: str, decision_id: str | None = None):
        body = {"business_id": BUSINESS, "question": question}
        if decision_id is not None:
            body["decision_id"] = decision_id
        return self.client.post("/api/v1/analyst/ask", json=body)

    def _objects(self, assessment):
        opportunity = next(
            item for item in self.evaluation.opportunities
            if item.opportunity_id == assessment.originating_opportunity_id
        )
        plan = next(
            item for item in self.investigations.plans
            if item.plan_id == assessment.investigation_plan_id
        )
        return opportunity, plan

    def test_ready_answer_is_deterministic_grounded_and_provider_free(self) -> None:
        response = self.ask(
            "Why is this ready for human review?", self.ready.decision_id
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["question_category"], "DECISION_WHY_READY")
        self.assertIn("ready for human review", payload["answer"]["answer_summary"])
        self.assertIn("human review", response.text.lower())
        self.assertEqual(payload["meta"]["provider"], "deterministic")
        self.assertEqual(payload["meta"]["model"], "decision-readiness-engine-v1")
        self.assertEqual(
            payload["meta"]["answer_source"], "deterministic_decision_readiness"
        )
        self.assertEqual(payload["meta"]["provider_call_count"], 0)
        self.assertFalse(payload["meta"]["repair_attempted"])
        self.assertFalse(payload["meta"]["deterministic_fallback_used"])
        self.assertNotIn("should", payload["answer"]["answer_summary"].lower())
        self.provider_factory.assert_not_called()

    def test_not_ready_answer_surfaces_exact_requirements_and_tasks(self) -> None:
        response = self.ask("Why is this not ready?", self.not_ready.decision_id)
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["question_category"], "DECISION_WHY_NOT_READY")
        _opportunity, plan = self._objects(self.not_ready)
        requirements = {item.requirement_id: item for item in plan.evidence_requirements}
        tasks = {item.task_id: item for item in plan.tasks}
        for requirement_id in self.not_ready.unresolved_requirement_ids:
            self.assertIn(requirements[requirement_id].name, response.text)
        for task_id in self.not_ready.next_evidence_task_ids:
            self.assertIn(tasks[task_id].title, response.text)
        self.assertIn("opportunity priority", response.text.lower())
        self.provider_factory.assert_not_called()

    def test_missing_evidence_uses_existing_requirement_content_only(self) -> None:
        response = self.ask(
            "What evidence is missing?", self.not_ready.decision_id
        )
        self.assertEqual(response.status_code, 200, response.text)
        _opportunity, plan = self._objects(self.not_ready)
        requirements = {item.requirement_id: item for item in plan.evidence_requirements}
        for requirement_id in self.not_ready.unresolved_requirement_ids:
            requirement = requirements[requirement_id]
            self.assertIn(requirement.name, response.text)
            self.assertIn(requirement.description, response.text)
            self.assertIn(requirement.status.value, response.text)
            self.assertIn(requirement.collection_hint, response.text)
        for invented in ("api endpoint", "connector", "warehouse table", "data owner"):
            self.assertNotIn(invented, response.text.lower())

    def test_next_evidence_uses_only_assessment_task_ids(self) -> None:
        response = self.ask(
            "What could raise readiness?", self.not_ready.decision_id
        )
        self.assertEqual(response.status_code, 200, response.text)
        _opportunity, plan = self._objects(self.not_ready)
        selected = set(self.not_ready.next_evidence_task_ids)
        for task in plan.tasks:
            if task.task_id in selected:
                self.assertIn(task.title, response.text)
            else:
                self.assertNotIn(task.title, response.text)
        self.assertIn("does not mean the task has been completed", response.text)

    def test_boundary_priority_and_human_review_intents_use_engine_objects(self) -> None:
        opportunity, _plan = self._objects(self.not_ready)
        cases = (
            (
                "What is the decision boundary?",
                "DECISION_BOUNDARY",
                self.not_ready.decision_boundary,
            ),
            (
                "Why is this HIGH priority but not ready?",
                "DECISION_PRIORITY_VS_READINESS",
                opportunity.priority.value,
            ),
            (
                "Does ready mean Pulse recommends the change?",
                "DECISION_HUMAN_REVIEW",
                "No.",
            ),
        )
        for question, category, expected in cases:
            with self.subTest(question=question):
                response = self.ask(question, self.not_ready.decision_id)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["question_category"], category)
                self.assertIn(expected, response.text)
        human = self.ask(
            "Does ready mean Pulse recommends the change?", self.ready.decision_id
        )
        self.assertIn("Human review is required", human.text)
        self.assertIn("autonomous action is not allowed", human.text)

    def test_financial_boundary_answer_is_not_presented_as_ordinary_missing_evidence(self) -> None:
        opportunity, plan = self._objects(self.ready)
        blocked = replace(
            self.ready,
            readiness=DecisionReadiness.BLOCKED_BY_BOUNDARY,
            readiness_reason_codes=(
                ReadinessReasonCode.ECONOMIC_BOUNDARY,
                ReadinessReasonCode.HUMAN_REVIEW_REQUIRED,
            ),
            rationale_summary="An explicit economic boundary prevents review.",
            decision_boundary=(
                "Trusted cross-currency economics are unavailable while FX_REQUIRED applies."
            ),
        )
        portfolio = DecisionReadinessPortfolio(
            business_id=self.context.business_id,
            as_of_date=self.context.as_of_date,
            assessments=(blocked,),
            ready_for_human_review_count=0,
            needs_more_evidence_count=0,
            blocked_by_boundary_count=1,
            first_reviewable_decision_id=None,
        )
        answer = answer_decision_question(
            "Why is this not ready?",
            blocked,
            opportunity,
            plan,
            portfolio,
            self.investigations,
            self.context,
        )
        self.assertIn("blocked by an explicit boundary", answer.answer_summary)
        self.assertIn("FX_REQUIRED", " ".join(answer.limitations))
        self.assertNotIn("just collect", answer.answer_summary.lower())

    def test_mixed_context_ids_are_rejected(self) -> None:
        opportunity = self.evaluation.opportunities[0]
        task = next(
            task for plan in self.investigations.plans for task in plan.tasks
            if task.readiness is InvestigationReadiness.READY_NOW
        )
        cases = (
            {"opportunity_id": opportunity.opportunity_id, "decision_id": self.ready.decision_id},
            {"investigation_task_id": task.task_id, "decision_id": self.ready.decision_id},
            {"opportunity_id": opportunity.opportunity_id, "investigation_task_id": task.task_id},
        )
        for values in cases:
            with self.subTest(values=values):
                response = self.client.post(
                    "/api/v1/analyst/ask",
                    json={"business_id": BUSINESS, "question": "Explain this.", **values},
                )
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")

    def test_existing_ask_modes_remain_separate(self) -> None:
        opportunity = self.evaluation.opportunities[0]
        task = self.investigations.plans[0].tasks[0]
        opportunity_response = self.client.post(
            "/api/v1/analyst/ask",
            json={
                "business_id": BUSINESS,
                "question": "Why is this an opportunity?",
                "opportunity_id": opportunity.opportunity_id,
            },
        )
        investigation_response = self.client.post(
            "/api/v1/analyst/ask",
            json={
                "business_id": BUSINESS,
                "question": "What is this investigation?",
                "investigation_task_id": task.task_id,
            },
        )
        self.assertEqual(
            opportunity_response.json()["meta"]["answer_source"],
            "deterministic_opportunity",
        )
        self.assertEqual(
            investigation_response.json()["meta"]["answer_source"],
            "deterministic_investigation",
        )
        provider = OfflineFakeProvider()
        ordinary = TestClient(create_app(AnalystService(
            context_builder=lambda _business_id: self.context,
            provider_factory=lambda _name: provider,
            warehouse_checker=lambda: True,
        ))).post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": "Why are returns high?"},
        )
        self.assertEqual(ordinary.status_code, 200, ordinary.text)
        self.assertEqual(ordinary.json()["meta"]["answer_source"], "grounded_analyst")

    def test_decision_ask_builds_once_and_uses_only_context_evidence(self) -> None:
        self.context_builder.reset_mock()
        with patch(
            "src.api.service.evaluate_opportunities", wraps=evaluate_opportunities
        ) as evaluate, patch(
            "src.api.service.build_investigation_portfolio",
            wraps=build_investigation_portfolio,
        ) as investigate, patch(
            "src.api.service.build_decision_readiness_portfolio",
            wraps=build_decision_readiness_portfolio,
        ) as decide:
            response = self.ask("Why is this not ready?", self.not_ready.decision_id)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.context_builder.call_count, 1)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(investigate.call_count, 1)
        self.assertEqual(decide.call_count, 1)
        for evidence_ref in response.json()["answer"]["evidence_refs"]:
            self.assertIn(evidence_ref, self.context.evidence_by_id)
            self.assertFalse(evidence_ref.startswith((
                "decision-readiness:", "opportunity:", "investigation-plan:",
                "investigation-task:", "requirement:", "evidence-gap:",
            )))
        self.provider_factory.assert_not_called()

    def test_direct_answers_pass_existing_validator_and_classifier_is_deterministic(self) -> None:
        cases = {
            "What does this decision mean?": DecisionQuestionIntent.DECISION_OVERVIEW,
            "Why is this ready for review?": DecisionQuestionIntent.DECISION_WHY_READY,
            "Why is this not ready?": DecisionQuestionIntent.DECISION_WHY_NOT_READY,
            "What evidence is missing?": DecisionQuestionIntent.DECISION_MISSING_EVIDENCE,
            "What could raise readiness?": DecisionQuestionIntent.DECISION_NEXT_EVIDENCE,
            "What is the decision boundary?": DecisionQuestionIntent.DECISION_BOUNDARY,
            "Why is this HIGH priority but not ready?": DecisionQuestionIntent.DECISION_PRIORITY_VS_READINESS,
            "Does ready mean Pulse recommends the change?": DecisionQuestionIntent.DECISION_HUMAN_REVIEW,
        }
        opportunity, plan = self._objects(self.not_ready)
        for question, intent in cases.items():
            with self.subTest(question=question):
                self.assertIs(classify_decision_question(question), intent)
                answer = answer_decision_question(
                    question,
                    self.not_ready,
                    opportunity,
                    plan,
                    self.decisions,
                    self.investigations,
                    self.context,
                )
                self.assertIs(validate_answer(answer, question, self.context), answer)


class DecisionUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
        cls.styles = (STATIC_DIR / "styles.css").read_text(encoding="utf-8")

    def test_initial_load_fetches_decisions_once_without_detail_n_plus_one(self) -> None:
        self.assertEqual(self.javascript.count("fetch(`${DECISIONS_ENDPOINT}"), 1)
        self.assertIn("Promise.all", self.javascript)
        self.assertNotIn("fetch(`${DECISIONS_ENDPOINT}/${", self.javascript)

    def test_summary_and_first_reviewable_use_api_fields_verbatim(self) -> None:
        for value in (
            "decisions.ready_for_human_review_count",
            "decisions.needs_more_evidence_count",
            "decisions.blocked_by_boundary_count",
            "decisions.first_reviewable_decision_id",
        ):
            self.assertIn(value, self.javascript)
        self.assertIn("first.decision_question", self.javascript)
        self.assertNotIn("Best decision", self.javascript)
        self.assertNotIn("Recommended decision", self.javascript)

    def test_cards_preserve_order_labels_boundary_and_human_agency(self) -> None:
        self.assertIn("for (const assessment of decisions.assessments)", self.javascript)
        for label in (
            "READY FOR HUMAN REVIEW", "NEEDS MORE EVIDENCE", "BLOCKED BY BOUNDARY",
            "Investigation direction", "Business change consideration",
            "Measurement governance", "Financial decision",
            "Decision boundary", "Human review required", "Autonomous action allowed",
        ):
            self.assertIn(label, self.javascript)
        self.assertIn("assessment.decision_boundary", self.javascript)
        self.assertIn(
            'const humanReviewValue = assessment.human_review_required ? "Yes" : "No";',
            self.javascript,
        )
        self.assertIn(
            'const autonomousActionValue = assessment.autonomous_action_allowed ? "Yes" : "No";',
            self.javascript,
        )
        self.assertIn(
            "`Human review required: ${humanReviewValue} · "
            "Autonomous action allowed: ${autonomousActionValue}`",
            self.javascript,
        )
        self.assertIn("decision-boundary", self.styles)

    def test_associations_use_stable_ids_with_safe_degraded_state(self) -> None:
        self.assertIn("opportunitiesById.get(assessment.originating_opportunity_id)", self.javascript)
        self.assertIn("plansById.get(assessment.investigation_plan_id)", self.javascript)
        self.assertIn("Originating opportunity is unavailable", self.javascript)
        self.assertIn("Plan unavailable in current product payload", self.javascript)
        self.assertNotIn("title === assessment", self.javascript)

    def test_opportunity_summary_aggregates_assessments_without_reclassifying(self) -> None:
        self.assertIn("assessment.originating_opportunity_id", self.javascript)
        self.assertIn("renderOpportunityDecisionSummary", self.javascript)
        self.assertIn("Decision readiness", self.javascript)
        self.assertNotIn("assessment.priority", self.javascript)

    def test_decision_actions_send_decision_id_only_and_reuse_answer_area(self) -> None:
        self.assertIn("requestBody.decision_id = decisionId", self.javascript)
        self.assertIn("else if (investigationTaskId)", self.javascript)
        self.assertIn("else if (opportunityId)", self.javascript)
        self.assertIn("Answer about decision:", self.javascript)
        self.assertIn("data", self.javascript)
        self.assertNotIn("/api/v1/analyst/decisions/${", self.javascript)

    def test_existing_ask_paths_persistence_and_responsive_safety_remain(self) -> None:
        self.assertIn("requestBody.investigation_task_id", self.javascript)
        self.assertIn("requestBody.opportunity_id", self.javascript)
        self.assertIn("askPulse(questionInput.value)", self.javascript)
        self.assertNotIn("localStorage", self.javascript)
        self.assertNotIn("sessionStorage", self.javascript)
        self.assertIn("<details class=\"metadata-card\">", self.html)
        self.assertIn("overflow-wrap: anywhere", self.styles)
        self.assertIn("@media (max-width: 760px)", self.styles)
        for forbidden_button in (
            ">Pause campaign<", ">Change budget<", ">Apply recommendation<",
            ">Approve change<", ">Execute<", ">Trigger investigation<",
        ):
            self.assertNotIn(forbidden_button, self.html)
            self.assertNotIn(forbidden_button, self.javascript)

    def test_ui_has_no_hardcoded_real_readiness_counts_or_metrics(self) -> None:
        for forbidden in (
            "2 READY_FOR_HUMAN_REVIEW", "4 NEEDS_MORE_EVIDENCE",
            "84.48", "97.71", "267", "951",
        ):
            self.assertNotIn(forbidden, self.javascript)
            self.assertNotIn(forbidden, self.html)


if __name__ == "__main__":
    unittest.main()
