"""Phase 6.7D investigation API, deterministic answers, and UI contracts."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import STATIC_DIR, create_app
from src.api.service import AnalystService
from src.intelligence.investigation_answers import (
    InvestigationQuestionIntent,
    answer_investigation_question,
    classify_investigation_question,
)
from src.intelligence.investigation_models import (
    InvestigationReadiness,
    InvestigationValidationError,
)
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.narration import validate_answer
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.providers import OfflineFakeProvider
from tests.test_opportunities import BUSINESS, synthetic_context


class InvestigationApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.portfolio = build_investigation_portfolio(self.context, self.evaluation)
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.context_builder = MagicMock(return_value=self.context)
        self.service = AnalystService(
            context_builder=self.context_builder,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )
        self.client = TestClient(create_app(self.service))
        self.plan = self.portfolio.plans[0]
        self.ready_task = next(
            task for plan in self.portfolio.plans for task in plan.tasks
            if task.readiness is InvestigationReadiness.READY_NOW
        )
        self.blocked_task = next(
            task for plan in self.portfolio.plans for task in plan.tasks
            if task.readiness is InvestigationReadiness.BLOCKED_MISSING_EVIDENCE
        )

    def test_portfolio_returns_engine_contract_for_active_opportunities(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/investigations", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        expected = self.portfolio.to_dict()
        self.assertEqual(payload, expected)
        self.assertEqual(
            [plan["opportunity_id"] for plan in payload["plans"]],
            [item.opportunity_id for item in self.evaluation.opportunities],
        )
        self.assertEqual(payload["recommended_start_task_id"], expected["recommended_start_task_id"])
        self.assertEqual(
            (payload["ready_task_count"], payload["partial_task_count"], payload["blocked_task_count"]),
            (expected["ready_task_count"], expected["partial_task_count"], expected["blocked_task_count"]),
        )
        self.assertNotIn("suppressed_candidates", response.text)
        self.provider_factory.assert_not_called()

    def test_suppressed_efficiency_candidate_has_no_plan(self) -> None:
        context = synthetic_context(acquisition_band="LOW")
        evaluation = evaluate_opportunities(context)
        client = TestClient(create_app(AnalystService(
            context_builder=lambda _business_id: context,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )))
        response = client.get(
            "/api/v1/analyst/investigations", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        active_ids = {item.opportunity_id for item in evaluation.opportunities}
        self.assertEqual(
            {item["opportunity_id"] for item in response.json()["plans"]},
            active_ids,
        )
        self.assertNotIn("acquisition_efficiency_review", " ".join(active_ids))

    def test_plan_detail_known_unknown_invalid_and_cross_business_are_safe(self) -> None:
        known = self.client.get(
            f"/api/v1/analyst/investigations/{self.plan.plan_id}",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(known.status_code, 200, known.text)
        self.assertEqual(known.json()["plan"], self.plan.to_dict())
        unknown = self.client.get(
            "/api/v1/analyst/investigations/investigation-plan:confirmation_leakage:missing",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.json()["error"]["code"], "INVESTIGATION_PLAN_NOT_FOUND")
        invalid = self.client.get(
            "/api/v1/analyst/investigations/not-a-plan",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(invalid.json()["error"]["code"], "INVALID_INVESTIGATION_ID")
        mismatch = self.client.get(
            f"/api/v1/analyst/investigation-tasks/{self.ready_task.task_id}",
            params={"business_id": "different_business"},
        )
        self.assertEqual(mismatch.status_code, 404)
        self.assertIn(
            mismatch.json()["error"]["code"],
            {"BUSINESS_NOT_FOUND", "INVESTIGATION_TASK_NOT_FOUND"},
        )
        mismatch = self.client.get(
            f"/api/v1/analyst/investigations/{self.plan.plan_id}",
            params={"business_id": "different_business"},
        )
        self.assertEqual(mismatch.status_code, 404)
        self.assertIn(
            mismatch.json()["error"]["code"],
            {"BUSINESS_NOT_FOUND", "INVESTIGATION_PLAN_NOT_FOUND"},
        )

    def test_task_detail_known_unknown_and_invalid_are_safe(self) -> None:
        known = self.client.get(
            f"/api/v1/analyst/investigation-tasks/{self.ready_task.task_id}",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(known.status_code, 200, known.text)
        self.assertEqual(known.json()["task"], self.ready_task.to_dict())
        unknown = self.client.get(
            "/api/v1/analyst/investigation-tasks/investigation-task:confirmation_leakage:missing:task",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.json()["error"]["code"], "INVESTIGATION_TASK_NOT_FOUND")
        invalid = self.client.get(
            "/api/v1/analyst/investigation-tasks/not-a-task",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(invalid.json()["error"]["code"], "INVALID_INVESTIGATION_ID")

    def test_endpoints_build_context_evaluation_and_portfolio_once(self) -> None:
        with patch(
            "src.api.service.evaluate_opportunities",
            wraps=evaluate_opportunities,
        ) as evaluate, patch(
            "src.api.service.build_investigation_portfolio",
            wraps=build_investigation_portfolio,
        ) as build:
            response = self.client.get(
                "/api/v1/analyst/investigations", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.context_builder.call_count, 1)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(build.call_count, 1)

    def test_planner_validation_failure_has_controlled_error(self) -> None:
        with patch(
            "src.api.service.build_investigation_portfolio",
            side_effect=InvestigationValidationError("private planner detail"),
        ):
            response = self.client.get(
                "/api/v1/analyst/investigations", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["error"]["code"], "INVESTIGATION_VALIDATION_FAILED"
        )
        self.assertNotIn("private planner detail", response.text)

    def test_portfolio_response_excludes_private_raw_and_unsafe_fields(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/investigations", params={"business_id": BUSINESS}
        )
        lowered = response.text.lower()
        for forbidden in (
            "suppressed_candidates", "raw_context", "source_relation", "select ",
            "customer_id", "order_id", "lead_id", "phone_number", "email_address",
            "tracking_number", "gap_score", "roi", "estimated_value",
        ):
            self.assertNotIn(forbidden, lowered)


class InvestigationAskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = synthetic_context()
        self.evaluation = evaluate_opportunities(self.context)
        self.portfolio = build_investigation_portfolio(self.context, self.evaluation)
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.context_builder = MagicMock(return_value=self.context)
        self.client = TestClient(create_app(AnalystService(
            context_builder=self.context_builder,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )))
        self.ready_task = next(
            task for plan in self.portfolio.plans for task in plan.tasks
            if task.readiness is InvestigationReadiness.READY_NOW
        )
        self.ready_plan = next(plan for plan in self.portfolio.plans if self.ready_task in plan.tasks)
        self.blocked_task = next(
            task for plan in self.portfolio.plans for task in plan.tasks
            if task.readiness is InvestigationReadiness.BLOCKED_MISSING_EVIDENCE
        )
        self.blocked_plan = next(plan for plan in self.portfolio.plans if self.blocked_task in plan.tasks)

    def ask(self, question: str, task_id: str | None = None):
        body = {"business_id": BUSINESS, "question": question}
        if task_id is not None:
            body["investigation_task_id"] = task_id
        return self.client.post("/api/v1/analyst/ask", json=body)

    def test_ready_task_ask_is_deterministic_grounded_and_provider_free(self) -> None:
        response = self.ask("Why can I investigate this now?", self.ready_task.task_id)
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["question_category"], "INVESTIGATION_WHY_READY")
        self.assertIn("current validated aggregate evidence", payload["answer"]["answer_summary"])
        self.assertEqual(payload["meta"]["provider"], "deterministic")
        self.assertEqual(payload["meta"]["model"], "investigation-engine-v1")
        self.assertEqual(payload["meta"]["answer_source"], "deterministic_investigation")
        self.assertEqual(payload["meta"]["provider_call_count"], 0)
        self.assertFalse(payload["meta"]["repair_attempted"])
        self.assertFalse(payload["meta"]["deterministic_fallback_used"])
        self.assertTrue(set(payload["answer"]["evidence_refs"]).issubset(self.context.evidence_by_id))
        self.provider_factory.assert_not_called()

    def test_blocked_and_missing_answers_use_existing_requirement_content(self) -> None:
        for question, category in (
            ("Why is this blocked?", "INVESTIGATION_WHY_BLOCKED"),
            ("What data is missing?", "INVESTIGATION_MISSING_EVIDENCE"),
        ):
            with self.subTest(question=question):
                response = self.ask(question, self.blocked_task.task_id)
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["question_category"], category)
                self.assertIn("current validated intelligence context", response.text)
                requirements = {
                    item.requirement_id: item for item in self.blocked_plan.evidence_requirements
                }
                for requirement_id in self.blocked_task.missing_requirement_ids:
                    self.assertIn(requirements[requirement_id].name, response.text)
                    self.assertIn(requirements[requirement_id].description, response.text)
                for invented in ("connector", "api endpoint", "warehouse table", "data owner"):
                    self.assertNotIn(invented, response.text.lower())

    def test_expected_completion_confirm_refute_and_decision_use_plan_fields(self) -> None:
        cases = (
            ("What should this produce?", "INVESTIGATION_EXPECTED_OUTPUT", self.ready_task.expected_output),
            ("When is this task complete?", "INVESTIGATION_COMPLETION", self.ready_task.completion_criteria[0]),
            ("How does this confirm or refute the hypothesis?", "INVESTIGATION_CONFIRM_REFUTE", self.ready_task.strengthens_criteria[0]),
            ("What decision could this unlock?", "INVESTIGATION_DECISION", self.ready_plan.decision_unlocked),
        )
        for question, category, expected in cases:
            with self.subTest(question=question):
                response = self.ask(question, self.ready_task.task_id)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()["question_category"], category)
                self.assertIn(expected, response.text)

    def test_ambiguous_ids_are_rejected_and_modes_remain_separate(self) -> None:
        opportunity = self.evaluation.opportunities[0]
        ambiguous = self.client.post(
            "/api/v1/analyst/ask",
            json={
                "business_id": BUSINESS,
                "question": "Explain this.",
                "opportunity_id": opportunity.opportunity_id,
                "investigation_task_id": self.ready_task.task_id,
            },
        )
        self.assertEqual(ambiguous.status_code, 422)
        self.assertEqual(ambiguous.json()["error"]["code"], "INVALID_REQUEST")

        opportunity_response = self.client.post(
            "/api/v1/analyst/ask",
            json={
                "business_id": BUSINESS,
                "question": "Why is this an opportunity?",
                "opportunity_id": opportunity.opportunity_id,
            },
        )
        self.assertEqual(opportunity_response.status_code, 200)
        self.assertEqual(
            opportunity_response.json()["meta"]["answer_source"],
            "deterministic_opportunity",
        )

    def test_unknown_task_cannot_generate_an_answer(self) -> None:
        response = self.ask(
            "Why is this blocked?",
            "investigation-task:confirmation_leakage:missing:task",
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            response.json()["error"]["code"], "INVESTIGATION_TASK_NOT_FOUND"
        )
        self.provider_factory.assert_not_called()

    def test_ordinary_ask_path_still_uses_existing_provider(self) -> None:
        provider = OfflineFakeProvider()
        client = TestClient(create_app(AnalystService(
            context_builder=lambda _business_id: self.context,
            provider_factory=lambda _name: provider,
            warehouse_checker=lambda: True,
        )))
        response = client.post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": "Why are returns high?"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["meta"]["answer_source"], "grounded_analyst")
        self.assertEqual(response.json()["meta"]["model"], provider.model)

    def test_task_ask_builds_once_and_never_uses_object_ids_as_evidence(self) -> None:
        self.context_builder.reset_mock()
        with patch(
            "src.api.service.evaluate_opportunities",
            wraps=evaluate_opportunities,
        ) as evaluate, patch(
            "src.api.service.build_investigation_portfolio",
            wraps=build_investigation_portfolio,
        ) as build:
            response = self.ask("Why is this blocked?", self.blocked_task.task_id)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.context_builder.call_count, 1)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(build.call_count, 1)
        for evidence_ref in response.json()["answer"]["evidence_refs"]:
            self.assertFalse(evidence_ref.startswith((
                "opportunity:", "investigation-task:", "investigation-plan:",
                "requirement:", "evidence-gap:",
            )))
        self.provider_factory.assert_not_called()

    def test_direct_answer_passes_existing_validator_for_ready_and_blocked_tasks(self) -> None:
        for question, task, plan in (
            ("Why can I investigate this now?", self.ready_task, self.ready_plan),
            ("Why is this blocked?", self.blocked_task, self.blocked_plan),
        ):
            with self.subTest(question=question):
                answer = answer_investigation_question(
                    question, task, plan, self.portfolio, self.context
                )
                self.assertIs(validate_answer(answer, question, self.context), answer)

    def test_intent_classifier_is_deterministic(self) -> None:
        cases = {
            "What is this investigation?": InvestigationQuestionIntent.INVESTIGATION_OVERVIEW,
            "Why can I do this now?": InvestigationQuestionIntent.INVESTIGATION_WHY_READY,
            "Why is this blocked?": InvestigationQuestionIntent.INVESTIGATION_WHY_BLOCKED,
            "What data is missing?": InvestigationQuestionIntent.INVESTIGATION_MISSING_EVIDENCE,
            "What should this produce?": InvestigationQuestionIntent.INVESTIGATION_EXPECTED_OUTPUT,
            "When is this complete?": InvestigationQuestionIntent.INVESTIGATION_COMPLETION,
            "How does this confirm or refute it?": InvestigationQuestionIntent.INVESTIGATION_CONFIRM_REFUTE,
            "What decision could this unlock?": InvestigationQuestionIntent.INVESTIGATION_DECISION,
        }
        for question, expected in cases.items():
            self.assertIs(classify_investigation_question(question), expected)


class InvestigationUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
        cls.styles = (STATIC_DIR / "styles.css").read_text(encoding="utf-8")

    def test_initial_load_fetches_portfolio_once_without_detail_n_plus_one(self) -> None:
        self.assertEqual(self.javascript.count("fetch(`${INVESTIGATIONS_ENDPOINT}"), 1)
        self.assertIn("Promise.all", self.javascript)
        self.assertNotIn("/investigation-tasks/", self.javascript)
        self.assertNotIn("/investigations/${", self.javascript)

    def test_plans_are_joined_by_opportunity_id_with_safe_unavailable_state(self) -> None:
        self.assertIn("plansByOpportunityId.get(opportunity.opportunity_id)", self.javascript)
        self.assertIn("Investigation plan unavailable.", self.javascript)
        self.assertNotIn("title === plan", self.javascript)

    def test_portfolio_and_task_readiness_are_visible_from_api_fields(self) -> None:
        for value in (
            "portfolio.ready_task_count", "portfolio.partial_task_count",
            "portfolio.blocked_task_count", "portfolio.recommended_start_task_id",
            "portfolio.evidence_gaps", "plan.status", "task.readiness",
            "READY NOW", "PARTIALLY READY", "MISSING EVIDENCE", "BLOCKED BY BOUNDARY",
            "Not available in current validated context", "Used by",
        ):
            self.assertIn(value, self.javascript)
        for element_id in (
            "investigation-portfolio", "investigation-counts",
            "recommended-investigation", "evidence-gaps", "evidence-gap-list",
        ):
            self.assertIn(f'id="{element_id}"', self.html)

    def test_task_actions_send_only_investigation_task_context(self) -> None:
        action_start = self.javascript.index("function investigationAction")
        action_end = self.javascript.index("function renderInvestigationTask")
        action = self.javascript[action_start:action_end]
        self.assertIn('askPulse(question, null, "", task.task_id, task.title)', action)
        self.assertIn("requestBody.investigation_task_id = investigationTaskId", self.javascript)
        self.assertIn("else if (opportunityId) requestBody.opportunity_id = opportunityId", self.javascript)
        self.assertIn("Answer about investigation:", self.javascript)

    def test_ordinary_and_opportunity_ask_paths_and_safety_remain_present(self) -> None:
        self.assertIn("askPulse(questionInput.value)", self.javascript)
        self.assertIn("askPulse(question, opportunity.opportunity_id", self.javascript)
        self.assertIn('<details class="metadata-card">', self.html)
        combined = self.html + self.javascript
        for forbidden in (
            "localStorage", "sessionStorage", "OPENAI_API_KEY", "Authorization",
            "provider:", "model:", "267", "951", "84.48", "97.71",
        ):
            self.assertNotIn(forbidden, combined)
        self.assertIn("overflow-wrap: anywhere", self.styles)


if __name__ == "__main__":
    unittest.main()
