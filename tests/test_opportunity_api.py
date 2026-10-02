"""Phase 6.7B opportunity API, deterministic answers, and UI contracts."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import STATIC_DIR, create_app
from src.api.service import AnalystService
from src.intelligence.context import PostgresEvidenceRepository
from src.intelligence.narration import validate_answer
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_answers import (
    OpportunityQuestionIntent,
    answer_opportunity_question,
    classify_opportunity_question,
)
from src.intelligence.opportunity_models import OpportunityValidationError
from src.intelligence.providers import OfflineFakeProvider
from tests.test_grounded_analyst import BUSINESS, context


class OpportunityApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = context()
        self.evaluation = evaluate_opportunities(self.context)
        self.assertTrue(self.evaluation.opportunities)
        self.opportunity = self.evaluation.opportunities[0]
        self.provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        self.service = AnalystService(
            context_builder=lambda business_id: self.context,
            provider_factory=self.provider_factory,
            warehouse_checker=lambda: True,
        )
        self.client = TestClient(create_app(self.service))

    def test_list_returns_only_ordered_active_safe_opportunities(self) -> None:
        response = self.client.get(
            "/api/v1/analyst/opportunities", params={"business_id": BUSINESS}
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        expected = list(self.evaluation.opportunities)
        self.assertEqual(payload["count"], len(expected))
        self.assertEqual(
            [item["opportunity_id"] for item in payload["opportunities"]],
            [item.opportunity_id for item in expected],
        )
        self.assertEqual(
            [item["opportunity_order"] for item in payload["opportunities"]],
            [item.opportunity_order for item in expected],
        )
        self.assertNotIn("suppressed_candidates", response.text)
        self.assertNotIn("scope_id", response.text)
        self.assertNotIn("business_id", payload["opportunities"][0])
        self.assertNotIn("causal_claim", response.text)
        known = self.context.evidence_by_id
        for item in payload["opportunities"]:
            refs = (
                item["supporting_evidence_refs"]
                + item["counter_evidence_refs"]
                + item["blocking_evidence_refs"]
            )
            self.assertTrue(all(ref in known for ref in refs))
        self.provider_factory.assert_not_called()

    def test_known_detail_is_active_and_unknown_is_controlled(self) -> None:
        response = self.client.get(
            f"/api/v1/analyst/opportunities/{self.opportunity.opportunity_id}",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(
            response.json()["opportunity"]["opportunity_id"],
            self.opportunity.opportunity_id,
        )
        unknown = self.client.get(
            "/api/v1/analyst/opportunities/opportunity:confirmation_leakage:missing",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(unknown.json()["error"]["code"], "OPPORTUNITY_NOT_FOUND")
        self.provider_factory.assert_not_called()

    def test_invalid_id_and_business_mismatch_are_controlled(self) -> None:
        invalid = self.client.get(
            "/api/v1/analyst/opportunities/not-an-opportunity",
            params={"business_id": BUSINESS},
        )
        self.assertEqual(invalid.status_code, 422)
        self.assertEqual(invalid.json()["error"]["code"], "INVALID_OPPORTUNITY_ID")

        mismatch = self.client.get(
            f"/api/v1/analyst/opportunities/{self.opportunity.opportunity_id}",
            params={"business_id": "another_business"},
        )
        self.assertEqual(mismatch.status_code, 404)
        self.assertIn(
            mismatch.json()["error"]["code"],
            {"BUSINESS_NOT_FOUND", "OPPORTUNITY_NOT_FOUND"},
        )

    def test_opportunity_ask_is_provider_free_grounded_and_validated(self) -> None:
        response = self.client.post(
            "/api/v1/analyst/ask",
            json={
                "business_id": BUSINESS,
                "question": "Why is this an opportunity?",
                "opportunity_id": self.opportunity.opportunity_id,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["question_category"], "OPPORTUNITY_WHY")
        self.assertEqual(payload["meta"]["provider"], "deterministic")
        self.assertEqual(payload["meta"]["model"], "opportunity-engine-v1")
        self.assertEqual(payload["meta"]["answer_source"], "deterministic_opportunity")
        self.assertEqual(payload["meta"]["provider_call_count"], 0)
        self.assertFalse(payload["meta"]["repair_attempted"])
        self.assertFalse(payload["meta"]["deterministic_fallback_used"])
        self.assertNotIn(self.opportunity.opportunity_id, payload["answer"]["evidence_refs"])
        allowed_refs = set(
            self.opportunity.supporting_evidence_refs
            + self.opportunity.counter_evidence_refs
            + self.opportunity.blocking_evidence_refs
        )
        self.assertTrue(set(payload["answer"]["evidence_refs"]).issubset(allowed_refs))
        self.provider_factory.assert_not_called()

    def test_each_opportunity_intent_uses_engine_owned_content(self) -> None:
        cases = (
            ("What would confirm this?", "confirmation_criteria", "OPPORTUNITY_CONFIRM"),
            ("What would refute this?", "refutation_criteria", "OPPORTUNITY_REFUTE"),
            ("What should I investigate?", "investigation_steps", "OPPORTUNITY_INVESTIGATE"),
            ("What decision could this unlock?", "decision_unlocked", "OPPORTUNITY_DECISION"),
        )
        for question, field, category in cases:
            with self.subTest(question=question):
                response = self.client.post(
                    "/api/v1/analyst/ask",
                    json={
                        "business_id": BUSINESS,
                        "question": question,
                        "opportunity_id": self.opportunity.opportunity_id,
                    },
                )
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["question_category"], category)
                answer_text = response.text
                value = getattr(self.opportunity, field)
                if isinstance(value, tuple):
                    self.assertTrue(all(item in answer_text for item in value))
                else:
                    self.assertIn(value, answer_text)
                self.assertNotIn("you should", answer_text.lower())

    def test_direct_builder_passes_existing_answer_validator(self) -> None:
        question = "What would confirm this hypothesis?"
        answer = answer_opportunity_question(
            question, self.opportunity, self.context
        )
        self.assertIs(validate_answer(answer, question, self.context), answer)

    def test_unknown_opportunity_cannot_generate_an_answer(self) -> None:
        response = self.client.post(
            "/api/v1/analyst/ask",
            json={
                "business_id": BUSINESS,
                "question": "Why is this an opportunity?",
                "opportunity_id": "opportunity:confirmation_leakage:missing",
            },
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "OPPORTUNITY_NOT_FOUND")
        self.provider_factory.assert_not_called()

    def test_engine_validation_failure_is_controlled(self) -> None:
        with patch(
            "src.api.service.evaluate_opportunities",
            side_effect=OpportunityValidationError("private engine detail"),
        ):
            response = self.client.get(
                "/api/v1/analyst/opportunities", params={"business_id": BUSINESS}
            )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["error"]["code"], "OPPORTUNITY_VALIDATION_FAILED"
        )
        self.assertNotIn("private engine detail", response.text)
        self.provider_factory.assert_not_called()

    def test_opportunity_ask_builds_and_evaluates_exactly_once(self) -> None:
        context_builder = MagicMock(return_value=self.context)
        evaluator = MagicMock(return_value=self.evaluation)
        provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        service = AnalystService(
            context_builder=context_builder,
            provider_factory=provider_factory,
            warehouse_checker=lambda: True,
        )
        with patch("src.api.service.evaluate_opportunities", evaluator):
            response = TestClient(create_app(service)).post(
                "/api/v1/analyst/ask",
                json={
                    "business_id": BUSINESS,
                    "question": "Why is this an opportunity?",
                    "opportunity_id": self.opportunity.opportunity_id,
                },
            )
        self.assertEqual(response.status_code, 200, response.text)
        context_builder.assert_called_once_with(BUSINESS)
        evaluator.assert_called_once_with(self.context)
        provider_factory.assert_not_called()
        self.assertEqual(response.json()["meta"]["provider_call_count"], 0)

    def test_ordinary_ask_does_not_evaluate_opportunities(self) -> None:
        context_builder = MagicMock(return_value=self.context)
        evaluator = MagicMock(side_effect=AssertionError("evaluator called"))
        provider_factory = MagicMock(return_value=OfflineFakeProvider())
        service = AnalystService(
            context_builder=context_builder,
            provider_factory=provider_factory,
            warehouse_checker=lambda: True,
        )
        with patch("src.api.service.evaluate_opportunities", evaluator):
            response = TestClient(create_app(service)).post(
                "/api/v1/analyst/ask",
                json={
                    "business_id": BUSINESS,
                    "question": "What should I investigate first?",
                },
            )
        self.assertEqual(response.status_code, 200, response.text)
        context_builder.assert_called_once_with(BUSINESS)
        evaluator.assert_not_called()
        provider_factory.assert_called_once_with("fake")
        self.assertEqual(response.json()["meta"]["provider_call_count"], 1)
        self.assertEqual(response.json()["meta"]["answer_source"], "grounded_analyst")

    def _captured_connection_kwargs(self, settings: dict) -> dict:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.return_value.fetchall.return_value = []
        connect = MagicMock(return_value=connection)
        repository = PostgresEvidenceRepository(connect=connect)
        with patch("src.intelligence.context.connection_kwargs", return_value=settings):
            repository.fetch_as_of(BUSINESS)
        return connect.call_args.kwargs

    def test_local_context_connection_uses_ipv4_and_preserves_config(self) -> None:
        settings = {
            "dbname": "pulse_analytics",
            "user": "pulse",
            "password": "local-test",
            "host": "localhost",
            "port": 5433,
            "sslmode": "prefer",
        }
        kwargs = self._captured_connection_kwargs(settings)
        self.assertEqual(kwargs["host"], "localhost")
        self.assertEqual(kwargs["hostaddr"], "127.0.0.1")
        for key, value in settings.items():
            self.assertEqual(kwargs[key], value)

    def test_explicit_ipv4_context_host_is_not_rewritten(self) -> None:
        settings = {
            "dbname": "pulse_analytics",
            "user": "pulse",
            "password": "local-test",
            "host": "127.0.0.1",
            "port": 5433,
        }
        kwargs = self._captured_connection_kwargs(settings)
        self.assertNotIn("hostaddr", kwargs)
        for key, value in settings.items():
            self.assertEqual(kwargs[key], value)

    def test_non_localhost_context_host_is_untouched(self) -> None:
        settings = {
            "dbname": "pulse_analytics",
            "user": "pulse",
            "password": "local-test",
            "host": "warehouse-postgres",
            "port": 5432,
            "sslmode": "require",
        }
        kwargs = self._captured_connection_kwargs(settings)
        self.assertNotIn("hostaddr", kwargs)
        for key, value in settings.items():
            self.assertEqual(kwargs[key], value)

    def test_question_classifier_is_deterministic(self) -> None:
        expected = {
            "What is this opportunity?": OpportunityQuestionIntent.OPPORTUNITY_OVERVIEW,
            "Why is this an opportunity?": OpportunityQuestionIntent.OPPORTUNITY_WHY,
            "What would confirm this?": OpportunityQuestionIntent.OPPORTUNITY_CONFIRM,
            "What would refute this?": OpportunityQuestionIntent.OPPORTUNITY_REFUTE,
            "What should I investigate?": OpportunityQuestionIntent.OPPORTUNITY_INVESTIGATE,
            "What decision could this unlock?": OpportunityQuestionIntent.OPPORTUNITY_DECISION,
        }
        for question, intent in expected.items():
            self.assertIs(classify_opportunity_question(question), intent)


class OpportunityUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (STATIC_DIR / "app.js").read_text(encoding="utf-8")
        cls.styles = (STATIC_DIR / "styles.css").read_text(encoding="utf-8")

    def test_opportunity_cards_are_api_driven_and_engine_ordered(self) -> None:
        self.assertIn('id="opportunities-section"', self.html)
        self.assertIn("/api/v1/analyst/opportunities", self.javascript)
        self.assertIn("opportunity.opportunity_order", self.javascript)
        self.assertNotIn(".sort(", self.javascript)
        self.assertNotIn("suppressed_candidates", self.javascript)

    def test_cards_have_badges_details_and_opportunity_actions(self) -> None:
        for value in (
            "opportunity.priority", "opportunity.confidence", "Hypothesis to test",
            "Investigation steps", "What would confirm it", "What would refute it",
            "Decision this evidence could unlock", "Missing evidence", "Limitation",
            "supporting_evidence_refs", "counter_evidence_refs", "blocking_evidence_refs",
        ):
            self.assertIn(value, self.javascript)
        self.assertIn("requestBody.opportunity_id = opportunityId", self.javascript)
        self.assertIn("Why this?", self.javascript)
        self.assertIn("white-space: nowrap", self.styles)
        self.assertIn("flex-shrink: 0", self.styles)

    def test_opportunity_action_posts_without_detail_refetch(self) -> None:
        action_start = self.javascript.index("function opportunityAction")
        action_end = self.javascript.index("function renderOpportunityCard")
        action_source = self.javascript[action_start:action_end]
        self.assertIn("askPulse(question, opportunity.opportunity_id", action_source)
        self.assertNotIn("fetch(", action_source)

    def test_ui_has_no_hardcoded_pilot_metrics_or_persistent_history(self) -> None:
        combined = self.html + self.javascript
        for forbidden in ("267", "951", "84.48", "97.71", "localStorage", "sessionStorage"):
            self.assertNotIn(forbidden, combined)
        self.assertIn("overflow-wrap: anywhere", self.styles)
        self.assertIn("@media (max-width: 760px)", self.styles)
        self.assertIn('<details class="metadata-card">', self.html)


if __name__ == "__main__":
    unittest.main()
