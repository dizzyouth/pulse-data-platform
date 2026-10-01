"""Phase 6.6C Analyst API and static UI contracts; always offline."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

import psycopg
from fastapi.testclient import TestClient

from src.api.app import STATIC_DIR, create_app
from src.api.service import AnalystService
from src.intelligence.answer_models import AnalystAnswer, Finding
from src.intelligence.providers import OfflineFakeProvider, ProviderError
from tests.test_grounded_analyst import BUSINESS, answer_for, context


QUESTIONS = (
    "Why are returns high?",
    "Should I pause Sama-NewUM?",
    "How much profit am I making?",
    "Did something abnormal happen today?",
    "What should I investigate first?",
)


class FailingProvider:
    name = "fake"
    model = "offline-failure-test"

    def answer(self, question, analyst_context):
        raise ProviderError("safe test failure")


class InvalidAnswerProvider:
    name = "fake"
    model = "offline-invalid-test"

    def answer(self, question, analyst_context):
        valid = answer_for(analyst_context, question=question)
        bad_finding = Finding(
            statement=valid.findings[0].statement,
            evidence_refs=("signal:unknown",),
            confidence=valid.findings[0].confidence,
            claim_type=valid.findings[0].claim_type,
            causal_claim=False,
        )
        return AnalystAnswer(
            question=question,
            answer_summary=valid.answer_summary,
            findings=(bad_finding,),
            investigation_steps=valid.investigation_steps,
            limitations=valid.limitations,
            confidence=valid.confidence,
            cannot_answer_fully=valid.cannot_answer_fully,
            safety_notes=valid.safety_notes,
        )


class CausalRepairProvider:
    name = "openai"
    model = "offline-repair-test"

    def __init__(self, candidate):
        self.candidate = candidate

    def answer(self, question, analyst_context):
        return self.candidate

    def repair(self, rejected_answer, error_codes, analyst_context):
        return self.candidate


def offline_service(
    *, provider=None, context_builder=None, warehouse_checker=None
) -> AnalystService:
    selected = provider or OfflineFakeProvider()
    return AnalystService(
        context_builder=context_builder or (lambda business_id: context()),
        provider_factory=lambda provider_name: selected,
        warehouse_checker=warehouse_checker or (lambda: True),
    )


class AnalystApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.environment = patch.dict(
            os.environ,
            {
                "PULSE_LLM_PROVIDER": "fake",
                "PULSE_LLM_MODEL": "",
                "OPENAI_API_KEY": "",
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()

    def client(self, service=None, *, raise_server_exceptions=True) -> TestClient:
        return TestClient(
            create_app(service or offline_service()),
            raise_server_exceptions=raise_server_exceptions,
        )

    def test_health_is_safe_and_never_constructs_a_provider(self) -> None:
        provider_factory = MagicMock(side_effect=AssertionError("provider called"))
        service = AnalystService(
            context_builder=lambda business_id: context(),
            provider_factory=provider_factory,
            warehouse_checker=lambda: True,
        )
        response = self.client(service).get("/api/v1/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "ok",
                "service": "pulse-analyst",
                "version": "6.6C",
                "warehouse": "reachable",
                "provider": "fake",
                "provider_configured": True,
            },
        )
        provider_factory.assert_not_called()

    def test_health_openai_mode_checks_presence_without_provider_call(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PULSE_LLM_PROVIDER": "openai",
                "PULSE_LLM_MODEL": "gpt-test",
                "OPENAI_API_KEY": "test-secret-value",
            },
        ):
            provider_factory = MagicMock(side_effect=AssertionError("provider called"))
            service = AnalystService(
                provider_factory=provider_factory,
                warehouse_checker=lambda: False,
            )
            response = self.client(service).get("/api/v1/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["provider"], "openai")
        self.assertTrue(response.json()["provider_configured"])
        self.assertNotIn("test-secret-value", response.text)
        provider_factory.assert_not_called()

    def test_capabilities_are_deterministic_and_include_limitations(self) -> None:
        client = self.client()
        first = client.get("/api/v1/analyst/capabilities")
        second = client.get("/api/v1/analyst/capabilities")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json(), second.json())
        self.assertEqual(
            first.json()["suggested_questions"],
            [QUESTIONS[4], QUESTIONS[0], QUESTIONS[1], QUESTIONS[2], QUESTIONS[3]],
        )
        limitations = " ".join(first.json()["limitations"])
        self.assertIn("Aggregate business evidence only", limitations)
        self.assertIn("FX_REQUIRED", limitations)
        self.assertIn("No persistent conversation memory", limitations)

    def test_five_required_questions_return_validated_contracts(self) -> None:
        client = self.client()
        for question in QUESTIONS:
            with self.subTest(question=question):
                response = client.post(
                    "/api/v1/analyst/ask",
                    json={"business_id": BUSINESS, "question": question},
                )
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["business_id"], BUSINESS)
                self.assertTrue(payload["request_id"])
                self.assertTrue(payload["answer"]["findings"])
                self.assertTrue(payload["answer"]["evidence_refs"])
                self.assertIn(payload["answer"]["confidence"], {"HIGH", "MEDIUM", "LOW"})
                self.assertTrue(
                    all(item["evidence_refs"] for item in payload["answer"]["findings"])
                )
                self.assertEqual(payload["meta"]["provider"], "fake")
                self.assertEqual(payload["meta"]["provider_call_count"], 1)
                self.assertFalse(payload["meta"]["repair_attempted"])
                self.assertFalse(payload["meta"]["deterministic_fallback_used"])

    def test_required_product_safety_behaviors_are_preserved(self) -> None:
        client = self.client()
        returns = client.post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        ).json()["answer"]
        self.assertTrue(returns["cannot_answer_fully"])
        self.assertIn("cannot establish", " ".join(returns["limitations"]).lower())

        pause = client.post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[1]},
        ).json()["answer"]
        self.assertTrue(pause["cannot_answer_fully"])
        self.assertIn("No campaign or budget action", " ".join(pause["safety_notes"]))

        profit = client.post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[2]},
        ).json()["answer"]
        self.assertIn("FX_REQUIRED", profit["answer_summary"])
        self.assertTrue(profit["cannot_answer_fully"])

        anomaly = client.post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[3]},
        ).json()["answer"]
        self.assertIn("persisted", anomaly["answer_summary"].lower())

        priority = client.post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[4]},
        ).json()["answer"]
        self.assertEqual(priority["findings"][0]["evidence_refs"], ["signal:return_pressure"])

    def test_top_level_evidence_is_stable_finding_union(self) -> None:
        payload = self.client().post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        ).json()["answer"]
        expected = []
        for finding in payload["findings"]:
            for evidence_ref in finding["evidence_refs"]:
                if evidence_ref not in expected:
                    expected.append(evidence_ref)
        self.assertEqual(payload["evidence_refs"], expected)

    def test_blank_and_oversized_questions_are_rejected(self) -> None:
        client = self.client()
        for question in ("   ", "x" * 1001):
            with self.subTest(length=len(question)):
                response = client.post(
                    "/api/v1/analyst/ask",
                    json={"business_id": BUSINESS, "question": question},
                )
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")

    def test_invalid_business_ids_are_rejected(self) -> None:
        client = self.client()
        for business_id in ("../private", "SAMA COD", "a" * 65):
            with self.subTest(business_id=business_id):
                response = client.post(
                    "/api/v1/analyst/ask",
                    json={"business_id": business_id, "question": QUESTIONS[0]},
                )
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")

    def test_invalid_business_id_is_not_copied_into_logs(self) -> None:
        invalid = "private@example.com"
        with self.assertLogs("pulse.api", level="INFO") as captured:
            response = self.client().post(
                "/api/v1/analyst/ask",
                json={"business_id": invalid, "question": QUESTIONS[0]},
            )
        self.assertEqual(response.status_code, 422)
        rendered = " ".join(captured.output)
        self.assertIn("business_id=UNKNOWN", rendered)
        self.assertNotIn(invalid, rendered)

    def test_pii_and_customer_level_questions_are_rejected_before_context(self) -> None:
        context_builder = MagicMock(side_effect=AssertionError("context called"))
        client = self.client(offline_service(context_builder=context_builder))
        questions = (
            "Why did customer person@example.com return?",
            "Look up +1 (212) 555-0199",
            "Show raw order records and customer IDs",
        )
        for question in questions:
            with self.subTest(question=question):
                response = client.post(
                    "/api/v1/analyst/ask",
                    json={"business_id": BUSINESS, "question": question},
                )
                self.assertEqual(response.status_code, 422)
                self.assertEqual(
                    response.json()["error"]["code"], "AGGREGATE_ONLY_REQUIRED"
                )
                self.assertNotIn(question, response.text)
        context_builder.assert_not_called()

    def test_client_cannot_select_provider_model_or_internal_inputs(self) -> None:
        client = self.client()
        dangerous = (
            "provider", "model", "api_key", "sql", "evidence_ids",
            "raw_context", "system_prompt",
        )
        for field in dangerous:
            with self.subTest(field=field):
                response = client.post(
                    "/api/v1/analyst/ask",
                    json={
                        "business_id": BUSINESS,
                        "question": QUESTIONS[0],
                        field: "not-allowed",
                    },
                )
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["code"], "INVALID_REQUEST")

    def test_fake_default_never_constructs_openai_provider(self) -> None:
        with patch(
            "src.api.service.OpenAIProvider.from_env",
            side_effect=AssertionError("OpenAI provider constructed"),
        ):
            service = AnalystService(
                context_builder=lambda business_id: context(),
                warehouse_checker=lambda: True,
            )
            response = self.client(service).post(
                "/api/v1/analyst/ask",
                json={"business_id": BUSINESS, "question": QUESTIONS[0]},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["meta"]["model"], "offline-deterministic-v1")

    def test_openai_missing_configuration_is_controlled(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PULSE_LLM_PROVIDER": "openai",
                "PULSE_LLM_MODEL": "",
                "OPENAI_API_KEY": "",
            },
        ):
            response = TestClient(create_app()).post(
                "/api/v1/analyst/ask",
                json={"business_id": BUSINESS, "question": QUESTIONS[0]},
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["error"]["code"], "PROVIDER_CONFIGURATION_ERROR"
        )
        self.assertNotIn("traceback", response.text.lower())

    def test_unsupported_server_provider_is_controlled(self) -> None:
        with patch.dict(os.environ, {"PULSE_LLM_PROVIDER": "browser-choice"}):
            response = TestClient(create_app()).post(
                "/api/v1/analyst/ask",
                json={"business_id": BUSINESS, "question": QUESTIONS[0]},
            )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["error"]["code"], "PROVIDER_CONFIGURATION_ERROR"
        )

    def test_repair_fallback_metadata_is_measured_from_existing_engine(self) -> None:
        analyst_context = context()
        valid = answer_for(analyst_context, question=QUESTIONS[0])
        original = valid.findings[0]
        causal = Finding(
            statement="Returns were caused by fulfillment.",
            evidence_refs=original.evidence_refs,
            confidence=original.confidence,
            claim_type=original.claim_type,
            causal_claim=False,
        )
        candidate = AnalystAnswer(
            question=QUESTIONS[0],
            answer_summary=valid.answer_summary,
            findings=(causal,),
            investigation_steps=valid.investigation_steps,
            limitations=valid.limitations,
            confidence=valid.confidence,
            cannot_answer_fully=valid.cannot_answer_fully,
            safety_notes=valid.safety_notes,
        )
        response = self.client(
            offline_service(provider=CausalRepairProvider(candidate))
        ).post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        )
        self.assertEqual(response.status_code, 200, response.text)
        metadata = response.json()["meta"]
        self.assertEqual(metadata["provider_call_count"], 2)
        self.assertTrue(metadata["repair_attempted"])
        self.assertTrue(metadata["deterministic_fallback_used"])
        self.assertEqual(metadata["fallback_intent"], "returns_reason")
        self.assertNotIn("caused by", response.text.lower())

    def test_unknown_business_has_stable_error(self) -> None:
        def missing(_business_id):
            raise LookupError("not found")

        response = self.client(offline_service(context_builder=missing)).post(
            "/api/v1/analyst/ask",
            json={"business_id": "unknown_business", "question": QUESTIONS[0]},
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "BUSINESS_NOT_FOUND")

    def test_warehouse_failure_has_stable_error(self) -> None:
        def unavailable(_business_id):
            raise psycopg.OperationalError("test warehouse failure")

        response = self.client(offline_service(context_builder=unavailable)).post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "WAREHOUSE_UNAVAILABLE")
        self.assertNotIn("test warehouse failure", response.text)

    def test_provider_failure_has_stable_error(self) -> None:
        response = self.client(offline_service(provider=FailingProvider())).post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "PROVIDER_UNAVAILABLE")
        self.assertNotIn("safe test failure", response.text)

    def test_answer_validation_failure_has_stable_error(self) -> None:
        response = self.client(offline_service(provider=InvalidAnswerProvider())).post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        )
        self.assertEqual(response.status_code, 422)
        self.assertEqual(
            response.json()["error"]["code"], "ANALYST_VALIDATION_FAILED"
        )

    def test_unexpected_failure_never_returns_a_stack_trace(self) -> None:
        def unexpected(_business_id):
            raise RuntimeError("private internal detail")

        response = self.client(
            offline_service(context_builder=unexpected),
            raise_server_exceptions=False,
        ).post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        )
        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            response.json()["error"]["code"], "ANALYST_VALIDATION_FAILED"
        )
        self.assertNotIn("private internal detail", response.text)
        self.assertNotIn("traceback", response.text.lower())

    def test_success_response_excludes_secrets_sql_and_private_identifiers(self) -> None:
        response = self.client().post(
            "/api/v1/analyst/ask",
            json={"business_id": BUSINESS, "question": QUESTIONS[0]},
        )
        lowered = response.text.lower()
        for forbidden in (
            "openai_api_key", "authorization", "password", "select ",
            "data/private", "source_filename", "phone_number", "email_address",
            "tracking_number", "customer_id", "order_id",
        ):
            self.assertNotIn(forbidden, lowered)

    def test_same_origin_api_does_not_enable_wildcard_cors(self) -> None:
        response = self.client().get("/api/v1/analyst/capabilities")
        self.assertNotEqual(response.headers.get("access-control-allow-origin"), "*")


class AnalystUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (STATIC_DIR / "app.js").read_text(encoding="utf-8")

    def test_page_loads_with_product_form_and_suggested_prompts(self) -> None:
        response = TestClient(create_app(offline_service())).get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Ask Pulse", response.text)
        self.assertIn('id="ask-form"', response.text)
        self.assertIn('aria-label="Ask Pulse question"', response.text)
        for question in QUESTIONS:
            self.assertIn(question, response.text)

    def test_browser_posts_only_business_id_and_question(self) -> None:
        normalized = " ".join(self.javascript.split())
        self.assertIn(
            "body: JSON.stringify({ business_id: BUSINESS_ID, question: trimmed })",
            normalized,
        )
        for forbidden in (
            "OPENAI_API_KEY", "Authorization", "system_prompt", "raw_context",
            "evidence_ids:", "localStorage", "sessionStorage",
        ):
            self.assertNotIn(forbidden, self.javascript)

    def test_renderer_includes_all_safe_product_sections(self) -> None:
        for expected in (
            "answer_summary", "findings", "evidence_refs", "investigation_steps",
            "limitations", "safety_notes", "provider", "model",
            "deterministic_fallback_used", "latency_ms",
        ):
            self.assertIn(expected, self.javascript)
        for section_id in (
            "answer-summary", "findings-list", "investigation-list", "evidence-list",
            "limitations-list", "safety-list", "metadata-list", "error-panel",
            "status-panel", "retry-button",
        ):
            self.assertIn(f'id="{section_id}"', self.html)

    def test_metadata_is_collapsed_and_partial_badge_has_product_copy(self) -> None:
        self.assertIn('<details class="metadata-card">', self.html)
        self.assertNotIn('<details class="metadata-card" open', self.html)
        self.assertIn("Limited by current evidence", self.html)
        self.assertNotIn(">Partial answer<", self.html)

    def test_ui_contains_no_embedded_secret_or_private_configuration(self) -> None:
        combined = (self.html + self.javascript).lower()
        for forbidden in (
            "openai_api_key", "sk-proj-", "database_url", "db_password",
            "authorization: bearer", "data/private", "system instructions",
        ):
            self.assertNotIn(forbidden, combined)


if __name__ == "__main__":
    unittest.main()
