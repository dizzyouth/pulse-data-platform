"""Phase 6.6B grounded analyst contracts; all ordinary tests are offline."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from src.intelligence.answer_models import (
    PROVIDER_ANSWER_JSON_SCHEMA,
    AnalystAnswer,
    AnswerValidationError,
    ClaimType,
    Confidence,
    Finding,
    ProviderAnswer,
)
from src.intelligence.cli import main
from src.intelligence.context import (
    ANOMALY_SQL, CAMPAIGN_SQL, ECONOMICS_SQL, LEAKAGE_SQL, SIGNAL_SQL,
    EvidenceItem, build_context,
)
from src.intelligence.narration import (
    REPAIR_INSTRUCTIONS,
    SYSTEM_INSTRUCTIONS,
    SafetyValidationError,
    ValidationErrorCode,
    answer_question,
    serialize_context,
    validate_answer,
    validate_repair_contract,
)
from src.intelligence.providers import (
    OfflineFakeProvider,
    OpenAIProvider,
    ProviderError,
    QuestionIntent,
    classify_question_intent,
)


BUSINESS = "sama_cod_pilot"


def _signal(order: int, signal_type: str, *, priority: str = "MEDIUM",
            scope_type: str = "BUSINESS", scope_name: str = BUSINESS,
            observed: float = 50, baseline: float = 25,
            summary: str = "Observed 50 vs 25.") -> dict:
    return {
        "signal_order": order,
        "signal_id": f"{BUSINESS}|{scope_type}|{signal_type}|{scope_name}",
        "business_id": BUSINESS,
        "as_of_date": date(2026, 7, 31),
        "scope_type": scope_type,
        "scope_id": scope_name,
        "scope_name": scope_name,
        "signal_type": signal_type,
        "signal_category": "FULFILLMENT",
        "priority": priority,
        "confidence": "HIGH",
        "metric_name": signal_type.lower(),
        "observed_value": observed,
        "baseline_value": baseline,
        "absolute_gap": observed - baseline,
        "relative_gap": (observed - baseline) / baseline if baseline else None,
        "sample_size": 682,
        "impact_order_count": observed,
        "evidence_summary": summary,
        "why_it_matters": "The observed difference needs attention.",
        "recommended_next_step": "Review the aggregate downstream evidence.",
        "limitation": "Current aggregate data cannot establish cause.",
        "causal_claim": False,
    }


class FakeRepository:
    def __init__(self) -> None:
        self.requested: list[str] = []
        self.signals = [
            _signal(1, "RETURN_PRESSURE", priority="HIGH", observed=390, baseline=286,
                    summary="390 returned vs 286 delivered; return rate 57.18%."),
            _signal(2, "CONFIRMATION_LEAKAGE", priority="HIGH", observed=250, baseline=0,
                    summary="250 matched orders were not observed as confirmed."),
            _signal(3, "IDENTITY_RESOLUTION_GAP", priority="HIGH", observed=125, baseline=0,
                    summary="125 observed orders have unresolved downstream identity."),
            _signal(4, "PLATFORM_OBSERVED_GAP", priority="LOW", observed=900, baseline=700),
            _signal(5, "CAMPAIGN_DELIVERY_GAP", scope_type="CAMPAIGN",
                    scope_name="Sama-NewUM", observed=0.29, baseline=0.52),
            _signal(6, "CAMPAIGN_RETURN_PRESSURE", scope_type="CAMPAIGN",
                    scope_name="Sama-NewUM", observed=0.66, baseline=0.41,
                    summary="Observed return rate 66.00% vs peer rate 41.00%."),
            _signal(7, "CAMPAIGN_ACQUISITION_COST_GAP", priority="LOW",
                    scope_type="CAMPAIGN", scope_name="Sama-Other", observed=12, baseline=8),
        ]

    def _mark(self, business_id: str) -> None:
        self.requested.append(business_id)

    def fetch_as_of(self, business_id):
        self._mark(business_id); return {"business_id": business_id, "as_of_date": date(2026, 7, 31)}

    def fetch_signals(self, business_id):
        self._mark(business_id); return self.signals

    def fetch_leakage(self, business_id):
        self._mark(business_id)
        stages = (
            "PLATFORM_VS_OBSERVED", "IDENTITY_UNRESOLVED", "MATCHED_NOT_CONFIRMED",
            "CONFIRMED_NOT_SHIPPED", "RETURNED_AFTER_SHIPMENT", "SHIPPED_TERMINAL_UNRESOLVED",
        )
        return [{
            "business_id": business_id, "as_of_date": date(2026, 7, 31),
            "leakage_stage": stage, "category": "FULFILLMENT", "observed_count": index * 10,
            "denominator_count": 700, "observed_rate": index / 70,
            "interpretation": "Aggregate observed stage difference.",
            "is_measurement_gap": index == 1, "is_operational_gap": index > 2,
            "stage_order": index,
        } for index, stage in enumerate(stages, 1)]

    def fetch_campaigns(self, business_id):
        self._mark(business_id)
        return [{
            "business_id": business_id, "campaign_name": "Sama-NewUM", "spend_usd": 1200.0,
            "lightfunnels_orders": 200, "matched_orders": 180, "confirmed_orders": 140,
            "shipped_orders": 120, "delivered_orders": 35, "returned_orders": 79,
            "confirmation_rate": 0.7778, "peer_confirmation_rate": 0.81,
            "delivery_rate": 0.2917, "peer_delivery_rate": 0.52,
            "return_rate": 0.6583, "peer_return_rate": 0.41,
            "cost_per_lightfunnels_order_usd": 6.0,
            "peer_cost_per_lightfunnels_order_usd": 5.0,
            "cost_per_delivered_order_usd": 34.2857,
            "peer_cost_per_delivered_order_usd": 18.0,
            "delivery_benchmark_gap_orders": 27.4, "excess_returns_vs_peer": 29.8,
            "confirmation_sample_band": "HIGH", "fulfillment_sample_band": "HIGH",
            "acquisition_sample_band": "HIGH", "cost_delivered_sample_band": "MEDIUM",
            "sample_band": "HIGH",
            "benchmark_interpretation": "Leave-one-out observational peer benchmark.",
        }]

    def fetch_anomalies(self, business_id):
        self._mark(business_id)
        metrics = (
            "daily_target_spend", "lightfunnels_order_volume", "confirmed_order_volume",
            "delivered_order_volume", "returned_order_volume",
        )
        return [{
            "metric_name": metric, "status": "NORMAL", "severity": "INFO",
            "observed_at_utc": datetime(2026, 7, 31, tzinfo=timezone.utc),
            "observed_value": 10.0, "expected_value": 11.0, "lower_bound": 5.0,
            "upper_bound": 15.0, "baseline_strategy": "robust_history",
            "confidence": "HIGH", "explanation": "Within the persisted baseline.",
        } for metric in metrics]

    def fetch_economics(self, business_id):
        self._mark(business_id)
        return [{"business_id": business_id, "economic_status": "FX_REQUIRED", "currency": "SAR"}]


def context():
    return build_context(BUSINESS, FakeRepository())


def answer_for(ctx, **changes) -> AnalystAnswer:
    ref = ctx.evidence_items[0].evidence_id
    values = {
        "question": "What happened?",
        "answer_summary": "An observed signal is active.",
        "findings": (Finding(
            statement="Observed evidence is active.", evidence_refs=(ref,),
            confidence=Confidence.HIGH, claim_type=ClaimType.OBSERVATION,
            causal_claim=False,
        ),),
        "investigation_steps": ("Review the aggregate evidence.",),
        "limitations": ("Current data cannot establish cause.",),
        "confidence": Confidence.HIGH,
        "cannot_answer_fully": False,
        "safety_notes": ("No autonomous action is authorized.",),
    }
    values.update(changes)
    return AnalystAnswer(**values)


def provider_payload(answer: AnalystAnswer) -> dict:
    payload = answer.to_dict()
    payload.pop("evidence_refs")
    return payload


class ContextBuilderTests(unittest.TestCase):
    def test_bounded_context_is_complete_business_scoped_and_stable(self) -> None:
        repo = FakeRepository()
        first = build_context(BUSINESS, repo)
        second = build_context(BUSINESS, FakeRepository())
        self.assertEqual(len(first.signals), 7)
        self.assertEqual(len(first.leakage), 6)
        self.assertEqual(len(first.campaign_diagnostics), 1)
        self.assertEqual(len(first.time_anomalies), 5)
        self.assertEqual(first.economic_status.facts["economic_status"], "FX_REQUIRED")
        self.assertTrue(all(item.business_id == BUSINESS for item in first.evidence_items))
        self.assertTrue(all(value == BUSINESS for value in repo.requested))
        self.assertEqual(
            [item.evidence_id for item in first.evidence_items],
            [item.evidence_id for item in second.evidence_items],
        )
        self.assertIn("signal:return_pressure", first.evidence_by_id)
        self.assertIn("campaign:sama-newum", first.evidence_by_id)
        self.assertIn("anomaly:returned_order_volume", first.evidence_by_id)
        self.assertIn("economics:fx_required", first.evidence_by_id)

    def test_prompt_context_contains_no_pii_raw_fields_or_paths(self) -> None:
        payload = serialize_context(context()).lower()
        for forbidden in (
            "phone_hash", "phone_number", "email_address", "tracking_number",
            "customer_id", "order_id", "source_filename", "file_path", "raw_record",
        ):
            self.assertNotIn(forbidden, payload)
        self.assertNotIn("data/private", payload)
        self.assertNotIn("warehouse_password", payload)

    def test_sql_is_fixed_parameterized_and_selects_only_curated_fields(self) -> None:
        for query in (SIGNAL_SQL, LEAKAGE_SQL, CAMPAIGN_SQL, ANOMALY_SQL, ECONOMICS_SQL):
            lowered = query.lower()
            self.assertIn("%s", query)
            self.assertNotIn("select *", lowered)
            for forbidden in ("phone", "email", "address", "tracking_number", "source_filename"):
                self.assertNotIn(forbidden, lowered)
        self.assertIn("marts.sama_pilot_campaign_diagnostics", CAMPAIGN_SQL)

    def test_context_contract_rejects_pii_shaped_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "Forbidden"):
            EvidenceItem(
                evidence_id="bad:pii", evidence_type="TEST", business_id=BUSINESS,
                scope_type="BUSINESS", scope_name=BUSINESS, title="Bad",
                facts={"phone_hash": "secret"}, limitation="Not safe.",
                source_relation="marts.sama_pilot_unified_overview",
            )


class AnswerAndSafetyTests(unittest.TestCase):
    def test_contract_rejects_invalid_confidence_empty_and_unknown_fields(self) -> None:
        ctx = context()
        with self.assertRaises(AnswerValidationError):
            answer_for(ctx, confidence="CERTAIN")
        with self.assertRaises(AnswerValidationError):
            answer_for(ctx, answer_summary=" ")
        payload = answer_for(ctx).to_dict()
        payload.pop("evidence_refs")
        payload["unsupported"] = True
        with self.assertRaisesRegex(AnswerValidationError, "unsupported"):
            ProviderAnswer.from_mapping(payload)

    def test_finding_refs_derive_stable_ordered_union(self) -> None:
        ctx = context()
        first_ref = ctx.evidence_items[0].evidence_id
        second_ref = ctx.evidence_items[1].evidence_id
        answer = answer_for(ctx, findings=(
            Finding(
                statement="First observed finding.", evidence_refs=(first_ref,),
                confidence=Confidence.HIGH,
            ),
            Finding(
                statement="Second observed finding.", evidence_refs=(second_ref,),
                confidence=Confidence.HIGH,
            ),
        ))
        self.assertEqual(answer.evidence_refs, (first_ref, second_ref))

    def test_repeated_cross_finding_ref_is_deduplicated_first_seen(self) -> None:
        ctx = context()
        first_ref = ctx.evidence_items[0].evidence_id
        second_ref = ctx.evidence_items[1].evidence_id
        third_ref = ctx.evidence_items[2].evidence_id
        answer = answer_for(ctx, findings=(
            Finding(
                statement="First observed finding.",
                evidence_refs=(first_ref, second_ref), confidence=Confidence.HIGH,
            ),
            Finding(
                statement="Second observed finding.",
                evidence_refs=(second_ref, third_ref), confidence=Confidence.HIGH,
            ),
        ))
        self.assertEqual(answer.evidence_refs, (first_ref, second_ref, third_ref))
        with self.assertRaisesRegex(AnswerValidationError, "duplicates"):
            Finding(
                statement="Invalid duplicate refs.", evidence_refs=(first_ref, first_ref),
                confidence=Confidence.HIGH,
            )
        with self.assertRaisesRegex(AnswerValidationError, "cannot be empty"):
            Finding(
                statement="Invalid empty refs.", evidence_refs=(),
                confidence=Confidence.HIGH,
            )

    def test_unknown_evidence_reference_is_rejected(self) -> None:
        ctx = context()
        finding = Finding(
            statement="Unknown evidence.", evidence_refs=("unknown:ref",),
            confidence=Confidence.HIGH,
        )
        candidate = answer_for(ctx, findings=(finding,))
        with self.assertRaisesRegex(SafetyValidationError, "unknown"):
            answer_question(candidate.question, ctx, StaticProvider(candidate))

    def test_unsupported_causality_is_rejected(self) -> None:
        ctx = context(); ref = ctx.evidence_items[0].evidence_id
        finding = Finding(
            statement="The campaign caused the returns.", evidence_refs=(ref,),
            confidence=Confidence.HIGH, causal_claim=True,
        )
        candidate = answer_for(ctx, findings=(finding,))
        with self.assertRaisesRegex(SafetyValidationError, "causal"):
            answer_question(candidate.question, ctx, StaticProvider(candidate))

    def test_cross_currency_profit_and_autonomous_action_are_rejected(self) -> None:
        ctx = context(); econ = ctx.economic_status.evidence_id
        financial = answer_for(
            ctx, question="How much profit am I making?",
            answer_summary="Profit is $50.",
            findings=(Finding(
                statement="Profit is $50.", evidence_refs=(econ,), confidence=Confidence.HIGH,
            ),), cannot_answer_fully=True,
        )
        with self.assertRaises(SafetyValidationError):
            answer_question(financial.question, ctx, StaticProvider(financial))
        action = answer_for(ctx, investigation_steps=("Pause the campaign.",))
        with self.assertRaisesRegex(SafetyValidationError, "autonomous"):
            answer_question(action.question, ctx, StaticProvider(action))

    def test_number_absent_from_evidence_is_rejected(self) -> None:
        ctx = context()
        candidate = answer_for(ctx, answer_summary="The unsupported value is 987654.")
        with self.assertRaisesRegex(SafetyValidationError, "number"):
            answer_question(candidate.question, ctx, StaticProvider(candidate))
        derived = answer_for(ctx, answer_summary="The unsupported value is 39000%.")
        with self.assertRaisesRegex(SafetyValidationError, "number"):
            answer_question(derived.question, ctx, StaticProvider(derived))

    def test_customer_level_question_is_rejected_before_provider(self) -> None:
        ctx = context(); provider = MagicMock()
        provider.name = "mock"; provider.model = "mock"
        with self.assertRaisesRegex(SafetyValidationError, "customer-level"):
            answer_question("What happened to customer id 123?", ctx, provider)
        provider.answer.assert_not_called()


class StaticProvider:
    name = "static"
    model = "test"

    def __init__(self, answer): self.value = answer
    def answer(self, question, ctx): return self.value


class RepairingProvider:
    name = "openai"
    model = "offline-repair-test"

    def __init__(self, initial, repaired):
        self.initial = initial
        self.repaired = repaired
        self.answer_calls = 0
        self.repair_calls = 0
        self.repair_codes = None

    def answer(self, question, ctx):
        self.answer_calls += 1
        return self.initial

    def repair(self, rejected_answer, error_codes, ctx):
        self.repair_calls += 1
        self.repair_codes = error_codes
        return self.repaired


class CausalWordingAndRepairTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = context()
        self.ref = self.context.evidence_items[0].evidence_id

    def with_statement(self, statement: str, **changes) -> AnalystAnswer:
        finding = Finding(
            statement=statement, evidence_refs=(self.ref,), confidence=Confidence.HIGH,
            claim_type=ClaimType.OBSERVATION, causal_claim=False,
        )
        return answer_for(self.context, findings=(finding,), **changes)

    @staticmethod
    def replace_first_statement(
        answer: AnalystAnswer, statement: str
    ) -> AnalystAnswer:
        first = answer.findings[0]
        findings = (
            Finding(
                statement=statement,
                evidence_refs=first.evidence_refs,
                confidence=first.confidence,
                claim_type=first.claim_type,
                causal_claim=first.causal_claim,
            ),
            *answer.findings[1:],
        )
        return AnalystAnswer(
            question=answer.question,
            answer_summary=answer.answer_summary,
            findings=findings,
            investigation_steps=answer.investigation_steps,
            limitations=answer.limitations,
            confidence=answer.confidence,
            cannot_answer_fully=answer.cannot_answer_fully,
            safety_notes=answer.safety_notes,
        )

    def test_prompt_explicitly_constrains_causality_and_repair(self) -> None:
        prompt = SYSTEM_INSTRUCTIONS.lower()
        for phrase in (
            "driven by", "because of", "responsible for", "led to", "resulted from",
            "explains why", "root cause", "primary driver", "associated with",
            "differs from peers", "current data cannot establish the cause",
            "what is observed", "what is not known",
        ):
            self.assertIn(phrase, prompt)
        repair = REPAIR_INSTRUCTIONS.lower()
        for phrase in (
            "preserve every valid finding evidence_ref", "introduce no new evidence",
            "numbers", "claims", "preserve limitations", "investigation-only",
        ):
            self.assertIn(phrase, repair)

    def test_affirmative_causal_wording_is_rejected_with_stable_code(self) -> None:
        candidate = self.with_statement("Returns were caused by fulfillment.")
        with self.assertRaises(SafetyValidationError) as raised:
            validate_answer(candidate, candidate.question, self.context)
        self.assertEqual(
            raised.exception.codes,
            (ValidationErrorCode.UNSUPPORTED_CAUSAL_WORDING,),
        )

    def test_observational_association_wording_is_allowed(self) -> None:
        candidate = self.with_statement(
            "Fulfillment appears associated with the observed return pattern."
        )
        self.assertIs(validate_answer(candidate, candidate.question, self.context), candidate)

    def test_negated_causal_limitations_are_allowed(self) -> None:
        for statement in (
            "Current data cannot establish whether fulfillment caused the returns.",
            "This does not prove that the campaign caused the returns.",
        ):
            with self.subTest(statement=statement):
                candidate = self.with_statement(statement)
                self.assertIs(
                    validate_answer(candidate, candidate.question, self.context), candidate
                )

    def test_one_repair_rewrites_causal_wording_then_fully_validates(self) -> None:
        initial = self.with_statement("Returns were caused by fulfillment.")
        repaired = self.with_statement(
            "Return pressure is observed and warrants investigation."
        )
        provider = RepairingProvider(initial, repaired)
        with self.assertLogs("pulse.intelligence", level="INFO") as logs:
            result = answer_question(initial.question, self.context, provider)
        self.assertIs(result, repaired)
        self.assertEqual(provider.answer_calls, 1)
        self.assertEqual(provider.repair_calls, 1)
        self.assertIn("deterministic_fallback_used=False", logs.output[-1])
        self.assertEqual(
            provider.repair_codes, (ValidationErrorCode.UNSUPPORTED_CAUSAL_WORDING.value,)
        )

    def test_second_causal_violation_uses_validated_deterministic_fallback(self) -> None:
        question = "Why are returns high?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        repaired = self.with_statement(
            "Fulfillment caused the observed returns.", question=question
        )
        provider = RepairingProvider(initial, repaired)
        with self.assertLogs("pulse.intelligence", level="INFO") as logs:
            result = answer_question(question, self.context, provider)
        self.assertIs(validate_answer(result, question, self.context), result)
        self.assertIn("signal:return_pressure", result.evidence_refs)
        self.assertIn(
            "signal:campaign_return_pressure:sama-newum", result.evidence_refs
        )
        self.assertIn("cannot establish the causal reason", result.answer_summary)
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)
        metadata = logs.output[-1]
        self.assertIn("deterministic_fallback_used=True", metadata)
        self.assertIn("fallback_intent=returns_reason", metadata)
        self.assertIn("fallback_validation_result=PASSED", metadata)

    def test_nonrepairable_failures_never_call_repair(self) -> None:
        profit_question = "How much profit am I making?"
        cases = (
            answer_for(self.context, findings=(Finding(
                statement="Unknown evidence.", evidence_refs=("unknown:ref",),
                confidence=Confidence.HIGH,
            ),)),
            self.with_statement("The unsupported value is 987654."),
            self.with_statement("Customer id appears in this answer."),
            answer_for(
                self.context,
                question=profit_question,
                answer_summary="Profit is 390.",
                findings=(Finding(
                    statement="Profit is 390.", evidence_refs=(self.ref,),
                    confidence=Confidence.HIGH,
                ),),
            ),
            self.with_statement("Pause Sama-NewUM."),
        )
        expected_codes = (
            ValidationErrorCode.UNKNOWN_EVIDENCE_REF,
            ValidationErrorCode.UNSUPPORTED_NUMBER,
            ValidationErrorCode.PII_DETECTED,
            ValidationErrorCode.UNSUPPORTED_FINANCIAL_CLAIM,
            ValidationErrorCode.AUTONOMOUS_ACTION,
        )
        for candidate, expected_code in zip(cases, expected_codes, strict=True):
            with self.subTest(expected_code=expected_code):
                provider = RepairingProvider(candidate, candidate)
                with self.assertRaises(SafetyValidationError) as raised:
                    answer_question(candidate.question, self.context, provider)
                self.assertIn(expected_code, raised.exception.codes)
                self.assertEqual(provider.answer_calls, 1)
                self.assertEqual(provider.repair_calls, 0)

    def test_autonomous_repair_is_discarded_for_safe_campaign_pause_fallback(self) -> None:
        question = "Should I pause Sama-NewUM?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        repaired = self.with_statement(
            "Pause Sama-NewUM.", question=question
        )
        provider = RepairingProvider(initial, repaired)
        result = answer_question(question, self.context, provider)
        self.assertIs(validate_answer(result, question, self.context), result)
        self.assertTrue(result.cannot_answer_fully)
        self.assertIn(
            "insufficient for an automatic campaign pause decision",
            result.answer_summary,
        )
        self.assertIn(
            "No campaign or budget action is authorized.", result.safety_notes
        )
        self.assertIn("signal:campaign_delivery_gap:sama-newum", result.evidence_refs)
        self.assertIn("signal:campaign_return_pressure:sama-newum", result.evidence_refs)
        self.assertIn("campaign:sama-newum", result.evidence_refs)
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_repair_contract_violation_is_discarded_for_campaign_pause_fallback(self) -> None:
        question = "Should I pause Sama-NewUM?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        repaired = self.with_statement(
            "Return pressure is observed.",
            question=question,
            limitations=("The repair changed a fixed limitation.",),
        )
        provider = RepairingProvider(initial, repaired)
        with self.assertRaises(SafetyValidationError) as rejected:
            validate_repair_contract(initial, repaired)
        self.assertEqual(
            rejected.exception.codes,
            (ValidationErrorCode.REPAIR_CONTRACT_VIOLATION,),
        )
        with self.assertLogs("pulse.intelligence", level="INFO") as logs:
            result = answer_question(question, self.context, provider)
        self.assertIs(validate_answer(result, question, self.context), result)
        self.assertNotEqual(result, repaired)
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)
        metadata = logs.output[-1]
        self.assertIn(
            "initial_validation_codes=UNSUPPORTED_CAUSAL_WORDING", metadata
        )
        self.assertIn(
            "repair_validation_codes=REPAIR_CONTRACT_VIOLATION", metadata
        )
        self.assertIn("repair_discarded=True", metadata)
        self.assertIn("deterministic_fallback_used=True", metadata)
        self.assertIn("fallback_intent=campaign_pause", metadata)
        self.assertIn("provider_call_count=2", metadata)

    def test_unknown_repair_ref_is_discarded_and_absent_from_fallback(self) -> None:
        question = "Should I pause Sama-NewUM?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        repaired = answer_for(
            self.context,
            question=question,
            findings=(Finding(
                statement="Return pressure is observed.",
                evidence_refs=("unknown:repair-ref",),
                confidence=Confidence.HIGH,
            ),),
        )
        provider = RepairingProvider(initial, repaired)
        result = answer_question(question, self.context, provider)
        self.assertNotIn("unknown:repair-ref", result.evidence_refs)
        self.assertTrue(set(result.evidence_refs) <= self.context.evidence_by_id.keys())
        self.assertIs(validate_answer(result, question, self.context), result)
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_unsupported_repair_number_is_discarded_and_absent_from_fallback(self) -> None:
        question = "Should I pause Sama-NewUM?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        repaired = self.with_statement(
            "Return pressure is observed at 987654.", question=question
        )
        provider = RepairingProvider(initial, repaired)
        result = answer_question(question, self.context, provider)
        generated = "\n".join((
            result.answer_summary,
            *(finding.statement for finding in result.findings),
            *result.investigation_steps,
            *result.limitations,
            *result.safety_notes,
        ))
        self.assertNotIn("987654", generated)
        self.assertIs(validate_answer(result, question, self.context), result)
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_pii_and_financial_repair_failures_are_discarded(self) -> None:
        pause_question = "Should I pause Sama-NewUM?"
        pii_initial = self.with_statement(
            "Returns were caused by fulfillment.", question=pause_question
        )
        pii_repair = self.with_statement(
            "Customer id appears in this answer.", question=pause_question
        )

        profit_question = "How much profit am I making?"
        financial_initial = answer_for(
            self.context,
            question=profit_question,
            answer_summary="FX_REQUIRED applies; observed evidence includes 390.",
            findings=(Finding(
                statement="The evidence at 390 caused the observed result.",
                evidence_refs=(self.ref,),
                confidence=Confidence.HIGH,
            ),),
            cannot_answer_fully=True,
        )
        financial_repair = self.replace_first_statement(
            financial_initial, "Profit is 390."
        )

        cases = (
            (pause_question, pii_initial, pii_repair, ValidationErrorCode.PII_DETECTED),
            (
                profit_question,
                financial_initial,
                financial_repair,
                ValidationErrorCode.UNSUPPORTED_FINANCIAL_CLAIM,
            ),
        )
        for question, initial, repaired, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                validate_repair_contract(initial, repaired)
                with self.assertRaises(SafetyValidationError) as rejected:
                    validate_answer(repaired, question, self.context)
                self.assertIn(expected_code, rejected.exception.codes)
                provider = RepairingProvider(initial, repaired)
                result = answer_question(question, self.context, provider)
                self.assertIs(validate_answer(result, question, self.context), result)
                self.assertNotEqual(result, repaired)
                self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_fallback_uses_context_values_instead_of_hardcoded_business_numbers(self) -> None:
        repo = FakeRepository()
        repo.signals[0] = _signal(
            1, "RETURN_PRESSURE", priority="HIGH", observed=432, baseline=321,
            summary="432 returned vs 321 delivered.",
        )
        changed_context = build_context(BUSINESS, repo)
        ref = changed_context.evidence_items[0].evidence_id
        question = "Why are returns high?"
        finding = Finding(
            statement="Returns were caused by fulfillment.", evidence_refs=(ref,),
            confidence=Confidence.HIGH,
        )
        initial = answer_for(changed_context, question=question, findings=(finding,))
        provider = RepairingProvider(initial, initial)
        result = answer_question(question, changed_context, provider)
        self.assertIn("432 returned vs 321 delivered", result.findings[0].statement)
        self.assertNotIn("390 returned", "\n".join(
            finding.statement for finding in result.findings
        ))
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_fallback_is_run_through_the_normal_validator(self) -> None:
        question = "Why are returns high?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        provider = RepairingProvider(initial, initial)
        with patch(
            "src.intelligence.narration.validate_answer", wraps=validate_answer
        ) as validator:
            result = answer_question(question, self.context, provider)
        self.assertEqual(validator.call_count, 3)
        self.assertIs(validate_answer(result, question, self.context), result)

    def test_invalid_deterministic_fallback_still_fails_closed(self) -> None:
        question = "Why are returns high?"
        initial = self.with_statement(
            "Returns were caused by fulfillment.", question=question
        )
        invalid_fallback = answer_for(
            self.context,
            question=question,
            answer_summary="The unsupported value is 987654.",
        )
        provider = RepairingProvider(initial, initial)
        with patch(
            "src.intelligence.narration.build_deterministic_answer",
            return_value=invalid_fallback,
        ), self.assertRaises(SafetyValidationError) as raised:
            answer_question(question, self.context, provider)
        self.assertIn(ValidationErrorCode.UNSUPPORTED_NUMBER, raised.exception.codes)
        self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_question_intents_are_small_and_deterministic(self) -> None:
        cases = {
            "What should I investigate first?": QuestionIntent.INVESTIGATE_FIRST,
            "Why are returns high?": QuestionIntent.RETURNS_REASON,
            "Should I pause Sama-NewUM?": QuestionIntent.CAMPAIGN_PAUSE,
            "How much profit am I making?": QuestionIntent.PROFIT,
            "Did something abnormal happen today?": QuestionIntent.ANOMALY_TODAY,
            "Summarize current evidence.": QuestionIntent.GENERAL,
        }
        for question, expected in cases.items():
            with self.subTest(question=question):
                self.assertIs(classify_question_intent(question), expected)

    def test_all_required_intents_can_use_the_same_validated_fallback(self) -> None:
        questions = (
            "What should I investigate first?",
            "Why are returns high?",
            "Should I pause Sama-NewUM?",
            "How much profit am I making?",
            "Did something abnormal happen today?",
        )
        deterministic = OfflineFakeProvider()
        for question in questions:
            with self.subTest(question=question):
                safe = deterministic.answer(question, self.context)
                causal = self.replace_first_statement(
                    safe, "The cited evidence caused the observed result."
                )
                provider = RepairingProvider(causal, causal)
                result = answer_question(question, self.context, provider)
                self.assertIs(validate_answer(result, question, self.context), result)
                self.assertEqual(provider.answer_calls + provider.repair_calls, 2)

    def test_mixed_initial_causal_and_unknown_ref_has_no_recovery(self) -> None:
        initial = answer_for(
            self.context,
            findings=(Finding(
                statement="The fabricated input caused the result.",
                evidence_refs=("unknown:initial-ref",),
                confidence=Confidence.HIGH,
            ),),
        )
        provider = RepairingProvider(initial, initial)
        with self.assertRaises(SafetyValidationError) as raised:
            answer_question(initial.question, self.context, provider)
        self.assertEqual(set(raised.exception.codes), {
            ValidationErrorCode.UNKNOWN_EVIDENCE_REF,
            ValidationErrorCode.UNSUPPORTED_CAUSAL_WORDING,
        })
        self.assertEqual(provider.answer_calls, 1)
        self.assertEqual(provider.repair_calls, 0)

    def test_valid_initial_answer_uses_exactly_one_provider_call(self) -> None:
        valid = self.with_statement("Return pressure is observed.")
        provider = RepairingProvider(valid, valid)
        with self.assertLogs("pulse.intelligence", level="INFO") as logs:
            self.assertIs(answer_question(valid.question, self.context, provider), valid)
        self.assertEqual(provider.answer_calls, 1)
        self.assertEqual(provider.repair_calls, 0)
        self.assertIn("deterministic_fallback_used=False", logs.output[-1])


class FakeQuestionBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = context()
        self.provider = OfflineFakeProvider()

    def ask(self, question: str) -> AnalystAnswer:
        return answer_question(question, self.context, self.provider)

    def test_investigate_first_uses_stored_priority_order(self) -> None:
        answer = self.ask("What should I investigate first?")
        self.assertEqual(answer.evidence_refs[:3], (
            "signal:return_pressure", "signal:confirmation_leakage",
            "signal:identity_resolution_gap",
        ))
        self.assertTrue(all("priority" not in finding.statement.lower() for finding in answer.findings))

    def test_why_returns_preserves_observation_and_causal_limit(self) -> None:
        answer = self.ask("Why are returns high?")
        self.assertIn("390 returned vs 286 delivered", answer.findings[0].statement)
        self.assertTrue(answer.cannot_answer_fully)
        self.assertIn("cannot establish the causal reason", answer.answer_summary)
        self.assertIn("signal:campaign_return_pressure:sama-newum", answer.evidence_refs)

    def test_pause_question_does_not_make_autonomous_decision(self) -> None:
        answer = self.ask("Should I pause Sama-NewUM?")
        self.assertTrue(answer.cannot_answer_fully)
        self.assertIn("insufficient for an automatic campaign pause decision", answer.answer_summary)
        self.assertIn("campaign:sama-newum", answer.evidence_refs)

    def test_profit_question_preserves_fx_required(self) -> None:
        answer = self.ask("How much profit am I making?")
        self.assertTrue(answer.cannot_answer_fully)
        self.assertIn("FX_REQUIRED", answer.answer_summary)
        self.assertNotIn("$", answer.answer_summary)

    def test_abnormal_today_distinguishes_anomaly_and_structural_signals(self) -> None:
        answer = self.ask("Did something abnormal happen today?")
        self.assertIn("no current anomaly", answer.answer_summary)
        self.assertIn("Structural deterministic business signals", answer.answer_summary)
        self.assertEqual(len([ref for ref in answer.evidence_refs if ref.startswith("anomaly:")]), 5)


class ProviderAndCliTests(unittest.TestCase):
    def test_openai_configuration_missing_fails_without_import_or_network(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ProviderError, "PULSE_LLM_MODEL"):
                OpenAIProvider.from_env()

    def test_openai_uses_responses_structured_output_and_rejects_malformed(self) -> None:
        client = MagicMock()
        client.responses.create.return_value.status = "completed"
        client.responses.create.return_value.output_text = "not-json"
        provider = OpenAIProvider(model="explicit-model", api_key="test-secret", client=client)
        with self.assertRaisesRegex(ProviderError, "malformed"):
            provider.answer("What should I investigate?", context())
        kwargs = client.responses.create.call_args.kwargs
        self.assertEqual(kwargs["model"], "explicit-model")
        self.assertIs(kwargs["store"], False)
        self.assertEqual(kwargs["text"]["format"]["type"], "json_schema")
        self.assertNotIn("test-secret", json.dumps(kwargs))

    def test_openai_provider_omits_top_level_refs_and_derives_final_union(self) -> None:
        ctx = context()
        first_ref = ctx.evidence_items[0].evidence_id
        second_ref = ctx.evidence_items[1].evidence_id
        payload = {
            "question": "What should I investigate?",
            "answer_summary": "Review the cited aggregate evidence.",
            "findings": [
                {
                    "statement": "First observed finding.",
                    "evidence_refs": [first_ref], "confidence": "HIGH",
                    "claim_type": "OBSERVATION", "causal_claim": False,
                },
                {
                    "statement": "Second observed finding.",
                    "evidence_refs": [first_ref, second_ref], "confidence": "HIGH",
                    "claim_type": "OBSERVATION", "causal_claim": False,
                },
            ],
            "investigation_steps": ["Review the aggregate evidence."],
            "limitations": ["Current data cannot establish cause."],
            "confidence": "HIGH", "cannot_answer_fully": False,
            "safety_notes": ["No autonomous action is authorized."],
        }
        self.assertNotIn("evidence_refs", PROVIDER_ANSWER_JSON_SCHEMA["properties"])
        self.assertNotIn("evidence_refs", PROVIDER_ANSWER_JSON_SCHEMA["required"])
        client = MagicMock()
        client.responses.create.return_value.status = "completed"
        client.responses.create.return_value.output_text = json.dumps(payload)
        provider = OpenAIProvider(model="explicit-model", api_key="test-secret", client=client)
        answer = provider.answer(payload["question"], ctx)
        self.assertEqual(answer.evidence_refs, (first_ref, second_ref))
        request_format = client.responses.create.call_args.kwargs["text"]["format"]
        self.assertEqual(request_format["name"], "pulse_provider_answer")

    def test_openai_repair_request_is_bounded_and_called_once(self) -> None:
        ctx = context()
        ref = ctx.evidence_items[0].evidence_id
        initial = answer_for(ctx, findings=(Finding(
            statement="Returns were caused by fulfillment.", evidence_refs=(ref,),
            confidence=Confidence.HIGH,
        ),))
        repaired = answer_for(ctx, findings=(Finding(
            statement="Return pressure is observed and warrants investigation.",
            evidence_refs=(ref,), confidence=Confidence.HIGH,
        ),))
        responses = []
        for answer in (initial, repaired):
            response = MagicMock()
            response.status = "completed"
            response.output_text = json.dumps(provider_payload(answer))
            responses.append(response)
        client = MagicMock()
        client.responses.create.side_effect = responses
        provider = OpenAIProvider(model="explicit-model", api_key="test-secret", client=client)
        result = answer_question(initial.question, ctx, provider)
        self.assertEqual(result.findings[0].statement, repaired.findings[0].statement)
        self.assertEqual(client.responses.create.call_count, 2)
        repair_call = client.responses.create.call_args_list[1].kwargs
        repair_input = json.loads(repair_call["input"])
        self.assertEqual(set(repair_input), {
            "rejected_structured_answer", "validation_error_codes",
            "authoritative_evidence_context",
        })
        self.assertEqual(
            repair_input["validation_error_codes"], ["UNSUPPORTED_CAUSAL_WORDING"]
        )
        self.assertNotIn(
            "evidence_refs", repair_input["rejected_structured_answer"]
        )
        self.assertIn("Preserve every valid finding evidence_ref", repair_call["instructions"])
        self.assertIs(repair_call["store"], False)

    def test_openai_provider_error_is_safe(self) -> None:
        client = MagicMock()
        client.responses.create.side_effect = RuntimeError("authorization secret leaked")
        provider = OpenAIProvider(model="explicit-model", api_key="test-secret", client=client)
        with self.assertRaises(ProviderError) as raised:
            provider.answer("What should I investigate?", context())
        self.assertEqual(str(raised.exception), "OpenAI narration request failed")

    def test_fake_cli_json_parses_without_api_key_or_network(self) -> None:
        output = io.StringIO(); errors = io.StringIO()
        with patch("src.intelligence.cli.build_context", return_value=context()), \
             patch.dict(os.environ, {}, clear=True), redirect_stdout(output), redirect_stderr(errors):
            code = main([
                "ask", "--business-id", BUSINESS, "--provider", "fake", "--format", "json",
                "--question", "How much profit am I making?",
            ])
        self.assertEqual(code, 0, errors.getvalue())
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["confidence"], "HIGH")
        self.assertTrue(payload["cannot_answer_fully"])
        self.assertEqual(payload["evidence_refs"], ["economics:fx_required"])

    def test_live_acceptance_is_guarded(self) -> None:
        with patch.dict(os.environ, {}, clear=True), redirect_stderr(io.StringIO()):
            self.assertEqual(main(["live-acceptance", "--business-id", BUSINESS]), 2)


@unittest.skipUnless(
    os.environ.get("RUN_PULSE_LLM_ACCEPTANCE") == "1",
    "Live OpenAI acceptance is explicit opt-in",
)
class LiveOpenAIAcceptanceTests(unittest.TestCase):
    def test_real_sanitized_context_answers_validate(self) -> None:
        ctx = build_context(BUSINESS)
        provider = OpenAIProvider.from_env()
        for question in (
            "What should I investigate first?", "Why are returns high?",
            "Should I pause Sama-NewUM?", "How much profit am I making?",
            "Did something abnormal happen today?",
        ):
            with self.subTest(question=question):
                answer = answer_question(question, ctx, provider)
                self.assertIs(validate_answer(answer, question, ctx), answer)
                self.assertTrue(answer.limitations)
                self.assertTrue(answer.evidence_refs)
                self.assertTrue(set(answer.evidence_refs) <= ctx.evidence_by_id.keys())
                self.assertEqual(
                    set(answer.evidence_refs),
                    {
                        ref
                        for finding in answer.findings
                        for ref in finding.evidence_refs
                    },
                )
                if question == "Why are returns high?":
                    self.assertTrue(answer.cannot_answer_fully)
                if question == "Should I pause Sama-NewUM?":
                    self.assertTrue(answer.cannot_answer_fully)
                if question == "How much profit am I making?":
                    self.assertIn("FX_REQUIRED", "\n".join((
                        answer.answer_summary, *answer.limitations,
                        *answer.safety_notes,
                    )))


if __name__ == "__main__":
    unittest.main()
