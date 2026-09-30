"""Narration providers. Fake is offline; OpenAI is lazy and explicitly configured."""

from __future__ import annotations

import json
import os
from enum import StrEnum
from typing import Any, Protocol

from src.intelligence.answer_models import (
    PROVIDER_ANSWER_JSON_SCHEMA,
    AnalystAnswer,
    AnswerValidationError,
    ClaimType,
    Confidence,
    Finding,
    ProviderAnswer,
)
from src.intelligence.context import EvidenceItem, IntelligenceContext


class ProviderError(RuntimeError):
    """Safe provider failure that contains no prompt, secret, or raw response."""


class NarrationProvider(Protocol):
    name: str
    model: str

    def answer(self, question: str, context: IntelligenceContext) -> AnalystAnswer: ...


def _confidence(item: EvidenceItem) -> Confidence:
    value = item.facts.get("confidence", "MEDIUM")
    try:
        return Confidence(value)
    except ValueError:
        return Confidence.MEDIUM


def _finding(item: EvidenceItem, statement: str | None = None) -> Finding:
    return Finding(
        statement=statement or str(item.facts.get("evidence_summary", item.title)),
        evidence_refs=(item.evidence_id,), confidence=_confidence(item),
        claim_type=ClaimType.OBSERVATION, causal_claim=False,
    )


def _dedupe(values: list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value))


class QuestionIntent(StrEnum):
    INVESTIGATE_FIRST = "investigate_first"
    RETURNS_REASON = "returns_reason"
    CAMPAIGN_PAUSE = "campaign_pause"
    PROFIT = "profit"
    ANOMALY_TODAY = "anomaly_today"
    GENERAL = "general"


def classify_question_intent(question: str) -> QuestionIntent:
    """Classify only the small stable intent set required by Phase 6.6B."""
    normalized = " ".join(question.lower().split())
    if any(word in normalized for word in ("profit", "roas", "mer", "margin")):
        return QuestionIntent.PROFIT
    if (
        "abnormal" in normalized or "anomaly" in normalized
        or "changed recently" in normalized or "what changed" in normalized
    ):
        return QuestionIntent.ANOMALY_TODAY
    if "pause" in normalized or "budget" in normalized:
        return QuestionIntent.CAMPAIGN_PAUSE
    if "return" in normalized and any(
        word in normalized for word in ("why", "reason", "high")
    ):
        return QuestionIntent.RETURNS_REASON
    if (
        "investigate first" in normalized or "prioritize" in normalized
        or "priority" in normalized
    ):
        return QuestionIntent.INVESTIGATE_FIRST
    return QuestionIntent.GENERAL


class DeterministicAnswerBuilder:
    """Build grounded answers locally from the validated aggregate context."""

    def answer(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        normalized = " ".join(question.lower().split())
        intent = classify_question_intent(question)
        if intent is QuestionIntent.PROFIT:
            return self._economics(question, context)
        if intent is QuestionIntent.ANOMALY_TODAY:
            return self._anomalies(question, context)
        if intent is QuestionIntent.CAMPAIGN_PAUSE:
            return self._pause(question, context)
        if intent is QuestionIntent.RETURNS_REASON:
            return self._returns(question, context)
        if "which campaign" in normalized or "different" in normalized and "peer" in normalized:
            return self._campaigns(question, context)
        return self._priorities(question, context)

    def _priorities(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        ordered = [
            item for item in context.evidence_items
            if item.evidence_type == "SIGNAL" and item.facts.get("priority") == "HIGH"
        ]
        if not ordered:
            ordered = [item for item in context.evidence_items if item.evidence_type == "SIGNAL"][:3]
        findings = tuple(_finding(item) for item in ordered)
        steps = _dedupe([str(item.facts.get("recommended_next_step", "")) for item in ordered])
        limitations = _dedupe([item.limitation for item in ordered])
        return AnalystAnswer(
            question=question,
            answer_summary=(
                "Investigate the active deterministic signals in their stored priority order."
            ),
            findings=findings, investigation_steps=steps,
            limitations=limitations, confidence=Confidence.HIGH if ordered else Confidence.LOW,
            cannot_answer_fully=not bool(ordered),
            safety_notes=("Investigation guidance is not an autonomous business action.",),
        )

    def _returns(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        items = [
            item for item in context.evidence_items
            if (
                item.evidence_type == "SIGNAL"
                and str(item.facts.get("signal_type", "")) in {
                    "RETURN_PRESSURE", "CAMPAIGN_RETURN_PRESSURE",
                    "CAMPAIGN_DELIVERY_GAP",
                }
            )
        ]
        if not items:
            items = [
                item for item in context.campaign_diagnostics
                if item.facts.get("returned_orders") is not None
            ][:2]
        findings = tuple(_finding(item) for item in items)
        return AnalystAnswer(
            question=question,
            answer_summary=(
                "Return pressure is observed, including campaign differences from leave-one-out "
                "peers, but current data cannot establish the causal reason."
            ),
            findings=findings,
            investigation_steps=_dedupe([
                str(item.facts.get("recommended_next_step", "Review downstream return evidence."))
                for item in items
            ]),
            limitations=_dedupe([item.limitation for item in items] + [
                "Current data cannot establish the causal reason for returns."
            ]),
            confidence=Confidence.HIGH if items else Confidence.LOW,
            cannot_answer_fully=True,
            safety_notes=("Observed association and peer difference are not causal proof.",),
        )

    def _pause(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        query = question.lower()
        campaigns = [
            item for item in context.campaign_diagnostics
            if item.scope_name.lower() in query
            or item.scope_name.lower().replace("-", " ") in query.replace("-", " ")
        ]
        if not campaigns:
            campaigns = list(context.campaign_diagnostics[:1])
        names = {item.scope_name for item in campaigns}
        signals = [
            item for item in context.evidence_items
            if item.evidence_type == "SIGNAL" and item.scope_name in names
        ]
        items = [*signals, *campaigns]
        findings = tuple(_finding(item) for item in items)
        return AnalystAnswer(
            question=question,
            answer_summary=(
                "Current evidence is insufficient for an automatic campaign pause decision. "
                "It supports investigation of the observed downstream and peer benchmark gaps."
            ),
            findings=findings,
            investigation_steps=_dedupe([
                str(item.facts.get("recommended_next_step", "Review the campaign diagnostic evidence."))
                for item in items
            ]),
            limitations=_dedupe([item.limitation for item in items] + [
                "Pulse has no validated autonomous decision policy for campaign pausing."
            ]),
            confidence=Confidence.HIGH if items else Confidence.LOW,
            cannot_answer_fully=True,
            safety_notes=("No campaign or budget action is authorized.",),
        )

    def _campaigns(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        normalized = question.lower().replace("-", " ")
        named = {
            item.scope_name for item in context.campaign_diagnostics
            if item.scope_name.lower().replace("-", " ") in normalized
        }
        signals = [
            item for item in context.evidence_items
            if item.evidence_type == "SIGNAL" and item.scope_type == "CAMPAIGN"
            and (not named or item.scope_name in named)
        ]
        campaigns = [
            item for item in context.campaign_diagnostics
            if not named or item.scope_name in named
        ]
        items = [*signals, *campaigns[:3]]
        if not items:
            return self._priorities(question, context)
        return AnalystAnswer(
            question=question,
            answer_summary=(
                "The cited campaigns have observed differences from leave-one-out peers. "
                "These comparisons identify attention areas, not causal effects or budget actions."
            ),
            findings=tuple(_finding(item) for item in items),
            investigation_steps=_dedupe([
                str(item.facts.get("recommended_next_step", "Compare the aggregate campaign evidence."))
                for item in items
            ]),
            limitations=_dedupe([item.limitation for item in items]),
            confidence=Confidence.HIGH if items else Confidence.LOW,
            cannot_answer_fully=not bool(items),
            safety_notes=("Campaign comparison is observational and does not authorize action.",),
        )

    def _economics(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        item = context.economic_status
        status = str(item.facts["economic_status"])
        return AnalystAnswer(
            question=question,
            answer_summary=(
                f"Pulse cannot currently calculate trusted business profit. {status} applies "
                "because native COD revenue and USD costs cannot be combined without trusted FX."
            ),
            findings=(Finding(
                statement=f"The current economic safety status is {status}.",
                evidence_refs=(item.evidence_id,), confidence=Confidence.HIGH,
                claim_type=ClaimType.LIMITATION, causal_claim=False,
            ),),
            investigation_steps=("Provide a governed, date-aware FX source before calculating cross-currency economics.",),
            limitations=(item.limitation,),
            confidence=Confidence.HIGH, cannot_answer_fully=True,
            safety_notes=("No currency conversion or financial metric was invented.",),
        )

    def _anomalies(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        current = list(context.time_anomalies)
        active = [item for item in current if item.facts.get("status") == "ANOMALY"]
        if active:
            summary = "The latest persisted time-series evaluation contains a current anomaly."
            selected = active
        else:
            summary = (
                "The latest persisted time-series evaluations show no current anomaly. "
                "Structural deterministic business signals may still be active."
            )
            selected = current
        structural = next((
            item for item in context.evidence_items
            if item.evidence_type == "SIGNAL" and item.facts.get("priority") == "HIGH"
        ), None)
        findings = [_finding(item, f"{item.scope_name}: {item.facts.get('status')}.") for item in selected]
        if structural is not None:
            findings.append(_finding(structural))
        return AnalystAnswer(
            question=question, answer_summary=summary, findings=tuple(findings),
            investigation_steps=("Review structural signals separately from time-series anomaly state.",),
            limitations=(
                "Latest persisted anomaly state is distinct from structural business diagnostics.",
            ),
            confidence=Confidence.HIGH, cannot_answer_fully=False,
            safety_notes=("No absence of an anomaly is interpreted as absence of business risk.",),
        )


def build_deterministic_answer(
    question: str, context: IntelligenceContext
) -> AnalystAnswer:
    """Construct a local answer without provider prose, network calls, or new metrics."""
    return DeterministicAnswerBuilder().answer(question, context)


class OfflineFakeProvider:
    """Deterministic behavioral narrator used by local execution and CI."""

    name = "fake"
    model = "offline-deterministic-v1"

    def answer(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        return build_deterministic_answer(question, context)


class OpenAIProvider:
    """Optional official Responses API adapter; construction never makes a request."""

    name = "openai"

    def __init__(self, *, model: str, api_key: str, client: Any | None = None) -> None:
        if not model.strip():
            raise ProviderError("PULSE_LLM_MODEL is required for the OpenAI provider")
        if not api_key.strip():
            raise ProviderError("OPENAI_API_KEY is required for the OpenAI provider")
        self.model = model.strip()
        if client is None:
            try:
                from openai import OpenAI
            except ImportError:
                raise ProviderError(
                    "The optional OpenAI SDK is not installed; install project requirements"
                ) from None
            client = OpenAI(api_key=api_key)
        self._client = client

    @classmethod
    def from_env(cls, *, client: Any | None = None) -> "OpenAIProvider":
        return cls(
            model=os.environ.get("PULSE_LLM_MODEL", ""),
            api_key=os.environ.get("OPENAI_API_KEY", ""),
            client=client,
        )

    def answer(self, question: str, context: IntelligenceContext) -> AnalystAnswer:
        from src.intelligence.narration import SYSTEM_INSTRUCTIONS, serialize_context

        return self._request_answer(
            instructions=SYSTEM_INSTRUCTIONS,
            input_text=(
                "Question:\n" + question + "\n\nAuthoritative evidence JSON:\n"
                + serialize_context(context)
            ),
        )

    def repair(
        self,
        rejected_answer: AnalystAnswer,
        error_codes: tuple[str, ...],
        context: IntelligenceContext,
    ) -> AnalystAnswer:
        """Make one caller-bounded wording repair using only sanitized evidence."""
        from src.intelligence.narration import REPAIR_INSTRUCTIONS, SYSTEM_INSTRUCTIONS

        rejected_payload = rejected_answer.to_dict()
        rejected_payload.pop("evidence_refs")
        repair_payload = {
            "rejected_structured_answer": rejected_payload,
            "validation_error_codes": list(error_codes),
            "authoritative_evidence_context": context.to_prompt_dict(),
        }
        return self._request_answer(
            instructions=f"{SYSTEM_INSTRUCTIONS}\n\n{REPAIR_INSTRUCTIONS}",
            input_text=json.dumps(
                repair_payload, sort_keys=True, separators=(",", ":"),
                ensure_ascii=True, allow_nan=False,
            ),
        )

    def _request_answer(self, *, instructions: str, input_text: str) -> AnalystAnswer:
        try:
            response = self._client.responses.create(
                model=self.model,
                store=False,
                instructions=instructions,
                input=input_text,
                text={
                    "format": {
                        "type": "json_schema", "name": "pulse_provider_answer",
                        "strict": True, "schema": PROVIDER_ANSWER_JSON_SCHEMA,
                    }
                },
            )
            if getattr(response, "status", "completed") != "completed":
                raise ProviderError("OpenAI returned an incomplete analyst answer")
            payload = json.loads(response.output_text)
            return ProviderAnswer.from_mapping(payload).to_analyst_answer()
        except ProviderError:
            raise
        except (AnswerValidationError, json.JSONDecodeError, TypeError, AttributeError):
            raise ProviderError("OpenAI returned a malformed structured analyst answer") from None
        except Exception:
            raise ProviderError("OpenAI narration request failed") from None
