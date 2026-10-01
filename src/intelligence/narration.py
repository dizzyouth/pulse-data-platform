"""Prompt construction, post-generation safety validation, and presentation."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable

from src.intelligence.answer_models import (
    AnalystAnswer,
    AnswerValidationError,
    derive_evidence_refs,
)
from src.intelligence.context import IntelligenceContext
from src.intelligence.providers import (
    NarrationProvider,
    ProviderError,
    build_deterministic_answer,
    classify_question_intent,
)


SYSTEM_INSTRUCTIONS = """You are the narration layer for Pulse.
The supplied bounded evidence JSON is authoritative. Do not invent facts or numbers.
Do not calculate unsupported metrics, financial results, currency conversions, or lift.
Do not make causal claims beyond evidence explicitly marked causal_claim=true.
When cited evidence has causal_claim=false, never say caused, causes, caused by,
driven by, due to, because of, responsible for, led to, resulted from, explains why,
root cause, or primary driver. Use observational wording: observed, associated with,
differs from peers, benchmark gap, appears at this stage, warrants investigation, or
current data cannot establish the cause.
For "Why are returns high?", separate WHAT IS OBSERVED (return pressure and relevant
campaign peer differences) from WHAT IS NOT KNOWN (the causal reason for returns).
Do not expose or request customer-level data, identifiers, credentials, or source files.
Every material finding must reference one or more supplied evidence_id values.
State evidence limitations explicitly. Peer differences and benchmark gaps are observational.
Platform conversions versus observed orders is a measurement difference, not proof of loss,
fraud, or tracking failure. Suggestions must be investigations (review, compare, validate),
not autonomous actions (pause, scale, cut spend, change budgets, refund, or contact customers).
If FX_REQUIRED is present, do not calculate profit, contribution, margin, MER, or ROAS.
Return only the requested structured ProviderAnswer JSON. Put grounding references on each
finding; Pulse derives the final top-level evidence summary. Single-turn only; do not claim memory.
"""


REPAIR_INSTRUCTIONS = """This is one bounded repair of rejected narration.
Preserve every valid finding evidence_ref exactly and preserve all supported numerical facts.
Introduce no new evidence, numbers, claims, or recommendations. Rewrite only unsupported
causal wording as observational language. Preserve limitations and investigation-only
recommendations. Return the complete ProviderAnswer JSON and obey all Pulse safety instructions.
"""


class ValidationErrorCode(StrEnum):
    QUESTION_MISMATCH = "QUESTION_MISMATCH"
    DERIVED_EVIDENCE_MISMATCH = "DERIVED_EVIDENCE_MISMATCH"
    UNKNOWN_EVIDENCE_REF = "UNKNOWN_EVIDENCE_REF"
    MISSING_EVIDENCE_REF = "MISSING_EVIDENCE_REF"
    UNSUPPORTED_CAUSAL_CLAIM = "UNSUPPORTED_CAUSAL_CLAIM"
    UNSUPPORTED_CAUSAL_WORDING = "UNSUPPORTED_CAUSAL_WORDING"
    AUTONOMOUS_ACTION = "AUTONOMOUS_ACTION"
    PII_DETECTED = "PII_DETECTED"
    UNSUPPORTED_NUMBER = "UNSUPPORTED_NUMBER"
    UNSUPPORTED_FINANCIAL_CLAIM = "UNSUPPORTED_FINANCIAL_CLAIM"
    REPAIR_CONTRACT_VIOLATION = "REPAIR_CONTRACT_VIOLATION"


class SafetyValidationError(AnswerValidationError):
    """The structured answer could not be established as safe and grounded."""

    def __init__(
        self, message: str, codes: Iterable[ValidationErrorCode]
    ) -> None:
        self.codes = tuple(dict.fromkeys(codes))
        if not self.codes:
            raise ValueError("SafetyValidationError requires at least one code")
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class AnswerExecutionMetadata:
    """Safe execution facts suitable for presentation surfaces."""

    provider_call_count: int
    repair_attempted: bool
    deterministic_fallback_used: bool
    fallback_intent: str | None
    latency_ms: float


@dataclass(frozen=True, slots=True)
class AnswerExecutionResult:
    answer: AnalystAnswer
    metadata: AnswerExecutionMetadata


_NUMBER = re.compile(r"(?<![A-Za-z0-9_])\$?(\d+(?:,\d{3})*(?:\.\d+)?)(%?)")
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE = re.compile(r"(?<!\d)(?:\+?\d[\s().-]*){8,}\d(?!\d)")
_PII_TERMS = re.compile(
    r"\b(?:phone hash|phone number|email address|street address|tracking number|customer id|order id)\b",
    re.IGNORECASE,
)
_CAUSAL_ASSERTION = re.compile(
    r"\b(?:caus(?:e|es|ed)|caused\s+by|driven\s+by|due\s+to|because\s+of|"
    r"responsible\s+for|led\s+to|resulted\s+from|explains\s+why|root\s+cause|"
    r"primary\s+driver)\b",
    re.IGNORECASE,
)
_CAUSAL_LIMITATION = re.compile(
    r"\b(?:cannot|can't|could not|does not|doesn't|do not|don't|did not|didn't)\b"
    r"[^.!?;]*(?:establish|determine|show|prove|confirm|identify|attribute|mean|say)|"
    r"\b(?:unknown|not known|no evidence|insufficient evidence)\b[^.!?;]*",
    re.IGNORECASE,
)
_AUTONOMOUS_STEP = re.compile(
    r"^\s*(?:pause|stop|increase|decrease|cut|scale|fire|refund|contact|change)\b",
    re.IGNORECASE,
)
_AUTONOMOUS_ASSERTION = re.compile(
    r"(?:^|[.!?]\s+)(?:you should\s+|i recommend\s+)?"
    r"(?:pause|stop|increase|decrease|cut|scale|fire|refund|contact|change)\b",
    re.IGNORECASE,
)


def serialize_context(context: IntelligenceContext) -> str:
    """Stable serialization containing only the bounded evidence contract."""
    return json.dumps(
        context.to_prompt_dict(), sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False,
    )


def _answer_text(answer: AnalystAnswer) -> Iterable[str]:
    yield answer.answer_summary
    yield from (finding.statement for finding in answer.findings)
    yield from answer.investigation_steps
    yield from answer.limitations
    yield from answer.safety_notes


def _number_tokens(text: str) -> set[str]:
    return {match.group(1).replace(",", "") for match in _NUMBER.finditer(text)}


def _allowed_numbers(context: IntelligenceContext) -> set[str]:
    allowed = set(_number_tokens(context.as_of_date.isoformat()))
    for item in context.evidence_items:
        for key, value in item.facts.items():
            if isinstance(value, bool) or value is None:
                continue
            if isinstance(value, (int, float)):
                numeric = float(value)
                allowed.update({
                    str(value), f"{numeric:g}", f"{numeric:.1f}", f"{numeric:.2f}",
                    f"{numeric:.4f}",
                })
                if "rate" in key or key == "relative_gap":
                    allowed.update({
                        f"{numeric * 100:g}", f"{numeric * 100:.1f}",
                        f"{numeric * 100:.2f}",
                    })
            elif isinstance(value, str):
                allowed.update(_number_tokens(value))
    return {token.replace(",", "") for token in allowed}


def _has_unsupported_causal_wording(text: str) -> bool:
    for sentence in re.split(r"(?<=[.!?;])\s+|\n+", text):
        if _CAUSAL_ASSERTION.search(sentence) and not _CAUSAL_LIMITATION.search(sentence):
            return True
    return False


def validate_repair_contract(
    rejected: AnalystAnswer, repaired: AnalystAnswer
) -> AnalystAnswer:
    """Allow wording repair only; all structured grounding and facts stay fixed."""
    fixed_answer_fields_match = (
        repaired.question == rejected.question
        and repaired.investigation_steps == rejected.investigation_steps
        and repaired.limitations == rejected.limitations
        and repaired.confidence == rejected.confidence
        and repaired.cannot_answer_fully == rejected.cannot_answer_fully
        and repaired.safety_notes == rejected.safety_notes
        and len(repaired.findings) == len(rejected.findings)
    )
    fixed_finding_fields_match = fixed_answer_fields_match and all(
        repaired_finding.evidence_refs == rejected_finding.evidence_refs
        and repaired_finding.confidence == rejected_finding.confidence
        and repaired_finding.claim_type == rejected_finding.claim_type
        and repaired_finding.causal_claim == rejected_finding.causal_claim
        for rejected_finding, repaired_finding in zip(
            rejected.findings, repaired.findings, strict=True
        )
    )
    rejected_numbers = _number_tokens("\n".join(_answer_text(rejected)))
    repaired_numbers = _number_tokens("\n".join(_answer_text(repaired)))
    if not fixed_finding_fields_match or repaired_numbers != rejected_numbers:
        raise SafetyValidationError(
            "Repair changed evidence, numbers, limitations, or other fixed answer fields",
            (ValidationErrorCode.REPAIR_CONTRACT_VIOLATION,),
        )
    return repaired


def validate_answer(
    answer: AnalystAnswer, question: str, context: IntelligenceContext
) -> AnalystAnswer:
    """Fail closed unless every structured and exact safety constraint passes."""
    violations: list[tuple[ValidationErrorCode, str]] = []

    def reject(code: ValidationErrorCode, message: str) -> None:
        violations.append((code, message))

    if answer.question != question:
        reject(ValidationErrorCode.QUESTION_MISMATCH, "Provider changed the question")
    known = context.evidence_by_id
    expected_refs = derive_evidence_refs(answer.findings)
    if answer.evidence_refs != expected_refs:
        reject(
            ValidationErrorCode.DERIVED_EVIDENCE_MISMATCH,
            "Answer evidence summary is not deterministically derived",
        )
    all_refs = set(expected_refs)
    unknown = all_refs - known.keys()
    if unknown:
        reject(
            ValidationErrorCode.UNKNOWN_EVIDENCE_REF,
            "Answer references unknown evidence IDs",
        )
    if not all_refs:
        reject(ValidationErrorCode.MISSING_EVIDENCE_REF, "Answer must cite evidence")

    causal_wording_violation = False
    for finding in answer.findings:
        refs_known = all(ref in known for ref in finding.evidence_refs)
        evidence_allows_causality = refs_known and all(
            known[ref].facts.get("causal_claim") is True for ref in finding.evidence_refs
        )
        if finding.causal_claim and not evidence_allows_causality:
            reject(ValidationErrorCode.UNSUPPORTED_CAUSAL_CLAIM, "Unsupported causal claim")
        if _has_unsupported_causal_wording(finding.statement) and not evidence_allows_causality:
            causal_wording_violation = True

    for step in answer.investigation_steps:
        if _AUTONOMOUS_STEP.search(step):
            reject(
                ValidationErrorCode.AUTONOMOUS_ACTION,
                "Recommendation is an autonomous business action",
            )
        if "guaranteed opportunity" in step.lower():
            reject(
                ValidationErrorCode.AUTONOMOUS_ACTION,
                "Recommendation claims a guaranteed opportunity",
            )

    generated = "\n".join(_answer_text(answer))
    answer_allows_causality = bool(all_refs) and not unknown and all(
        known[ref].facts.get("causal_claim") is True for ref in all_refs
    )
    non_finding_text = "\n".join((
        answer.answer_summary, *answer.investigation_steps,
        *answer.limitations, *answer.safety_notes,
    ))
    if _has_unsupported_causal_wording(non_finding_text) and not answer_allows_causality:
        causal_wording_violation = True
    if causal_wording_violation:
        reject(
            ValidationErrorCode.UNSUPPORTED_CAUSAL_WORDING,
            "Causal wording is unsupported by cited evidence",
        )
    if _AUTONOMOUS_ASSERTION.search(generated):
        reject(
            ValidationErrorCode.AUTONOMOUS_ACTION,
            "Answer recommends an autonomous business action",
        )
    if _EMAIL.search(generated) or _PHONE.search(generated) or _PII_TERMS.search(generated):
        reject(
            ValidationErrorCode.PII_DETECTED,
            "Answer contains a customer-level identifier or field",
        )

    allowed_numbers = _allowed_numbers(context)
    unsupported_numbers = _number_tokens(generated) - allowed_numbers
    if unsupported_numbers:
        reject(
            ValidationErrorCode.UNSUPPORTED_NUMBER,
            "Answer contains a number absent from supplied evidence",
        )

    economics = context.economic_status
    status = economics.facts.get("economic_status")
    economic_question = any(
        term in question.lower() for term in ("profit", "roas", "mer", "margin", "contribution")
    )
    if status == "FX_REQUIRED" and economic_question:
        if not answer.cannot_answer_fully or "FX_REQUIRED" not in generated:
            reject(
                ValidationErrorCode.UNSUPPORTED_FINANCIAL_CLAIM,
                "FX_REQUIRED limitation was not preserved",
            )
        unsafe_financial = re.search(
            r"\b(?:profit|roas|mer|margin|contribution)\s+(?:is|equals|=)\s*\$?\d",
            generated, re.IGNORECASE,
        )
        if unsafe_financial:
            reject(
                ValidationErrorCode.UNSUPPORTED_FINANCIAL_CLAIM,
                "Cross-currency financial result is forbidden",
            )
    if violations:
        raise SafetyValidationError(
            "; ".join(dict.fromkeys(message for _, message in violations)),
            (code for code, _ in violations),
        )
    return answer


def question_category(question: str) -> str:
    normalized = question.lower()
    if any(term in normalized for term in ("profit", "roas", "mer", "margin")):
        return "ECONOMICS"
    if "pause" in normalized or "budget" in normalized:
        return "ACTION_REQUEST"
    if "anomal" in normalized or "abnormal" in normalized:
        return "TIME_ANOMALY"
    if "changed recently" in normalized or "what changed" in normalized:
        return "TIME_ANOMALY"
    if "campaign" in normalized or "peer" in normalized:
        return "CAMPAIGN_COMPARISON"
    if "return" in normalized:
        return "RETURNS"
    return "PRIORITIZATION"


def answer_question_with_metadata(
    question: str, context: IntelligenceContext, provider: NarrationProvider
) -> AnswerExecutionResult:
    if not isinstance(question, str) or not question.strip():
        raise AnswerValidationError("question must be non-empty text")
    if _EMAIL.search(question) or _PHONE.search(question) or _PII_TERMS.search(question):
        raise SafetyValidationError(
            "Question contains customer-level data; submit an aggregate business question",
            (ValidationErrorCode.PII_DETECTED,),
        )
    started = time.perf_counter()
    success = False
    initial_validation_result = "NOT_RUN"
    initial_validation_codes = "NONE"
    repair_attempted = False
    repair_reason = "NONE"
    repair_validation_codes = "NOT_ATTEMPTED"
    repaired_validation_result = "NOT_ATTEMPTED"
    repair_discarded = False
    deterministic_fallback_used = False
    fallback_intent = "NONE"
    fallback_validation_result = "NOT_ATTEMPTED"
    provider_call_count = 0

    def completed(answer: AnalystAnswer) -> AnswerExecutionResult:
        nonlocal success
        success = True
        return AnswerExecutionResult(
            answer=answer,
            metadata=AnswerExecutionMetadata(
                provider_call_count=provider_call_count,
                repair_attempted=repair_attempted,
                deterministic_fallback_used=deterministic_fallback_used,
                fallback_intent=(
                    None if fallback_intent == "NONE" else fallback_intent
                ),
                latency_ms=(time.perf_counter() - started) * 1000,
            ),
        )

    def validated_deterministic_fallback() -> AnswerExecutionResult:
        nonlocal deterministic_fallback_used, fallback_intent
        nonlocal fallback_validation_result
        deterministic_fallback_used = True
        fallback_intent = classify_question_intent(question.strip()).value
        try:
            fallback = build_deterministic_answer(question.strip(), context)
            fallback = validate_answer(fallback, question.strip(), context)
            fallback_validation_result = "PASSED"
            return completed(fallback)
        except SafetyValidationError as fallback_error:
            fallback_validation_result = (
                "FAILED:" + ",".join(fallback_error.codes)
            )
            raise
        except AnswerValidationError:
            fallback_validation_result = "FAILED_CONTRACT"
            raise

    try:
        provider_call_count = 1
        answer = provider.answer(question.strip(), context)
        try:
            answer = validate_answer(answer, question.strip(), context)
            initial_validation_result = "PASSED"
            return completed(answer)
        except SafetyValidationError as initial_error:
            initial_validation_codes = ",".join(initial_error.codes)
            initial_validation_result = "FAILED:" + ",".join(initial_error.codes)
            repair_method = getattr(provider, "repair", None)
            repairable = initial_error.codes == (
                ValidationErrorCode.UNSUPPORTED_CAUSAL_WORDING,
            )
            if provider.name != "openai" or not repairable or not callable(repair_method):
                raise
            repair_attempted = True
            repair_reason = ValidationErrorCode.UNSUPPORTED_CAUSAL_WORDING.value
            provider_call_count = 2
            try:
                repaired = repair_method(
                    answer,
                    tuple(code.value for code in initial_error.codes),
                    context,
                )
                repaired = validate_repair_contract(answer, repaired)
                repaired = validate_answer(repaired, question.strip(), context)
                repair_validation_codes = "NONE"
                repaired_validation_result = "PASSED"
                return completed(repaired)
            except SafetyValidationError as repaired_error:
                repair_validation_codes = ",".join(repaired_error.codes)
                repaired_validation_result = "FAILED:" + ",".join(repaired_error.codes)
                repair_discarded = True
                return validated_deterministic_fallback()
            except (AnswerValidationError, ProviderError) as repaired_error:
                repair_validation_codes = (
                    "PROVIDER_ERROR"
                    if isinstance(repaired_error, ProviderError)
                    else "ANSWER_CONTRACT_ERROR"
                )
                repaired_validation_result = "FAILED:" + repair_validation_codes
                repair_discarded = True
                return validated_deterministic_fallback()
    except (AnswerValidationError, ProviderError):
        raise
    except Exception:
        raise ProviderError("Narration failed safely") from None
    finally:
        logging.getLogger("pulse.intelligence").info(
            "analyst_answer business_id=%s provider=%s model=%s category=%s "
            "evidence_count=%d latency_ms=%.2f initial_validation_result=%s "
            "initial_validation_codes=%s repair_attempted=%s repair_reason=%s "
            "repair_validation_codes=%s repaired_validation_result=%s "
            "repair_discarded=%s provider_call_count=%d "
            "deterministic_fallback_used=%s fallback_intent=%s "
            "fallback_validation_result=%s validation_success=%s",
            context.business_id, provider.name, provider.model, question_category(question),
            len(context.evidence_items), (time.perf_counter() - started) * 1000,
            initial_validation_result, initial_validation_codes, repair_attempted,
            repair_reason, repair_validation_codes, repaired_validation_result,
            repair_discarded, provider_call_count, deterministic_fallback_used,
            fallback_intent, fallback_validation_result, success,
        )


def answer_question(
    question: str, context: IntelligenceContext, provider: NarrationProvider
) -> AnalystAnswer:
    """Return the validated answer for existing CLI and library callers."""
    return answer_question_with_metadata(question, context, provider).answer


def render_answer_markdown(answer: AnalystAnswer) -> str:
    lines = [
        "# Pulse AI Analyst", "", "Question:", answer.question, "", "Answer:",
        answer.answer_summary, "", "Key findings:",
    ]
    for index, finding in enumerate(answer.findings, 1):
        lines.extend((
            f"{index}. {finding.statement}",
            f"   Evidence: [{', '.join(finding.evidence_refs)}]",
            f"   Confidence: {finding.confidence.value}",
        ))
    lines.extend(("", "Investigate:"))
    lines.extend(f"- {step}" for step in answer.investigation_steps)
    lines.extend(("", "Limitations:"))
    lines.extend(f"- {limitation}" for limitation in answer.limitations)
    if answer.safety_notes:
        lines.extend(("", "Safety notes:"))
        lines.extend(f"- {note}" for note in answer.safety_notes)
    return "\n".join(lines)
