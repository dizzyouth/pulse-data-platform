"""Provider-neutral, fail-closed contracts for grounded analyst answers."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Sequence


class AnswerValidationError(ValueError):
    """A provider answer did not satisfy the local Pulse contract."""


class Confidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class ClaimType(StrEnum):
    OBSERVATION = "OBSERVATION"
    LIMITATION = "LIMITATION"
    INVESTIGATION = "INVESTIGATION"


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AnswerValidationError(f"{field} must be non-empty text")
    return value.strip()


def _string_list(value: Any, field: str, *, allow_empty: bool = True) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or isinstance(value, (str, bytes)):
        raise AnswerValidationError(f"{field} must be a list")
    items = tuple(_text(item, field) for item in value)
    if not allow_empty and not items:
        raise AnswerValidationError(f"{field} cannot be empty")
    if len(items) != len(set(items)):
        raise AnswerValidationError(f"{field} cannot contain duplicates")
    return items


@dataclass(frozen=True, slots=True, kw_only=True)
class Finding:
    statement: str
    evidence_refs: tuple[str, ...]
    confidence: Confidence
    claim_type: ClaimType = ClaimType.OBSERVATION
    causal_claim: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "statement", _text(self.statement, "finding.statement"))
        object.__setattr__(self, "evidence_refs", _string_list(
            self.evidence_refs, "finding.evidence_refs", allow_empty=False
        ))
        try:
            object.__setattr__(self, "confidence", Confidence(self.confidence))
            object.__setattr__(self, "claim_type", ClaimType(self.claim_type))
        except ValueError as exc:
            raise AnswerValidationError(str(exc)) from None
        if not isinstance(self.causal_claim, bool):
            raise AnswerValidationError("finding.causal_claim must be boolean")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "Finding":
        allowed = {"statement", "evidence_refs", "confidence", "claim_type", "causal_claim"}
        if not isinstance(value, Mapping) or set(value) != allowed:
            raise AnswerValidationError("finding has missing or unsupported fields")
        return cls(
            statement=value["statement"],
            evidence_refs=value["evidence_refs"],
            confidence=value["confidence"],
            claim_type=value["claim_type"],
            causal_claim=value["causal_claim"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "statement": self.statement,
            "evidence_refs": list(self.evidence_refs),
            "confidence": self.confidence.value,
            "claim_type": self.claim_type.value,
            "causal_claim": self.causal_claim,
        }


def derive_evidence_refs(findings: Sequence[Finding]) -> tuple[str, ...]:
    """Return the stable first-seen union of authoritative finding references."""
    seen: set[str] = set()
    refs: list[str] = []
    for finding in findings:
        for evidence_ref in finding.evidence_refs:
            if evidence_ref not in seen:
                seen.add(evidence_ref)
                refs.append(evidence_ref)
    return tuple(refs)


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderAnswer:
    """Structured provider output; findings own all model-generated grounding refs."""

    question: str
    answer_summary: str
    findings: tuple[Finding, ...]
    investigation_steps: tuple[str, ...]
    limitations: tuple[str, ...]
    confidence: Confidence
    cannot_answer_fully: bool
    safety_notes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "question", _text(self.question, "question"))
        object.__setattr__(self, "answer_summary", _text(
            self.answer_summary, "answer_summary"
        ))
        if not isinstance(self.findings, (list, tuple)) or not self.findings:
            raise AnswerValidationError("findings must be a non-empty list")
        findings = tuple(self.findings)
        if not all(isinstance(item, Finding) for item in findings):
            raise AnswerValidationError("findings contains a malformed item")
        object.__setattr__(self, "findings", findings)
        for field in ("investigation_steps", "limitations", "safety_notes"):
            object.__setattr__(self, field, _string_list(getattr(self, field), field))
        try:
            object.__setattr__(self, "confidence", Confidence(self.confidence))
        except ValueError as exc:
            raise AnswerValidationError(str(exc)) from None
        if not isinstance(self.cannot_answer_fully, bool):
            raise AnswerValidationError("cannot_answer_fully must be boolean")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ProviderAnswer":
        allowed = {
            "question", "answer_summary", "findings", "investigation_steps",
            "limitations", "confidence", "cannot_answer_fully", "safety_notes",
        }
        if not isinstance(value, Mapping) or set(value) != allowed:
            raise AnswerValidationError("answer has missing or unsupported fields")
        raw_findings = value["findings"]
        if not isinstance(raw_findings, Sequence) or isinstance(raw_findings, (str, bytes)):
            raise AnswerValidationError("findings must be a list")
        return cls(
            question=value["question"],
            answer_summary=value["answer_summary"],
            findings=tuple(Finding.from_mapping(item) for item in raw_findings),
            investigation_steps=value["investigation_steps"],
            limitations=value["limitations"],
            confidence=value["confidence"],
            cannot_answer_fully=value["cannot_answer_fully"],
            safety_notes=value["safety_notes"],
        )

    def _content_dict(self) -> dict[str, Any]:
        return {
            "question": self.question,
            "answer_summary": self.answer_summary,
            "findings": [finding.to_dict() for finding in self.findings],
            "investigation_steps": list(self.investigation_steps),
            "limitations": list(self.limitations),
            "confidence": self.confidence.value,
            "cannot_answer_fully": self.cannot_answer_fully,
            "safety_notes": list(self.safety_notes),
        }

    def to_dict(self) -> dict[str, Any]:
        return self._content_dict()

    def to_analyst_answer(self) -> "AnalystAnswer":
        return AnalystAnswer(
            question=self.question,
            answer_summary=self.answer_summary,
            findings=self.findings,
            investigation_steps=self.investigation_steps,
            limitations=self.limitations,
            confidence=self.confidence,
            cannot_answer_fully=self.cannot_answer_fully,
            safety_notes=self.safety_notes,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class AnalystAnswer(ProviderAnswer):
    """Final Pulse answer with a deterministic summary of finding evidence refs."""

    evidence_refs: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        ProviderAnswer.__post_init__(self)
        object.__setattr__(self, "evidence_refs", derive_evidence_refs(self.findings))

    def to_dict(self) -> dict[str, Any]:
        value = self._content_dict()
        value["evidence_refs"] = list(self.evidence_refs)
        return value


PROVIDER_ANSWER_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "question", "answer_summary", "findings", "investigation_steps",
        "limitations", "confidence", "cannot_answer_fully", "safety_notes",
    ],
    "properties": {
        "question": {"type": "string"},
        "answer_summary": {"type": "string"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object", "additionalProperties": False,
                "required": [
                    "statement", "evidence_refs", "confidence", "claim_type",
                    "causal_claim",
                ],
                "properties": {
                    "statement": {"type": "string"},
                    "evidence_refs": {
                        "type": "array", "items": {"type": "string"},
                    },
                    "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
                    "claim_type": {
                        "type": "string",
                        "enum": ["OBSERVATION", "LIMITATION", "INVESTIGATION"],
                    },
                    "causal_claim": {"type": "boolean"},
                },
            },
        },
        "investigation_steps": {
            "type": "array", "items": {"type": "string"},
        },
        "limitations": {
            "type": "array", "items": {"type": "string"},
        },
        "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
        "cannot_answer_fully": {"type": "boolean"},
        "safety_notes": {
            "type": "array", "items": {"type": "string"},
        },
    },
}
