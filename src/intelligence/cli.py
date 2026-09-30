"""Read deterministic decision intelligence from the local warehouse."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import sys
from typing import Sequence

import psycopg
from psycopg.rows import dict_row

from src.intelligence.models import DecisionSignal
from src.intelligence.answer_models import AnswerValidationError
from src.intelligence.context import build_context
from src.intelligence.narration import answer_question, question_category, render_answer_markdown
from src.intelligence.providers import OfflineFakeProvider, OpenAIProvider, ProviderError
from src.warehouse.load_gold import connection_kwargs


SIGNAL_COLUMNS = tuple(DecisionSignal.__dataclass_fields__)


def load_signals(business_id: str) -> list[DecisionSignal]:
    columns = ", ".join(SIGNAL_COLUMNS)
    with psycopg.connect(**connection_kwargs(), row_factory=dict_row) as connection:
        connection.read_only = True
        rows = connection.execute(
            f"SELECT {columns} FROM marts.sama_pilot_intelligence_signals "
            "WHERE business_id = %s ORDER BY signal_order",
            (business_id,),
        ).fetchall()
    return [DecisionSignal.from_row(row) for row in rows]


def render_markdown(signals: Sequence[DecisionSignal], business_id: str) -> str:
    lines = [f"# Pulse intelligence brief — {business_id}", ""]
    if not signals:
        return "\n".join((*lines, "No active deterministic signals."))
    for index, signal in enumerate(signals, 1):
        baseline = f"{signal.baseline_value:.4g}"
        observed = f"{signal.observed_value:.4g}"
        if signal.impact_order_count is None:
            impact = "Not quantified"
        elif signal.scope_type == "CAMPAIGN":
            impact = f"{signal.impact_order_count:.2f} benchmark-gap orders"
        else:
            impact = f"{signal.impact_order_count:.2f} observed affected orders"
        lines.extend((
            f"## {index}. {signal.signal_type} — {signal.priority}",
            f"Scope: {signal.scope_name} ({signal.scope_type})",
            f"Evidence: {signal.evidence_summary}",
            f"Observed vs baseline: {observed} vs {baseline}",
            f"Estimated magnitude: {impact}",
            f"Why it matters: {signal.why_it_matters}",
            f"Investigate: {signal.recommended_next_step}",
            f"Confidence: {signal.confidence}",
            f"Limitation: {signal.limitation}",
            "",
        ))
    return "\n".join(lines).rstrip()


def _provider(name: str):
    if name == "fake":
        return OfflineFakeProvider()
    return OpenAIProvider.from_env()


def _run_ask(args: argparse.Namespace) -> int:
    context = build_context(args.business_id)
    provider_name = args.provider or os.environ.get("PULSE_LLM_PROVIDER") or "fake"
    if provider_name not in {"fake", "openai"}:
        raise ProviderError("PULSE_LLM_PROVIDER must be fake or openai")
    answer = answer_question(args.question, context, _provider(provider_name))
    if args.format == "json":
        print(json.dumps(answer.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(render_answer_markdown(answer))
    return 0


def _run_live_acceptance(args: argparse.Namespace) -> int:
    if os.environ.get("RUN_PULSE_LLM_ACCEPTANCE") != "1":
        raise ProviderError(
            "Live acceptance is disabled; set RUN_PULSE_LLM_ACCEPTANCE=1 to opt in"
        )
    context = build_context(args.business_id)
    provider = OpenAIProvider.from_env()
    questions = (
        "What should I investigate first?",
        "Why are returns high?",
        "Should I pause Sama-NewUM?",
        "How much profit am I making?",
        "Did something abnormal happen today?",
    )
    results = []
    for question in questions:
        answer_question(question, context, provider)
        results.append({
            "question_category": question_category(question), "validation_success": True
        })
    print(json.dumps({
        "provider": provider.name,
        "model": provider.model,
        "business_id": context.business_id,
        "evidence_count": len(context.evidence_items),
        "results": results,
    }, indent=2))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    brief = subparsers.add_parser("brief", help="Render the current signal brief")
    brief.add_argument("--business-id", required=True)
    brief.add_argument("--format", choices=("markdown", "json"), default="markdown")
    brief.add_argument("--limit", type=int, default=5)
    ask = subparsers.add_parser("ask", help="Ask a single grounded analyst question")
    ask.add_argument("--business-id", required=True)
    ask.add_argument("--question", required=True)
    ask.add_argument("--provider", choices=("fake", "openai"))
    ask.add_argument("--format", choices=("markdown", "json"), default="markdown")
    acceptance = subparsers.add_parser(
        "live-acceptance", help="Run the guarded live OpenAI smoke questions"
    )
    acceptance.add_argument("--business-id", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "ask":
            return _run_ask(args)
        if args.command == "live-acceptance":
            return _run_live_acceptance(args)
        if args.limit < 1:
            parser.error("--limit must be positive")
        signals = load_signals(args.business_id)[:args.limit]
        if args.format == "json":
            print(json.dumps([asdict(signal) for signal in signals], default=str, indent=2))
        else:
            print(render_markdown(signals, args.business_id))
        return 0
    except (AnswerValidationError, ProviderError, LookupError, ValueError) as exc:
        print(f"Pulse analyst error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
