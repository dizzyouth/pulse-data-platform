"""Read deterministic decision intelligence from the local warehouse."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from typing import Sequence

import psycopg
from psycopg.rows import dict_row

from src.intelligence.models import DecisionSignal
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    brief = subparsers.add_parser("brief", help="Render the current signal brief")
    brief.add_argument("--business-id", required=True)
    brief.add_argument("--format", choices=("markdown", "json"), default="markdown")
    brief.add_argument("--limit", type=int, default=5)
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("--limit must be positive")
    signals = load_signals(args.business_id)[:args.limit]
    if args.format == "json":
        print(json.dumps([asdict(signal) for signal in signals], default=str, indent=2))
    else:
        print(render_markdown(signals, args.business_id))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
