"""List deterministic cross-domain investigation opportunities."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

import psycopg

from src.intelligence.context import build_context
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import (
    InvestigationOpportunity,
    OpportunityEvaluation,
    OpportunityValidationError,
)


def render_markdown(evaluation: OpportunityEvaluation, *, show_suppressed: bool) -> str:
    lines = [f"# Pulse opportunities - {evaluation.business_id}", ""]
    if not evaluation.opportunities:
        lines.extend(("No active investigation opportunities.", ""))
    for opportunity in evaluation.opportunities:
        lines.extend(_render_opportunity(opportunity))
    if show_suppressed:
        lines.extend(("## Suppressed candidates", ""))
        if not evaluation.suppressed_candidates:
            lines.extend(("None.", ""))
        for item in evaluation.suppressed_candidates:
            lines.extend((
                f"- {item.rule_id} | {item.scope_name} | {item.reason_code.value}",
                f"  Evidence: {', '.join(item.evidence_refs) or 'none'}",
                f"  Reason: {item.explanation}",
            ))
    return "\n".join(lines).rstrip()


def _render_opportunity(opportunity: InvestigationOpportunity) -> tuple[str, ...]:
    impact = "Not quantified"
    if opportunity.impact_proxy_value is not None:
        impact = (
            f"{opportunity.impact_proxy_name}={opportunity.impact_proxy_value:g} "
            f"{opportunity.impact_proxy_unit}"
        )
    lines = [
        f"## {opportunity.opportunity_order}. {opportunity.title}",
        f"ID: {opportunity.opportunity_id}",
        f"Priority / confidence: {opportunity.priority.value} / {opportunity.confidence.value}",
        f"Type: {opportunity.opportunity_type.value}",
        f"Scope: {opportunity.scope_name} ({opportunity.scope_type.value})",
        f"Observation: {opportunity.observation_summary}",
        f"Hypothesis to test: {opportunity.hypothesis_to_test}",
        f"Impact proxy: {impact}",
        "Supporting evidence: " + ", ".join(opportunity.supporting_evidence_refs),
        "Counter evidence: " + (", ".join(opportunity.counter_evidence_refs) or "None"),
        "Blocking evidence: " + (", ".join(opportunity.blocking_evidence_refs) or "None"),
        "Investigation steps:",
        *(f"- {step}" for step in opportunity.investigation_steps),
        "Confirmation criteria:",
        *(f"- {criterion}" for criterion in opportunity.confirmation_criteria),
        "Refutation criteria:",
        *(f"- {criterion}" for criterion in opportunity.refutation_criteria),
        f"Decision unlocked: {opportunity.decision_unlocked}",
        "Missing evidence: " + (", ".join(opportunity.missing_evidence) or "None"),
        f"Limitation: {opportunity.limitation}",
        "",
    ]
    return tuple(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    listing = subparsers.add_parser("list", help="List validated opportunities")
    listing.add_argument("--business-id", required=True)
    listing.add_argument("--format", choices=("markdown", "json"), default="markdown")
    listing.add_argument("--show-suppressed", action="store_true")
    args = parser.parse_args(argv)
    try:
        evaluation = evaluate_opportunities(build_context(args.business_id))
        if args.format == "json":
            print(json.dumps(
                evaluation.to_dict(include_suppressed=args.show_suppressed),
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ))
        else:
            print(render_markdown(evaluation, show_suppressed=args.show_suppressed))
        return 0
    except (LookupError, ValueError, psycopg.Error, OpportunityValidationError) as exc:
        print(f"Pulse opportunity error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
