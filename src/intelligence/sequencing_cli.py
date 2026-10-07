"""Build deterministic evidence leverage and investigation sequencing offline."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

import psycopg

from src.intelligence.context import build_context
from src.intelligence.decision_models import DecisionValidationError
from src.intelligence.decisions import build_decision_readiness_portfolio
from src.intelligence.investigation_models import InvestigationValidationError
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import OpportunityValidationError
from src.intelligence.sequencing import build_investigation_sequencing_portfolio
from src.intelligence.sequencing_models import (
    InvestigationSequencingPortfolio,
    SequencingValidationError,
)


def render_markdown(portfolio: InvestigationSequencingPortfolio) -> str:
    """Render sequencing without narration, execution, or provider calls."""
    leverage_by_id = {
        item.requirement_id: item for item in portfolio.evidence_leverage_items
    }
    sequence_by_id = {item.task_id: item for item in portfolio.sequence_items}
    top = (
        leverage_by_id.get(portfolio.top_evidence_focus_requirement_id)
        if portfolio.top_evidence_focus_requirement_id
        else None
    )
    recommended = (
        sequence_by_id.get(portfolio.recommended_next_task_id)
        if portfolio.recommended_next_task_id
        else None
    )
    lines = [
        f"# Pulse evidence leverage and investigation sequencing - {portfolio.business_id}",
        "",
        f"Portfolio state: {portfolio.state.value}",
        "Top evidence focus: "
        + (f"{top.requirement_name} ({top.requirement_id})" if top else "None"),
    ]
    if recommended is not None:
        lines.append(
            "Recommended next startable investigation: "
            f"{recommended.task_title} ({recommended.task_id})"
        )
    elif top is not None:
        lines.append(
            "Recommended next startable investigation: None. No current "
            "readiness-raising investigation can begin with the validated context."
        )
    else:
        lines.append("Recommended next startable investigation: None")
    lines.extend(("", "## Evidence leverage", ""))
    if not portfolio.evidence_leverage_items:
        lines.append("No unresolved evidence requirements from current non-ready decisions.")
    for item in portfolio.evidence_leverage_items:
        lines.extend((
            f"- {item.requirement_name} ({item.requirement_id})",
            f"  Status: {item.requirement_status.value}",
            f"  Affected non-ready decisions: {item.affected_decision_count}",
            f"  Affected opportunities: {item.affected_opportunity_count}",
            f"  Highest opportunity priority: {item.highest_opportunity_priority.value}",
            "  Related readiness-raising tasks: "
            + (", ".join(item.related_task_ids) or "None"),
            "  Existing evidence gap: " + (item.existing_gap_id or "None"),
            f"  Limitation: {item.limitation}",
        ))
    lines.extend(("", "## Investigation sequence", ""))
    if not portfolio.sequence_items:
        lines.append("No readiness-raising investigation tasks are currently linked.")
    for item in portfolio.sequence_items:
        lines.extend((
            f"{item.sequence_order}. {item.task_title}",
            f"   Task ID: {item.task_id}",
            f"   Readiness: {item.task_readiness.value}",
            f"   Can begin now: {'Yes' if item.can_begin_now else 'No'}",
            f"   Affected non-ready decisions: {item.affected_decision_count}",
            "   Addressed requirements: "
            + (", ".join(item.addressed_requirement_ids) or "None"),
            f"   Why sequenced here: {item.sequencing_reason}",
            f"   Limitation: {item.limitation}",
            "",
        ))
    lines.extend((
        "## Boundary-blocked decisions",
        "",
        *(f"- {item}" for item in portfolio.boundary_blocked_decision_ids),
    ))
    if not portfolio.boundary_blocked_decision_ids:
        lines.append("None")
    lines.extend(("", f"Portfolio limitation: {portfolio.limitation}"))
    return "\n".join(lines).rstrip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    sequence = subparsers.add_parser(
        "sequence", help="Build evidence leverage and readiness-raising task order"
    )
    sequence.add_argument("--business-id", required=True)
    sequence.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args(argv)
    try:
        context = build_context(args.business_id)
        evaluation = evaluate_opportunities(context)
        investigations = build_investigation_portfolio(context, evaluation)
        decisions = build_decision_readiness_portfolio(
            context, evaluation, investigations
        )
        portfolio = build_investigation_sequencing_portfolio(
            context, evaluation, investigations, decisions
        )
        if args.format == "json":
            print(json.dumps(
                portfolio.to_dict(),
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ))
        else:
            print(render_markdown(portfolio))
        return 0
    except (
        LookupError,
        ValueError,
        psycopg.Error,
        OpportunityValidationError,
        InvestigationValidationError,
        DecisionValidationError,
        SequencingValidationError,
    ) as exc:
        print(f"Pulse sequencing error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
