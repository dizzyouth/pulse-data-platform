"""Build deterministic decision-readiness assessments offline."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

import psycopg

from src.intelligence.context import build_context
from src.intelligence.decision_models import (
    DecisionReadinessPortfolio,
    DecisionValidationError,
)
from src.intelligence.decisions import build_decision_readiness_portfolio
from src.intelligence.investigation_models import InvestigationValidationError
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import OpportunityEvaluation


def render_markdown(portfolio: DecisionReadinessPortfolio) -> str:
    """Render aggregate-only readiness without narration or provider calls."""
    lines = [f"# Pulse decision readiness - {portfolio.business_id}", ""]
    if not portfolio.assessments:
        lines.extend(("No active opportunities have decision-readiness frames.", ""))
    for item in portfolio.assessments:
        lines.extend((
            f"## {item.decision_order}. {item.decision_question}",
            f"Decision ID: {item.decision_id}",
            f"Decision class: {item.decision_class.value}",
            f"Originating opportunity: {item.originating_opportunity_id}",
            f"Readiness: {item.readiness.value}",
            "",
            "Why:",
            f"- Reason codes: {', '.join(code.value for code in item.readiness_reason_codes)}",
            f"- Supporting evidence: {', '.join(item.supporting_evidence_refs)}",
            "- Counter evidence: " + (", ".join(item.counter_evidence_refs) or "None"),
            "- Blockers: " + (", ".join(item.blocking_evidence_refs) or "None"),
            "- Unresolved requirements: "
            + (", ".join(item.unresolved_requirement_ids) or "None"),
            f"- Rationale: {item.rationale_summary}",
            "",
            "Tasks that could raise readiness:",
            *(f"- {task_id}" for task_id in item.next_evidence_task_ids),
        ))
        if not item.next_evidence_task_ids:
            lines.append("- None under the current frame")
        lines.extend((
            "",
            f"Decision boundary: {item.decision_boundary}",
            "Human review required: Yes",
            "Autonomous action allowed: No",
            f"Limitation: {item.limitation}",
            "",
        ))
    lines.extend((
        f"Ready for human review: {portfolio.ready_for_human_review_count}",
        f"Needs more evidence: {portfolio.needs_more_evidence_count}",
        f"Blocked by boundary: {portfolio.blocked_by_boundary_count}",
        "First reviewable decision: "
        + (portfolio.first_reviewable_decision_id or "None"),
        "",
        "Portfolio limitation: readiness supports human review only; it is not a "
        "recommendation, authorization, selected option, or autonomous action.",
    ))
    return "\n".join(lines).rstrip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    readiness = subparsers.add_parser(
        "readiness", help="Build active-opportunity decision readiness"
    )
    readiness.add_argument("--business-id", required=True)
    readiness.add_argument("--format", choices=("markdown", "json"), default="markdown")
    readiness.add_argument("--opportunity-id")
    args = parser.parse_args(argv)
    try:
        context = build_context(args.business_id)
        evaluation = evaluate_opportunities(context)
        if args.opportunity_id:
            selected = tuple(
                item for item in evaluation.opportunities
                if item.opportunity_id == args.opportunity_id
            )
            if not selected:
                raise DecisionValidationError(
                    "Requested opportunity is not active in the current validated context"
                )
            evaluation = OpportunityEvaluation(
                business_id=evaluation.business_id,
                as_of_date=evaluation.as_of_date,
                opportunities=selected,
                suppressed_candidates=(),
            )
        investigation_portfolio = build_investigation_portfolio(context, evaluation)
        portfolio = build_decision_readiness_portfolio(
            context, evaluation, investigation_portfolio
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
        InvestigationValidationError,
        DecisionValidationError,
    ) as exc:
        print(f"Pulse decision-readiness error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
