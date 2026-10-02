"""Build deterministic evidence-gap and investigation plans."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

import psycopg

from src.intelligence.context import build_context
from src.intelligence.investigation_models import (
    InvestigationPortfolio,
    InvestigationValidationError,
)
from src.intelligence.investigations import build_investigation_portfolio
from src.intelligence.opportunities import evaluate_opportunities
from src.intelligence.opportunity_models import OpportunityEvaluation


def render_markdown(
    portfolio: InvestigationPortfolio,
    evaluation: OpportunityEvaluation,
) -> str:
    """Render aggregate-only plans without provider or narration calls."""
    opportunities = {item.opportunity_id: item for item in evaluation.opportunities}
    lines = [f"# Pulse investigation plans - {portfolio.business_id}", ""]
    if not portfolio.plans:
        lines.extend(("No active investigation opportunities.", ""))
    for plan in portfolio.plans:
        opportunity = opportunities[plan.opportunity_id]
        lines.extend((
            f"## Opportunity: {opportunity.title}",
            f"Opportunity ID: {plan.opportunity_id}",
            f"Priority / confidence: {plan.opportunity_priority.value} / "
            f"{plan.opportunity_confidence.value}",
            f"Plan status: {plan.status.value}",
            "",
            "### Tasks",
            "",
        ))
        for task in plan.tasks:
            links = (*task.strengthens_criteria, *task.weakens_criteria)
            lines.extend((
                f"{task.task_order}. [{task.readiness.value}] {task.title}",
                f"   Objective: {task.objective}",
                "   Available evidence: "
                + (", ".join(task.available_evidence_refs) or "None in current context"),
                "   Missing requirements: "
                + (", ".join(task.missing_requirement_ids) or "None"),
                f"   Expected output: {task.expected_output}",
                "   Confirmation/refutation link: "
                + (" | ".join(links) or "Contextual or coverage evidence"),
                "   Completion criteria: " + " | ".join(task.completion_criteria),
                f"   Limitation: {task.limitation}",
                "",
            ))
        lines.extend((
            f"Decision unlocked: {plan.decision_unlocked}",
            f"Plan limitation: {plan.limitation}",
            "",
        ))
    lines.extend(("## Evidence gaps", ""))
    if not portfolio.evidence_gaps:
        lines.extend(("None in the current validated intelligence context.", ""))
    for gap in portfolio.evidence_gaps:
        lines.extend((
            f"- {gap.requirement_id}",
            f"  Affected opportunities: {', '.join(gap.affected_opportunity_ids)}",
            f"  Affected tasks: {', '.join(gap.affected_task_ids)}",
            f"  Limitation: {gap.limitation}",
        ))
    lines.extend((
        "",
        "Recommended start: " + (portfolio.recommended_start_task_id or "None"),
        "Portfolio limitation: availability means availability in the current validated "
        "intelligence context only; Pulse does not execute tasks or business actions.",
    ))
    return "\n".join(lines).rstrip()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan", help="Build active-opportunity investigation plans")
    plan.add_argument("--business-id", required=True)
    plan.add_argument("--format", choices=("markdown", "json"), default="markdown")
    plan.add_argument("--opportunity-id")
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
                raise InvestigationValidationError(
                    "Requested opportunity is not active in the current validated context"
                )
            evaluation = OpportunityEvaluation(
                business_id=evaluation.business_id,
                as_of_date=evaluation.as_of_date,
                opportunities=selected,
                suppressed_candidates=(),
            )
        portfolio = build_investigation_portfolio(context, evaluation)
        if args.format == "json":
            print(json.dumps(
                portfolio.to_dict(),
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
                allow_nan=False,
            ))
        else:
            print(render_markdown(portfolio, evaluation))
        return 0
    except (LookupError, ValueError, psycopg.Error, InvestigationValidationError) as exc:
        print(f"Pulse investigation error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
