# Phase 6.9B evidence sequencing product

Phase 6.9B exposes the validated Phase 6.9A evidence-leverage and investigation-
sequencing portfolio through the local Analyst API and the existing Ask Pulse
page. It answers what the business should learn next. It does not rank, select,
recommend, or execute a business action.

## Engine and product boundary

Phase 6.9A remains the single source of truth for evidence leverage, affected
decision and opportunity counts, ordering, task startability, portfolio state,
top evidence focus, and the next startable task. The API and browser render
those values without recalculating them.

For every sequencing request the service builds the current business context,
opportunity evaluation, investigation portfolio, decision-readiness portfolio,
and sequencing portfolio exactly once. The product layer uses
`build_investigation_sequencing_portfolio(...)`; it does not add an alternative
ordering or readiness rule.

## Portfolio endpoint

`GET /api/v1/analyst/sequencing?business_id=<id>` returns the complete validated
`InvestigationSequencingPortfolio` contract:

- evidence-leverage items in engine order;
- sequence items in engine order;
- the authoritative state and top evidence-focus identifier;
- the authoritative recommended-next-task identifier, which may be null;
- startable and blocked task counts;
- separately identified targeted and boundary-blocked decisions; and
- the engine limitation.

The response contains no raw context, warehouse rows, SQL, private source
metadata, task execution state, owner, completion field, due date, score,
probability, or financial valuation. Detail endpoints are intentionally omitted;
sequencing Ask identifiers resolve only within the current portfolio.

## Sequencing Ask modes

`POST /api/v1/analyst/ask` accepts one of these new optional fields:

- `sequencing_requirement_id` for a current evidence-leverage item; or
- `sequencing_task_id` for a current sequence item.

Exactly one contextual object identifier may be supplied across opportunity,
investigation, decision, sequencing-requirement, and sequencing-task modes.
Malformed identifiers fail closed. Well-formed identifiers must resolve within
the current business-scoped sequencing portfolio; they are never used in SQL or
treated as evidence references.

Supported deterministic explanations cover the portfolio overview, what to
learn next, top evidence focus, transparent leverage ordering, affected
decisions, the next startable task, the absence of a startable task, task order,
task startability, what a task could clarify, and the no-guarantee boundary.

Sequencing answers report:

- provider `deterministic`;
- model `sequencing-engine-v1`;
- source `deterministic_evidence_sequencing`;
- zero provider calls;
- no repair attempt; and
- no deterministic fallback.

Every final answer passes the existing grounded-answer validator. Findings cite
only real same-business `IntelligenceContext` evidence references. Requirement,
gap, task, plan, decision, and opportunity identifiers are intelligence object
associations, not evidence references.

## Evidence-leverage semantics

Top evidence focus means the first unresolved requirement under the explicit
Phase 6.9A ordering. The ordering reflects breadth across current
`NEEDS_MORE_EVIDENCE` decisions, followed by the documented deterministic
tie-breakers. It is not ROI, expected value, business impact, a probability, or
a guarantee that a decision will become ready.

Affected decisions are current assessments that reference the unresolved
requirement. Collecting or validating related evidence might change a later
assessment, but readiness must be recalculated after the evidence actually
exists. The association does not promise an unlock.

## Investigation sequencing semantics

The next startable investigation is present only when the engine supplies a
`recommended_next_task_id`. The browser never substitutes the first blocked
sequence item. `READY_NOW` means the existing analytical task can begin; it does
not mean the work is complete or that new evidence exists.

`EVIDENCE_GAP_FIRST` with a null recommended task is a valid, prominent product
state. It means current decision-readiness gaps and associated investigations
exist, while no readiness-raising investigation can begin with the current
validated context. The product does not describe this state as impossible and
does not claim missing evidence cannot exist elsewhere.

## Ask Pulse presentation

The existing page loads opportunities, investigations, decisions, and
sequencing in one parallel batch with one request per portfolio. The sequencing
section appears below Decision Readiness and includes:

- a human-readable state and compact authoritative summary;
- the top evidence focus and its affected breadth;
- the startable count and next startable investigation;
- an explicit no-startable message when applicable;
- evidence-leverage cards in engine order;
- sequence items in engine order; and
- bounded deterministic Ask actions that reuse the existing answer area.

Associations use stable identifiers. Technical identifiers are secondary and
long values wrap. Details are collapsed by default, layouts remain responsive,
and status is conveyed with text as well as badges.

## Product limits

This phase adds no evidence collection, task execution, task-completion
persistence, provider call, business-action recommendation, approval flow,
owner assignment, notification, connector, database table, orchestration job,
forecast, causal inference, embedding, or vector search. Local development
continues to use the configured aggregate warehouse and the fake provider for
ordinary Ask smoke testing; sequencing Ask itself is provider-free.
