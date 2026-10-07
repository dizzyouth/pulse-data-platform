# Phase 6.8B — Decision Readiness API and Ask Pulse integration

Phase 6.8B exposes the validated Phase 6.8A decision-readiness portfolio through
the existing local Analyst API and Ask Pulse page. It is a product layer, not a
second readiness engine: decision frames, requirements, readiness states,
reason codes, ordering, boundaries, and first-reviewable selection all come
unchanged from `build_decision_readiness_portfolio(...)`.

## API

`GET /api/v1/analyst/decisions?business_id=<id>` returns the current validated
portfolio. Its assessments preserve engine ordering and fields, including
supporting, counter, and blocking evidence references; required and unresolved
requirements; relevant and next-evidence tasks; the decision boundary; and the
human-agency flags. The three summary counts and
`first_reviewable_decision_id` are engine output, not API or browser
recalculations.

`GET /api/v1/analyst/decisions/{decision_id}?business_id=<id>` resolves a detail
only against the current business-scoped portfolio. The ID must have the stable
Phase 6.8A shape, identify an active assessment, originate from an active
opportunity, and refer to its active investigation plan. It is never used in a
warehouse query. Invalid, unknown, or engine-invalid states return controlled
`INVALID_DECISION_ID`, `DECISION_NOT_FOUND`, or
`DECISION_VALIDATION_FAILED` errors.

Neither endpoint exposes suppressed candidates, raw intelligence context,
warehouse rows, SQL, or private source metadata.

## Ask decision mode

`POST /api/v1/analyst/ask` accepts an optional `decision_id`. A request can
select at most one context mode: opportunity, investigation task, or decision.
Ordinary, opportunity, and investigation behavior is unchanged when
`decision_id` is absent.

For a decision question, the service builds the context, opportunity
evaluation, investigation portfolio, and decision portfolio once each. It then
resolves the selected assessment and produces a deterministic answer for these
intents:

- decision overview;
- why ready;
- why not ready;
- missing evidence;
- next evidence;
- decision boundary;
- priority versus readiness; and
- human review and recommendation meaning.

The response reports `provider=deterministic`,
`model=decision-readiness-engine-v1`,
`answer_source=deterministic_decision_readiness`, and zero provider calls. This
is the primary decision-answer path, not a fallback. It makes no OpenAI or
other network request and every answer passes the existing grounded-answer
validator.

## Priority, readiness, and human agency

Opportunity priority asks how urgently an issue deserves investigation.
Decision readiness asks whether the current evidence is sufficient to put one
bounded question before a human reviewer. They are orthogonal: a HIGH-priority,
HIGH-confidence opportunity can contain both a reviewable investigation
direction and an operational-change decision that still needs evidence. A LOW
priority does not contradict a reviewable bounded decision either.

`READY_FOR_HUMAN_REVIEW` means only that the current evidence satisfies the
frame's bounded review threshold. It does not mean approved, recommended, safe
to execute, or selected. Every assessment requires human review, forbids
autonomous action, and displays its engine-owned decision boundary. Ask Pulse
does not add approval state, assignments, persistence, or execution controls.

`NEEDS_MORE_EVIDENCE` retains the unresolved Phase 6.7C requirement IDs. Ask
answers resolve those IDs against the selected plan and use its existing
requirement name, description, validated-context status, missing reason, and
collection hint. They do not invent connectors, tables, files, APIs, or data
owners. Suggested investigations come only from
`next_evidence_task_ids`; task readiness means work can begin, not that the
investigation is complete.

`BLOCKED_BY_BOUNDARY` preserves an explicit engine boundary such as unavailable
trusted economics. The product does not recast a boundary as an ordinary data
collection gap.

## IDs are not evidence

Decision, opportunity, plan, task, requirement, and evidence-gap IDs identify
product objects. They can appear as associations or limitations, but never as
finding evidence references. Decision answers cite only actual aggregate
`IntelligenceContext` evidence IDs, such as `signal:...`, `leakage:...`,
`campaign:...`, and `anomaly:...`.

## Browser experience

The existing page loads opportunities, investigations, and decisions once in a
parallel initial batch. It does not make per-card detail requests. A Decision
readiness section shows API-provided counts, the API-selected first reviewable
question, engine-ordered cards, accessible text badges, prominent boundaries,
and collapsed technical details. Associations use stable opportunity and plan
IDs, with a safe degraded label if an associated object is unexpectedly absent.

Opportunity cards include presentation-only counts of associated assessments.
Decision action buttons send only `business_id`, `question`, and `decision_id`
to the existing answer area. There are no approve, execute, campaign-change,
data-collection, or investigation-trigger controls, and no conversation or
review state is persisted.

## Local development limitations

This remains a local development surface without authentication, authorization,
TLS termination, rate limiting, durable review workflow, or deployment
hardening. It must not be exposed publicly as-is. The product is aggregate-only,
does not establish causality, does not forecast outcomes or ROI, and preserves
`FX_REQUIRED` wherever trusted cross-currency economics are unavailable.

Run locally with the offline provider:

```powershell
$env:PULSE_LLM_PROVIDER = "fake"
python -m src.api.app
```

Then open `http://127.0.0.1:8088/`.
