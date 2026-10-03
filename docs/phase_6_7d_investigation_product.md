# Phase 6.7D - Investigation Planning API and Ask Pulse Integration

Phase 6.7D is the read-only product layer for the deterministic Phase 6.7C
investigation engine. Phase 6.7C remains the sole owner of plan specifications,
evidence requirements, readiness, plan status, shared-gap deduplication, task
ordering, and recommended-start selection. The API and browser serialize and
render those validated results without reproducing planning logic.

## Architecture

```text
HTTP / Ask Pulse
  -> build_context (once per request)
  -> evaluate_opportunities (once)
  -> active opportunities only
  -> build_investigation_portfolio (once)
  -> thin response contracts / deterministic task answer
  -> existing answer validator
```

There is no LLM planning, task ranking, narration parsing, new SQL, warehouse
write, connector, scheduled collection, persistent task state, assignment,
notification, or autonomous execution.

## API contracts

The aggregate portfolio endpoint is:

```text
GET /api/v1/analyst/investigations?business_id=<business_id>
```

It returns the validated portfolio's plans, tasks, evidence requirements,
business-level evidence gaps, readiness counts, and recommended task ID. It
does not return suppressed candidates, the raw intelligence context, arbitrary
source metadata, SQL, or customer-level records.

Validated active objects also have detail surfaces:

```text
GET /api/v1/analyst/investigations/{plan_id}?business_id=<business_id>
GET /api/v1/analyst/investigation-tasks/{task_id}?business_id=<business_id>
```

Both IDs must have the stable Phase 6.7C shape and resolve from the current
business-scoped active portfolio. The service never queries a warehouse by an
object ID. Unknown or suppressed objects return controlled errors.

## Three Ask modes

`POST /api/v1/analyst/ask` supports exactly three modes:

- ordinary Ask: `business_id` and `question`;
- opportunity Ask: additionally `opportunity_id`;
- investigation-task Ask: additionally `investigation_task_id`.

An opportunity ID and investigation-task ID cannot be supplied together. The
request remains strict with unsupported fields forbidden.

Task questions use a small deterministic classifier covering overview,
readiness, blocking, missing evidence, expected output, completion criteria,
confirmation/refutation linkage, and future decision support. The answer
builder consumes existing task and plan values verbatim; it does not re-plan or
recalculate readiness. Every answer passes the existing grounded-answer
validator.

Task-answer metadata is fixed:

```text
provider = deterministic
model = investigation-engine-v1
answer_source = deterministic_investigation
provider_call_count = 0
repair_attempted = false
deterministic_fallback_used = false
```

## IDs versus evidence

Opportunity, plan, task, requirement, and evidence-gap IDs identify product or
intelligence objects. They are not evidence citations. Findings cite only real
same-business evidence IDs from the validated `IntelligenceContext`, such as
signal, leakage, campaign, anomaly, and economics evidence.

When a blocked task has no direct evidence, its missing requirement is described
as a limitation. The answer may use existing plan-level aggregate grounding but
never substitutes a task, plan, requirement, gap, or opportunity ID as
evidence.

## Availability and shared gaps

`READY_NOW` means only that current validated aggregate evidence is sufficient
to begin the bounded analytical investigation. It does not mean an action is
approved, the task has been executed, or the underlying hypothesis is
confirmed.

Missing means “not available in the current validated intelligence context.” It
does not mean evidence is absent everywhere in the business. Explanations use
only the requirement names, descriptions, missing reasons, and generic
collection hints already validated by Phase 6.7C. They do not invent APIs,
connectors, tables, files, owners, endpoints, credentials, or collection jobs.

Portfolio evidence gaps are shown once and indicate how many investigations and
tasks use the same missing requirement. No gap score, urgency score, financial
value, ROI, or promised outcome is added.

## Ask Pulse UI

The existing Opportunities section loads the opportunity list and investigation
portfolio once each. It associates plans by `opportunity_id`, never by titles or
array position, and makes no per-plan or per-task request during initial render.

The portfolio summary displays API-owned ready, partial, blocked, and gap
counts. The recommended title is resolved from
`portfolio.recommended_start_task_id`; JavaScript does not rank or select it.
Each opportunity card contains a compact plan summary, with task details kept
inside the existing expandable view. Readiness is expressed with text and a
badge so color is not the only signal. Blocked tasks are described as missing
evidence or boundary-limited, not failed.

Task buttons reuse the existing Ask Pulse answer area. A task request sends
`business_id`, `question`, and `investigation_task_id`, never an opportunity ID
at the same time. The answer context identifies the selected task. No browser
history or task state is persisted.

## Safety and limitations

The integration preserves the existing causal, economic, evidence, numeric,
PII, and autonomous-action validators. Better evidence may support the future
decision already named by an opportunity, but Pulse does not prescribe or
execute that decision. Expected outputs are analytical artifacts, not expected
business conclusions, and completion criteria never mean a hypothesis is
proven or an external action is complete.

This remains a localhost development surface without authentication,
authorization, TLS, rate limiting, or deployment hardening. Do not expose it
publicly. A future production phase would require those controls; it would not
change the Phase 6.7C planning source of truth.

## Local use

Start the existing fake-provider server and open Ask Pulse:

```powershell
$env:PULSE_LLM_PROVIDER = "fake"
python -m src.api.app
```

Then visit `http://127.0.0.1:8088/` or query the portfolio endpoint directly.
Investigation-task answers remain provider-free even if another provider is
configured for ordinary Ask.
