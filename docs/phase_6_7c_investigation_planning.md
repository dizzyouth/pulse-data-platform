# Phase 6.7C - Evidence Gap and Investigation Planning

Phase 6.7C turns active Phase 6.7A opportunities into bounded, deterministic
investigation plans. It does not change opportunity eligibility, priority,
confidence, evidence, blockers, suppression, or business calculations. It also
does not execute a task or business action.

## Product boundary

An opportunity explains why an investigation deserves attention and owns the
hypothesis, evidence buckets, confirmation and refutation criteria, future
decision, and limitations. An investigation task is one analytical check. An
evidence requirement states what validated aggregate evidence the check needs.
An evidence gap means only that the requirement is not available in the current
validated `IntelligenceContext`; it does not mean the evidence or source does
not exist elsewhere in the business.

The engine is local and deterministic:

```text
curated marts / DecisionSignals
  -> build_context
  -> evaluate_opportunities
  -> active opportunities only
  -> plan and requirement registries
  -> evidence resolution and readiness
  -> InvestigationPlan / InvestigationPortfolio
  -> standalone CLI
```

There is no LLM, narration parsing, new SQL, warehouse write, connector,
automated collection, task state, or API/UI integration in this phase.

## Models and registries

The immutable contracts are `EvidenceRequirement`, `InvestigationTask`,
`InvestigationPlan`, `EvidenceGap`, and `InvestigationPortfolio`. Their enums
make task kind, requirement status, readiness, and plan status explicit. Plan,
task, requirement, and gap identities are normalized and deterministic; no UUID
or ranking score is used.

The explicit plan registry is keyed by the four existing opportunity types:

- confirmation leakage;
- acquisition/fulfillment misalignment;
- measurement reconciliation;
- acquisition efficiency review.

The requirement registry uses exact selectors over validated context evidence.
For example, a campaign diagnostic satisfies a campaign-diagnostic requirement
but does not satisfy campaign offer-mix or below-campaign cohort requirements.
Future evidence dimensions resolve only when their exact aggregate marker is
present. Missing requirements use the wording “Not available in the current
validated intelligence context.” Collection hints request validated aggregate
context evidence and do not invent APIs, connectors, tables, files, or owners.

## Readiness and plan status

Task readiness is categorical:

- `READY_NOW`: every hard requirement is available in current context.
- `PARTIALLY_READY`: useful relevant evidence exists, but the complete
  requirement is not satisfied.
- `BLOCKED_MISSING_EVIDENCE`: at least one hard requirement is absent.
- `BLOCKED_BOUNDARY`: a validated safety or economic boundary prevents the
  analysis.

`READY` means every task is ready. `PARTIAL` means at least one task can begin
and another is partial or blocked. `BLOCKED` means no task can meaningfully
begin. No percentage-complete or opaque readiness score is produced.

## Traceability and hypothesis linkage

Every available requirement carries real evidence IDs from the same-business
context. Product IDs such as opportunity, plan, and task IDs are never evidence.
Each ordinary task reuses the opportunity's confirmation and refutation
criteria. Coverage tasks are explicitly contextual and cannot manufacture new
hypothesis criteria. Expected outputs describe analytical artifacts, not
predetermined conclusions, and completion criteria describe evidence-based
completion conditions.

The validator fails closed on active-opportunity membership, stable identities,
duplicate or unexpected tasks, registered requirements, exact evidence
resolution, same-business references, readiness, ordering, unresolved
requirements, recommended starts, criterion provenance, causal language,
autonomous actions, PII-shaped content, and economic claims.

## Portfolio ordering and shared gaps

Plans preserve the Phase 6.7A order: priority (`HIGH`, `MEDIUM`, `LOW`) followed
by `opportunity_order`. Tasks are ordered by readiness (`READY_NOW`,
`PARTIALLY_READY`, `BLOCKED_MISSING_EVIDENCE`, `BLOCKED_BOUNDARY`) and then their
registered task order.

The recommended business start is the first ready task in the highest-priority
active opportunity. If no task is ready, the first partially ready task is used;
otherwise it is `None`.

Missing requirements are deduplicated into stable business-level evidence gaps.
Each gap retains deterministic affected-opportunity, affected-task, and
opportunity-priority lists. A shared gap is planning context, not proof that a
hypothesis is correct or that data collection should run automatically.

## Safety

Plans are aggregate and analytical only. They cannot pause campaigns, change
budgets or providers, contact customers, issue refunds, execute SQL, trigger
ETL, deploy changes, or mutate any external system. `autonomous_action` is
always false.

Existing causal guards remain authoritative and are supplemented with explicit
investigation phrasing checks. Plans can compare evidence or strengthen/weaken
an untested hypothesis; they cannot state a cause. `FX_REQUIRED` remains
authoritative: the planner cannot introduce profit, contribution, margin, MER,
ROAS, ROI, recovered-revenue, revenue-upside, or promised financial outcomes.
No customer names, phones, emails, addresses, tracking numbers, order IDs, lead
IDs, or raw rows belong in models, output, fixtures, or logs.

## CLI

Build all active plans in Markdown:

```powershell
python -m src.intelligence.investigation_cli plan --business-id sama_cod_pilot
```

Emit deterministic JSON or select one active opportunity:

```powershell
python -m src.intelligence.investigation_cli plan --business-id sama_cod_pilot --format json
python -m src.intelligence.investigation_cli plan --business-id sama_cod_pilot --opportunity-id <stable-opportunity-id>
```

The CLI reads the bounded context once, evaluates opportunities once, and uses
no narration provider or network service. An unknown or currently suppressed
opportunity ID is rejected.

## Limitations and future integration

Readiness reflects only the validated context at its `as_of_date`. It is not a
claim about source availability elsewhere, task ownership, task completion, or
the truth of a hypothesis. The engine performs no forecasting, causal
inference, ROI estimation, persistent tracking, assignment, or notification.
API and Ask Pulse integration are deliberately deferred to a future phase after
engine validation.
