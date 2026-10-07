# Phase 6.9A — Evidence Leverage and Investigation Sequencing

Phase 6.9A is a deterministic, engine-first layer that answers “What should we
learn next?” It does not answer what business action should happen next. It
consumes already-built `IntelligenceContext`, `OpportunityEvaluation`,
`InvestigationPortfolio`, and `DecisionReadinessPortfolio` objects and performs
no SQL, provider call, warehouse write, task execution, or API/UI work.

## Decision readiness versus sequencing

Phase 6.8A decides whether the current evidence satisfies a bounded human-review
threshold. Phase 6.9A does not revisit that judgment. It aggregates current
readiness gaps and orders only the existing analytical tasks that 6.8A already
identified in `next_evidence_task_ids`.

The distinctions remain explicit:

- HIGH opportunity priority does not mean a decision is ready.
- `READY_NOW` means an investigation can begin, not that it is complete.
- `READY_FOR_HUMAN_REVIEW` is not approval or recommendation.
- high evidence leverage is not a guaranteed decision unlock.

## Evidence leverage

Evidence leverage is the breadth of one currently unresolved requirement across
`NEEDS_MORE_EVIDENCE` assessments. A shared stable requirement ID produces one
deduplicated `EvidenceLeverageItem`. Counts state how many current non-ready
decisions and active opportunities reference it. They do not represent
importance, expected value, probability, causal impact, ROI, or financial value.

Requirement name, description, status, and source scope come only from existing
6.7C `EvidenceRequirement` objects. Repeated IDs must have compatible semantic
definitions and current status or validation fails closed. When a missing
requirement already has a 6.7C evidence gap, its existing `gap_id` is reused.
A partial unresolved requirement can legitimately have no evidence-gap ID.

Leverage items use this exact lexicographic order:

1. affected decision count, descending;
2. highest originating opportunity priority, HIGH then MEDIUM then LOW;
3. affected opportunity count, descending;
4. first deterministic appearance in decision and requirement order; and
5. requirement ID.

`top_evidence_focus_requirement_id` is simply the first item in that order. It
does not authorize acquisition, identify the highest ROI, or promise an unlock.

## Investigation sequencing

A task is eligible only when a current `NEEDS_MORE_EVIDENCE` assessment names
its ID in `next_evidence_task_ids`. Requirement overlap alone cannot add a task.
The task’s affected decisions are built only from those authoritative links.
`addressed_requirement_ids` is the intersection of the task’s existing required
requirements and the unresolved requirements on its linked decisions. This is
traceability, not a guarantee that the task will resolve them.

`can_begin_now` is true only for an existing 6.7C `READY_NOW` task. Phase 6.9A
does not recalculate readiness, mark completion, obtain evidence, remove an
unresolved requirement, or change a decision assessment.

Sequence items use this exact lexicographic order:

1. startable tasks before non-startable tasks;
2. opportunity priority, HIGH then MEDIUM then LOW;
3. affected decision count, descending;
4. 6.7A opportunity order;
5. 6.7C task order; and
6. task ID.

`recommended_next_task_id` is the first startable item under that ordering. If
all readiness-raising tasks are blocked, it is `None`; the engine never promotes
a blocked task merely because a related requirement has broad leverage.

## Portfolio states

- `READY_TASK_AVAILABLE`: at least one readiness-raising task is `READY_NOW`.
- `EVIDENCE_GAP_FIRST`: evidence-needing decisions exist, but none of their
  readiness-raising tasks can begin now.
- `BOUNDARY_ONLY`: remaining non-ready decisions are boundary-blocked rather
  than ordinary evidence-acquisition gaps.
- `NO_OPEN_READINESS_GAPS`: all current assessments are ready for human review,
  or no active assessments exist.

`BLOCKED_BY_BOUNDARY` decisions are listed separately and never inflate normal
leverage counts. An `FX_REQUIRED` financial boundary therefore remains a
boundary, not “one more dataset.” Ready decisions also contribute no readiness
gap and no sequencing demand.

## Human agency and safety

The output prioritizes bounded analytical learning. It contains no business
action ranking, opaque score, forecast, causal inference, task owner, due date,
completion state, persistent workflow, automated collection, or execution.
Wording says requirements affect or are relevant to decisions; it never says
collecting evidence will unlock them. Existing validated requirement metadata
is reused without inventing APIs, connectors, tables, files, owners, credentials,
vendors, or acquisition methods.

## CLI

Build the four upstream layers once and render the sequencing portfolio:

```powershell
python -m src.intelligence.sequencing_cli sequence `
  --business-id sama_cod_pilot
```

Stable JSON is available with:

```powershell
python -m src.intelligence.sequencing_cli sequence `
  --business-id sama_cod_pilot `
  --format json
```

The CLI displays portfolio state, top evidence focus, the next startable task
when one exists, leverage breadth, the readiness-raising sequence, boundary
exclusions, and limitations. It does not use OpenAI.

## Limitations and future integration

The ordering is a transparent investigation policy, not expected business
value. It cannot determine whether unavailable evidence exists elsewhere,
whether an investigation will succeed, or whether new evidence will change a
decision’s readiness. Phase 6.9B may expose this validated portfolio through the
product layer; Phase 6.9A intentionally adds no endpoint, Ask mode, or browser UI.
