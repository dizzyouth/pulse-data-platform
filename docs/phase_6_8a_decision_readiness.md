# Phase 6.8A — Decision Readiness Engine

Phase 6.8A adds a deterministic, aggregate-only layer that asks whether the
current validated evidence is sufficient to place a bounded decision question
in front of a human reviewer. It does not choose an option, recommend an action,
or execute a decision.

## Intelligence layers

The layers answer different questions and are deliberately not interchangeable:

1. A signal describes what is happening in validated aggregate evidence.
2. An opportunity identifies what deserves investigation.
3. An investigation plan identifies what can be checked now and which evidence
   requirements remain unresolved.
4. Decision readiness determines whether a specific, bounded question is
   sufficiently evidenced for human review.

Opportunity priority is not decision readiness. In particular, a high-priority,
high-confidence opportunity can support a bounded investigation review while a
higher-bar operational-change question remains `NEEDS_MORE_EVIDENCE`.

## Readiness is not recommendation

The engine emits only these non-numeric statuses:

- `READY_FOR_HUMAN_REVIEW`: current evidence meets the registered bar for
  responsibly presenting the question to a human. It does not approve an option.
- `NEEDS_MORE_EVIDENCE`: the question is relevant, but current validated
  evidence does not meet its registered bar.
- `BLOCKED_BY_BOUNDARY`: an explicit safety, economic, or measurement contract
  prevents evaluation under the current evidence.

Every assessment has `human_review_required=true` and
`autonomous_action_allowed=false`. There is no readiness score, selected action,
predicted outcome, expected return, or automatic decision.

## Decision classes and frame registry

The strict model supports four evidence bars:

- `INVESTIGATION_DIRECTION`
- `BUSINESS_CHANGE_CONSIDERATION`
- `MEASUREMENT_GOVERNANCE`
- `FINANCIAL_DECISION`

`src/intelligence/decisions.py` contains an explicit ordered frame registry.
Each frame declares its originating opportunity type, decision type and class,
question, required 6.7C requirement IDs, relevant 6.7C task keys, counter-evidence
behavior, blocker behavior, boundary behavior, readiness rule, rationale design,
decision boundary, and limitation.

Active confirmation-leakage, acquisition/fulfillment, measurement-reconciliation,
and acquisition-efficiency opportunities each have a bounded review frame and a
separate higher-bar frame where applicable. Suppressed opportunities have no
investigation plan and produce no assessment.

The financial boundary frame is explicit but is not automatically attached to a
current opportunity whose validated 6.7C plan lacks the trusted-economics
requirement. It supports contract testing and future registered plans without
inventing a requirement or changing 6.7C semantics.

## Requirement and evidence traceability

Required IDs must resolve to existing `EvidenceRequirement` objects in the
opportunity's validated investigation plan. Unknown requirement IDs fail closed.
The assessment's unresolved IDs are derived exactly from requirement status; the
decision layer does not reimplement requirement resolution.

Supporting, counter, and blocking evidence remain separate:

- support grounds review of the decision question;
- counter evidence weakens the apparent case;
- blocking evidence constrains interpretation.

All references must resolve to aggregate evidence in the same
`IntelligenceContext`. Opportunity, plan, task, requirement, evidence-gap, and
decision IDs cannot be used as evidence references. Material counter evidence
prevents business-change readiness, while a frame may explicitly allow disclosed
counter evidence for a bounded investigation-direction review.

## Task readiness is not completion

`InvestigationTask.readiness == READY_NOW` means only that the analytical task
can begin with current evidence. Phase 6.7C has no persistent task-completion
state. Phase 6.8A therefore never treats a task's expected output as evidence and
never infers that an investigation ran, produced results, or confirmed a
hypothesis.

`next_evidence_task_ids` contains existing tasks from the same active plan that
could raise readiness. It may point to ready or blocked work and never implies
execution. Plans, plan status, tasks, and recommended-start behavior are not
mutated.

## Boundaries and limitations

Observational evidence may be enough to prioritize investigation or measurement
reconciliation. It is not automatically enough to attribute cause or select a
corrective business action. Higher-bar business-change frames surface
`CAUSAL_UNCERTAINTY` when the current evidence bar is not met.

Financial frames requiring trusted cross-currency economics become
`BLOCKED_BY_BOUNDARY`, with `ECONOMIC_BOUNDARY`, when `FX_REQUIRED` applies. This
is intentionally distinct from missing evidence. The engine does not emit profit,
contribution, margin, MER, ROAS, ROI, recovered-revenue, or dollar-upside claims.

Measurement reconciliation never interprets a platform-versus-observed gap as
lost orders, fraud, tracking failure, operational loss, or financial impact. A
persisted `NORMAL` time anomaly may remain contextual evidence, but it neither
proves business-change readiness nor invalidates a structural opportunity.

## Deterministic portfolio ordering

Assessments are ordered by:

1. originating opportunity priority;
2. originating `opportunity_order`;
3. decision-frame specification order.

Stable IDs use the opportunity type, opportunity scope identity, and frame ID.
`first_reviewable_decision_id` is only the first
`READY_FOR_HUMAN_REVIEW` assessment in that deterministic order. It is not the
best, most profitable, or recommended action.

## Offline CLI

Markdown output:

```text
python -m src.intelligence.decision_cli readiness --business-id sama_cod_pilot
```

Deterministic JSON:

```text
python -m src.intelligence.decision_cli readiness --business-id sama_cod_pilot --format json
```

One active opportunity can be selected with `--opportunity-id`. The CLI builds
the context, opportunities, investigation portfolio, and decision portfolio once
each. It makes no OpenAI request, performs no narration parsing, writes no
warehouse data, and exposes no raw or customer-level rows.

## Limitations and future integration

The engine uses only current validated aggregate evidence. It performs no causal
inference, forecasting, ROI estimation, LLM ranking, task completion persistence,
approval workflow, notification, or external action. Phase 6.8B may expose this
pure engine through product/API surfaces; Phase 6.8A deliberately makes no API or
Ask Pulse browser changes.
