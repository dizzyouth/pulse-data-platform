# Phase 6.7A - Cross-Domain Opportunity Engine

Phase 6.7A adds deterministic investigation opportunities over the existing
validated aggregate intelligence context. It does not generate recommendations,
run experiments, forecast outcomes, calculate financial upside, or call an LLM.

## Product concepts

- A **signal** is an observed deterministic condition from Phase 6.6A.
- An **opportunity** is a material combination of validated observations that
  deserves investigation.
- A **hypothesis** is an explicitly untested possible explanation. It is not a
  fact or causal conclusion.
- A **decision unlocked** is the future business decision that better evidence
  could support. It is not the decision itself.

The engine describes what occurs together, what to test, what would confirm or
refute the hypothesis, and which future decision the investigation could
inform. It never authorizes campaign, budget, customer-contact, refund, or
other business actions.

## Architecture

```text
curated deterministic marts and evidence
  -> build_context(...)
  -> IntelligenceContext
  -> explicit cross-domain rule registry
  -> OpportunityCandidate or SuppressedOpportunity
  -> rule-specific blocker relevance
  -> validate_opportunity(...)
  -> stable deterministic ranking
  -> InvestigationOpportunity list
```

The implementation is in:

- `src/intelligence/opportunity_models.py`: immutable typed models.
- `src/intelligence/opportunities.py`: registry, rules, validation, and ranking.
- `src/intelligence/opportunity_cli.py`: offline deterministic presentation.

The engine consumes existing signal, leakage, campaign diagnostic, anomaly,
and economics evidence. It does not parse Phase 6.6B narration and does not
change Phase 6.5 calculations or Phase 6.6A signal semantics. Campaign spend
share is the only new cross-domain calculation; existing rates, peer
benchmarks, benchmark-gap orders, costs, sample bands, anomalies, and
economics status are reused without recalculation.

## Opportunity contract

Each `InvestigationOpportunity` has a stable ID and order, business/date/scope,
type and category, observation, untested hypothesis, priority and confidence,
an optional nonfinancial impact proxy, distinct supporting/counter/blocking
evidence buckets, investigation steps, confirmation and refutation criteria,
future decision unlocked, missing evidence, limitation, and
`causal_claim=false`.

IDs are deterministic normalized values such as:

```text
opportunity:acquisition_fulfillment_misalignment:sama-newum
opportunity:confirmation_leakage:sama_cod_pilot
```

The validator rejects unknown or duplicate evidence references, cross-bucket
duplication, empty support, unstable IDs, invalid bands, untraceable impact
values, PII-shaped content, affirmative causal language, autonomous actions,
and financial claims that violate `FX_REQUIRED`.

## Rule registry

### `OPP_001_ACQ_FULFILLMENT_MISALIGNMENT`

Required domains: campaign acquisition diagnostics plus an existing campaign
delivery-gap or return-pressure signal.

Eligibility:

- campaign spend share is at least 20% of target-campaign spend;
- fulfillment sample band is `MEDIUM` or `HIGH`; and
- an existing `CAMPAIGN_DELIVERY_GAP` or `CAMPAIGN_RETURN_PRESSURE` signal is
  active.

Priority is `HIGH` only when spend share is at least 35%, sample is `HIGH`, and
both downstream signals are active. Other eligible candidates are `MEDIUM`.
A material identity-resolution limitation reduces priority and confidence one
band because linked campaign downstream metrics depend on identity coverage.
This is a coverage/representativeness constraint; it does not invalidate the
observed resolved-cohort metrics. `FX_REQUIRED` blocks financial valuation but
does not reduce a valid nonfinancial investigation.

The hypothesis tests whether acquisition/offer characteristics and downstream
outcomes may be misaligned. It does not identify targeting, creative,
fulfillment, or customer quality as the cause. The future decision is whether
acquisition, offer, or fulfillment evidence deserves investigation first
before spend changes are even considered.

Confirmation requires the peer pattern to remain across adequately sized
cohorts and relevant downstream stages. Refutation includes normalization with
more sample, measurement reconciliation, or a demonstrated data-quality
artifact.

### `OPP_002_CONFIRMATION_LEAKAGE`

Required domains: `MATCHED_NOT_CONFIRMED` leakage plus the existing
confirmation-leakage signal.

Eligibility requires at least 30 matched orders, 10 unmatched-to-confirmation
observations, and a 5% gap. Priority is `HIGH` at 100 affected orders,
`MEDIUM` at 25 affected orders or a 20% rate, otherwise `LOW`. Confidence uses
the existing denominator bands. When that denominator is explicitly the
resolved high-confidence cohort, unresolved records outside it are a contextual
coverage limitation only and do not reduce priority or confidence. Identity
uncertainty within the denominator can reduce both by one band.

The hypothesis tests aggregate process, contact, offer, and upstream
order-quality factors without claiming which factor caused the gap.
Confirmation requires the gap to remain material across additional matched
cohorts or show repeated aggregate confirmation-stage concentration.
Refutation includes normalization or removal through measurement/identity
reconciliation. The future decision is whether confirmation operations, offer
quality, or upstream order quality deserves investigation first.

### `OPP_003_MEASUREMENT_RECONCILIATION`

Required domain: validated `PLATFORM_VS_OBSERVED` measurement leakage.

Eligibility requires a denominator of at least 30, at least 10 observations,
and a 5% gap. Priority is always `LOW`, keeping measurement reconciliation
below major operational investigations. Its wording explicitly avoids claims
of lost orders, fraud, tracking failure, or operational loss.
Confirmation requires repeatable evidence under documented event definitions
and aligned comparison windows; refutation occurs when aligned semantics remove
the difference or show a reporting-timing artifact. The future decision is
whether attribution/event semantics need reconciliation before platform
conversion evidence informs other decisions. Identity and economic limitations
are not scoring blockers for this measurement-only rule.

### `OPP_004_ACQUISITION_EFFICIENCY`

Required domains: campaign diagnostics plus an existing acquisition-cost-gap
signal.

Eligibility requires cost per observed Lightfunnels order at least 1.25 times
the peer value and a `MEDIUM` or `HIGH` acquisition sample band. `LOW` sample
is suppressed. Priority is `MEDIUM` only for `HIGH` sample with at least a 50%
relative cost gap; other eligible candidates are `LOW`.

Materially better delivery or return evidence is counter-evidence. One strong
downstream counter-signal forces `LOW` priority and reduces confidence; both
strong outcomes suppress the candidate as `CONTRADICTORY_EVIDENCE`. The rule
does not label cost/order as CAC and never calculates ROAS or profit.
Confirmation requires the peer-adjusted cost gap to remain with adequate sample
without downstream quality offsetting it. Refutation includes normalization
with more sample/measurement reconciliation or materially stronger downstream
outcomes. The future decision is whether acquisition efficiency deserves more
testing before any campaign or spend decision. Generic identity, currency, FX,
and platform-measurement limitations are not attached or scored by this rule.

## Counter-evidence and suppression

Evidence buckets are disjoint. Counter-evidence is retained when it weakens a
hypothesis; it can lower priority/confidence or suppress a candidate according
to the rule above. Each rule classifies known limitations as scoring,
contextual, or irrelevant. Only scoring blockers reduce priority/confidence;
contextual blockers may remain attached when they materially constrain coverage
or decisionability. `FX_REQUIRED`, missing initial currency, and platform
measurement gaps are not attached to or scored against unrelated non-economic
opportunities. Platform measurement evidence is direct support for measurement
reconciliation rather than a generic blocker.

`blocking_evidence_refs` contain validated evidence that materially constrains
interpretation or decisionability for that rule. `missing_evidence` names
information not currently present that would help confirm or refute the
hypothesis. Generic data-quality context is not copied into every opportunity.

Suppressed candidates contain only rule ID, scope, reason, bounded evidence
references, and a concise explanation. Supported reason codes are:

- `INSUFFICIENT_SAMPLE`
- `INSUFFICIENT_MATERIALITY`
- `CONTRADICTORY_EVIDENCE`
- `MISSING_REQUIRED_DOMAIN`
- `ECONOMIC_BOUNDARY`
- `DATA_QUALITY_BLOCKER`

Suppression is deterministic and visible only when requested in CLI/debug
output.

## Structural and anomaly context

Latest aggregate anomaly state is context rather than an eligibility switch.
A `NORMAL` latest state can be cited with wording that distinguishes it from a
structural peer or stage pattern. It does not deactivate a structural
opportunity or imply persistence beyond the supplied evidence.

## Economic boundary

`FX_REQUIRED` remains authoritative. The engine does not calculate or state
cross-currency profit, contribution, margin, MER, ROAS, recovered revenue,
dollar upside, or opportunity revenue. Impact proxies are bounded to existing
orders, observations, benchmark gaps, excess returns, spend relevance, or
validated USD cost per observed order. Native-currency collections remain
separate from USD costs.

## CLI

List active opportunities as Markdown:

```powershell
python -m src.intelligence.opportunity_cli list --business-id sama_cod_pilot
```

Return stable JSON and include suppressed candidates:

```powershell
python -m src.intelligence.opportunity_cli list `
  --business-id sama_cod_pilot `
  --format json `
  --show-suppressed
```

The CLI performs fixed parameterized warehouse reads through `build_context`
and makes no OpenAI or external network request. Output is aggregate-only.

## Limitations and future path

The registry has four initial rules and explicit bands rather than a hidden
score. It does not perform causal inference, forecasting, propensity scoring,
external benchmarking, automated experimentation, or autonomous action. Its
confirmation/refutation criteria identify evidence that could move an
investigation forward; they are not forecasts.

Phase 6.7A is engine-first. Ask Pulse, the Phase 6.6C API/UI, dashboards,
Airflow, and persistent conversation behavior are unchanged. A later phase may
present validated opportunities through Ask Pulse without moving rule
evaluation into the narration or API layers.
