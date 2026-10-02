# Phase 6.7B - Opportunity API and Ask Pulse integration

Phase 6.7B is the product surface for the deterministic opportunities introduced
in Phase 6.7A. It does not add or reproduce opportunity rules. The 6.7A engine
continues to own eligibility, suppression, ordering, priority, confidence,
blocker relevance, impact proxies, hypothesis wording, criteria, and future
decision language.

The product flow is:

```text
warehouse
  -> build_context(...)
  -> evaluate_opportunities(...)
  -> validated active InvestigationOpportunity objects
  -> Opportunity API / deterministic opportunity answer builder
  -> Ask Pulse
```

## API contracts

`GET /api/v1/analyst/opportunities?business_id=<id>` returns an object with the
business ID, context date, active count, and engine-ordered active opportunities.
Each opportunity exposes only aggregate product fields: identity and order,
scope name/type, type/category, title, observation, untested hypothesis,
priority/confidence, optional impact proxy, the three evidence-reference
buckets, investigation and test criteria, future decision, missing evidence,
and limitation. It never includes suppressed candidates, raw context, SQL, or
private warehouse rows.

`GET /api/v1/analyst/opportunities/{opportunity_id}?business_id=<id>` resolves
the ID only within the active result of `evaluate_opportunities(...)` for the
requested business. Invalid shapes return `INVALID_OPPORTUNITY_ID`; an unknown,
suppressed, or business-mismatched object returns a controlled not-found error.
The service does not interpret the ID as an evidence lookup key.

`POST /api/v1/analyst/ask` retains the strict request body and adds one optional
field:

```json
{
  "business_id": "sama_cod_pilot",
  "question": "What would refute this hypothesis?",
  "opportunity_id": "opportunity:confirmation_leakage:sama_cod_pilot"
}
```

Extra fields remain forbidden. Provider, model, API key, SQL, evidence IDs,
raw context, and system prompt cannot be selected by a browser client.

## Opportunity identity is not evidence identity

An `opportunity_id` selects one stable, validated product object. It is never a
warehouse citation. Answer findings cite only the supporting, counter, or
blocking evidence references already carried by that opportunity. The normal
analyst validator then verifies that every cited reference exists in the same
business-scoped `IntelligenceContext`.

## Deterministic opportunity answers

`src/intelligence/opportunity_answers.py` classifies six bounded intents:

- opportunity overview;
- why the observation warrants investigation;
- evidence that would strengthen the hypothesis;
- evidence that would weaken or refute it;
- investigation steps and missing evidence;
- the future decision better evidence could support.

The builder reads the selected `InvestigationOpportunity`, constructs the
normal `AnalystAnswer` contract, and passes it through the existing
`validate_answer(...)` safety layer. It does not query SQL, call a provider,
recalculate 6.7A logic, establish the hypothesis as fact, or prescribe the
future decision. The metadata is explicit:

```text
provider = deterministic
model = opportunity-engine-v1
answer_source = deterministic_opportunity
provider_call_count = 0
repair_attempted = false
deterministic_fallback_used = false
```

Without `opportunity_id`, the exact 6.6C route remains in place: server-selected
provider, grounding validation, optional one-shot repair, and deterministic
fallback. Its metadata uses `answer_source = grounded_analyst`.

## Ask Pulse UI

The framework-free page fetches the active list directly from the opportunity
API. It preserves engine order and calculates HIGH, MEDIUM, and LOW counts from
the response. Cards show priority, confidence, untested status, title, scope,
observation, and optional impact proxy. Expandable details contain the engine's
hypothesis, investigation steps, confirmation and refutation criteria, future
decision, missing evidence, limitation, and a secondary collapsed evidence list.

Card actions reuse the existing single-turn answer region. They submit only the
business ID, fixed action question, and selected opportunity ID. Ordinary form
and suggestion requests omit `opportunity_id`. The browser stores no history,
secrets, provider selection, model selection, or raw context.

## Suppression, privacy, and safety boundaries

Suppressed candidates are excluded from list, detail, and normal Ask Pulse UI
flows. The Phase 6.7A CLI with `--show-suppressed` remains the developer/debug
surface. All APIs stay aggregate-only and expose neither customer/private fields
nor arbitrary evidence retrieval. Opportunity narration remains non-causal and
investigation-only; `FX_REQUIRED` and the existing autonomous-action prohibition
remain enforced by the shared validator.

## Limitations and later work

This is a local development surface without authentication, persistent chat,
cloud deployment, embeddings, ranking models, or automated business actions.
Opportunities are investigation hypotheses rather than forecasts or financial
upside estimates. A later phase may optionally add opportunity-aware LLM
narration, but the deterministic object, citations, validation, and zero-provider
path remain the authoritative baseline.
