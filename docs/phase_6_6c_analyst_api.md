# Phase 6.6C - Pulse Analyst API and Ask Pulse

Phase 6.6C adds a local product surface to the validated Phase 6.6B grounded
analyst. It does not add a second intelligence implementation and does not
change Phase 6.5 or Phase 6.6A calculations.

## Architecture

The production path is:

```text
validated deterministic marts, signals, and anomalies
  -> bounded aggregate IntelligenceContext
  -> server-selected narration provider
  -> structured provider answer
  -> full local evidence and safety validation
  -> optional one-call causal-wording repair
  -> validated deterministic fallback when eligible
  -> AnalystAnswer plus measured execution metadata
  -> CLI or FastAPI presentation surface
```

The API calls the existing `build_context(...)` and
`answer_question_with_metadata(...)` functions. The latter wraps the same
`answer_question(...)` path used by the CLI and exposes safe execution facts;
the existing CLI continues to receive an `AnalystAnswer` exactly as before.
Signal rules, campaign peer comparisons, economics rules, anomaly logic,
grounding validation, causal validation, repair, and fallback remain in
`src/intelligence`.

The narration provider cannot execute SQL and receives only the bounded
aggregate context. It has no raw warehouse access, customer records, tools,
vector database, embeddings, or persistent memory. There is no agent loop or
autonomous action engine.

## Local server

Install the complete dependency set and run the default offline provider:

```powershell
pip install -r requirements.txt
$env:PULSE_LLM_PROVIDER = "fake"
python -m src.api.app
```

The equivalent explicit server command is:

```powershell
uvicorn src.api.app:app --host 127.0.0.1 --port 8088
```

Open `http://127.0.0.1:8088/`. Optional settings are:

```text
PULSE_API_HOST=127.0.0.1
PULSE_API_PORT=8088
```

The default host is `127.0.0.1`, not `0.0.0.0`.

**LOCAL DEVELOPMENT ONLY.** This service has no authentication. Do not expose
it publicly without authentication, TLS, authorization, rate limiting, and
deployment hardening. It is not described as production-ready or
Internet-secure.

## Endpoints

### `GET /api/v1/health`

Returns service, warehouse-connectivity, and configured-provider status:

```json
{
  "status": "ok",
  "service": "pulse-analyst",
  "version": "6.6C",
  "warehouse": "reachable",
  "provider": "fake",
  "provider_configured": true
}
```

The warehouse check is a lightweight `SELECT 1`. It never constructs or calls
a narration provider, including when OpenAI is configured. Configuration
status reports only whether required environment values are present; it never
returns a key or any portion of one.

### `GET /api/v1/analyst/capabilities`

Returns deterministic supported capabilities, the five suggested product
questions, and limitations such as aggregate-only evidence, no autonomous
actions, `FX_REQUIRED`, no causal inference, and no persistent memory.

### `POST /api/v1/analyst/ask`

The exact request body contains only:

```json
{
  "business_id": "sama_cod_pilot",
  "question": "Why are returns high?"
}
```

Unknown fields are rejected. A client cannot submit a provider, model, key,
SQL, evidence IDs, raw context, or system prompt.

A successful response has this shape:

```json
{
  "request_id": "locally-generated-uuid",
  "business_id": "sama_cod_pilot",
  "question_category": "returns_reason",
  "answer": {
    "answer_summary": "...",
    "findings": [
      {
        "statement": "...",
        "evidence_refs": ["signal:return_pressure"],
        "confidence": "HIGH",
        "claim_type": "OBSERVATION",
        "causal_claim": false
      }
    ],
    "investigation_steps": ["..."],
    "evidence_refs": ["signal:return_pressure"],
    "limitations": ["Current data cannot establish cause."],
    "confidence": "HIGH",
    "cannot_answer_fully": true,
    "safety_notes": ["..."]
  },
  "meta": {
    "provider": "fake",
    "model": "offline-deterministic-v1",
    "evidence_count": 31,
    "provider_call_count": 1,
    "repair_attempted": false,
    "deterministic_fallback_used": false,
    "fallback_intent": null,
    "latency_ms": 1.25
  }
}
```

Values shown with ellipses are illustrative. `evidence_count`, call count,
repair/fallback status, intent, and latency are measured from the actual
execution. Finding evidence references remain authoritative and the final
top-level list remains the locally derived stable ordered union.

## Provider configuration

Provider selection is server-side only:

```text
PULSE_LLM_PROVIDER=fake       # default when unset
PULSE_LLM_PROVIDER=openai     # explicit opt-in
PULSE_LLM_MODEL=<model name>  # required for OpenAI mode
OPENAI_API_KEY=<local secret> # required for OpenAI mode
```

The fake provider is deterministic, offline, and the ordinary CI/default
path. OpenAI mode uses the existing Phase 6.6B Responses API adapter. Missing
OpenAI configuration returns `PROVIDER_CONFIGURATION_ERROR`; the server does
not silently switch providers. The API key stays in the server environment,
never enters response JSON or browser JavaScript, and is not logged. The live
acceptance guard remains separate and this API phase does not make acceptance
calls.

## Input and privacy boundary

`business_id` is required, at most 64 characters, and restricted to lowercase
letters, digits, underscores, and hyphens. Existing parameterized warehouse
reads receive the validated value. `question` is trimmed, required, and at
most 1,000 characters.

A small deterministic boundary rejects email-shaped input, obvious telephone
numbers, and requests for raw or customer-level identifiers. Rejected text is
not logged. The API accepts aggregate business questions only. It never sends
raw warehouse rows, PII, private filenames, SQL, credentials, or database
configuration to the browser or narration provider.

Safe logs may contain the request ID, business ID, question category,
provider/model, evidence count, latency, provider call count, repair/fallback
flags, and final success/error code. They omit the raw question, prompts,
provider output, full evidence context, credentials, and customer data.

## Error contract

Errors use one stable shape and do not expose tracebacks or internal details:

```json
{
  "error": {
    "code": "ANALYST_VALIDATION_FAILED",
    "message": "The analyst could not produce a safely grounded answer.",
    "request_id": "locally-generated-uuid"
  }
}
```

Supported codes are:

- `INVALID_REQUEST`
- `BUSINESS_NOT_FOUND`
- `WAREHOUSE_UNAVAILABLE`
- `ANALYST_VALIDATION_FAILED`
- `PROVIDER_CONFIGURATION_ERROR`
- `PROVIDER_UNAVAILABLE`
- `AGGREGATE_ONLY_REQUIRED`

## Ask Pulse page

FastAPI serves a same-origin HTML/CSS/vanilla-JavaScript page at `/`. There is
no React, Node, build pipeline, WebSocket, or permissive CORS configuration.
The page renders the summary, confidence/partial-answer state, findings and
their evidence references, investigation steps, final evidence list,
limitations, safety notes, and unobtrusive execution metadata. It includes
loading/disabled controls, Enter-to-submit behavior, five suggested prompts,
clear errors, retry, accessible labels, and responsive wrapping.

The browser posts only `business_id` and `question`. It contains no API key,
database secret, provider configuration, prompt, raw context, or SQL. It keeps
only the currently rendered answer and retry value in page memory. Refreshing
clears them; there is no `localStorage`, session history, `conversation_id`,
embedding, or vector search.

## Product limitations

- Single-turn aggregate business questions only.
- No customer-level analysis or raw-record access.
- No autonomous campaign, budget, refund, or contact action.
- `FX_REQUIRED` prevents trusted cross-currency profit, contribution, margin,
  MER, or ROAS claims.
- Observed return pressure and peer differences do not establish why returns
  occurred.
- No persistent conversation memory.
- No authentication in this localhost-only phase.

Phase 6.6A remains the deterministic evidence source. Phase 6.6B remains the
only narration, grounding, repair, fallback, and validation implementation.
Phase 6.6C is a strict transport and presentation layer over those contracts.
