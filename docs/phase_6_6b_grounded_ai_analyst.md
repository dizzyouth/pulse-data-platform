# Phase 6.6B — Grounded AI Analyst

Phase 6.6B adds single-turn natural-language explanation over Phase 6.6A's
validated deterministic intelligence. The LLM is a narrator, not an analyst of
raw records: warehouse marts, stored signals, and persisted anomaly evaluations
remain the source of truth. The phase does not change any Phase 6.5 or 6.6A
business calculation.

## Architecture and trust boundary

The flow is fixed: curated aggregate relations → bounded evidence context →
provider-neutral narrator → locally validated `AnalystAnswer` → CLI. The Python
application executes fixed parameterized reads. A provider receives structured
JSON and never receives a database connection, arbitrary SQL capability,
credentials, environment configuration, source paths, or raw records.

`src/intelligence/context.py` reads only:

- `marts.sama_pilot_intelligence_signals`
- `marts.sama_pilot_business_leakage`
- `marts.sama_pilot_campaign_diagnostics`
- `marts.sama_pilot_unified_overview`
- `marts.sama_pilot_unified_native_economics`
- `monitoring_views.anomaly_baseline_history`

Every query selects an explicit allowlist of aggregate columns and binds the
business ID as a parameter. Campaign diagnostics already contain target
campaigns only. Economics exposes status and native currency labels—not the
mixed-currency revenue/cost values. Anomalies expose only the latest persisted
state for the five pilot metrics.

Customer rows, raw/hashed phone values, email, address, tracking number,
individual order IDs, private filenames, and raw source records are absent.
`EvidenceItem` also rejects PII-shaped field names, non-curated relations, and
unsupported scopes before prompt serialization.

## Evidence and answer contracts

`EvidenceItem` records a stable `evidence_id`, type, business and scope, title,
facts, limitation, and curated source relation. IDs are derived
deterministically, for example `signal:return_pressure`,
`signal:campaign_delivery_gap:sama-newum`,
`leakage:matched_not_confirmed`, `campaign:sama-newum`,
`anomaly:returned_order_volume`, and `economics:fx_required`.

`IntelligenceContext` contains all seven current deterministic signals, six
leakage rows, target-campaign diagnostics, five latest anomaly states, economic
safety, and the combined evidence list. It rejects duplicate IDs and
cross-business evidence.

`ProviderAnswer` is the model-generated contract. It contains the question,
summary, structured findings, investigation steps, limitations, confidence,
`cannot_answer_fully`, and safety notes. Each finding has required
`evidence_refs`, an enumerated claim type, and explicit `causal_claim`.

The final `AnalystAnswer` adds a top-level `evidence_refs` summary. The provider
does not generate this field. Pulse derives it as the stable first-seen union of
finding references, preserving finding and within-finding order. Repeated refs
across findings are deduplicated; duplicates within one finding remain invalid.
Confidence is only `HIGH`, `MEDIUM`, or `LOW`. Malformed fields, empty
answers/findings, duplicates, and unsupported fields are rejected.

After generation, Pulse fails closed unless:

- every finding reference names supplied evidence;
- final top-level references equal Pulse's deterministic ordered union;
- every business number is present in the supplied evidence;
- causal wording/flags are supported by causal evidence;
- recommendations remain investigative rather than autonomous actions;
- no customer-level identifier or forbidden field is emitted; and
- an `FX_REQUIRED` financial question explicitly remains unanswered without a
  cross-currency profit, contribution, margin, MER, or ROAS result.

These are structured and exact checks, not a claim that broad NLP censorship is
reliable. When safety cannot be established, no narration is returned.

Validation failures carry stable codes including `UNKNOWN_EVIDENCE_REF`,
`UNSUPPORTED_CAUSAL_CLAIM`, `UNSUPPORTED_CAUSAL_WORDING`, `AUTONOMOUS_ACTION`,
`PII_DETECTED`, `UNSUPPORTED_NUMBER`, `UNSUPPORTED_FINANCIAL_CLAIM`, and
`REPAIR_CONTRACT_VIOLATION`. Affirmative causal formulations are rejected when
cited evidence has `causal_claim=false`; explicitly negated limitations such as
“current data cannot establish whether X caused Y” remain valid.

### Bounded live wording repair

The live OpenAI provider may make exactly one repair request only when the sole
validation failure is `UNSUPPORTED_CAUSAL_WORDING`. This is not an agent or tool
loop. An initial answer with unknown evidence, fabricated numbers, PII,
financial claims, autonomous actions, malformed output, structured causal
claims, or mixed violations fails immediately without repair or fallback.

The repair receives only the rejected structured provider answer, its error
code, and the same bounded aggregate evidence context under the full Pulse
safety instructions. It may rewrite summary/finding wording only. Pulse requires
the same per-finding evidence refs and metadata, numbers, limitations,
investigation steps, confidence, and safety notes before running the complete
normal validator again. Fallback eligibility is fixed by the initial answer's
sole `UNSUPPORTED_CAUSAL_WORDING` failure. If the one repair fails validation or
its strict repair contract for any reason, Pulse discards the repaired output in
full, constructs a deterministic answer from the already validated context, and
runs that answer through the same normal validator. The invalid repair never
contributes prose, numbers, evidence references, or recommendations. There is no
third provider call.

The deterministic fallback has six bounded intents: `investigate_first`,
`returns_reason`, `campaign_pause`, `profit`, `anomaly_today`, and `general`.
It reuses stored signal ordering, evidence summaries, recommendations,
limitations, anomaly states, and economic status. It does not parse rejected
prose, calculate metrics, infer causes, or hardcode business values. The same
builder powers `OfflineFakeProvider`, keeping offline behavior deterministic.

## Providers and configuration

`OfflineFakeProvider` is deterministic and makes no network call. It is the CLI
default and covers prioritization, return explanation, pause requests,
economics, and anomaly-state questions. Signal prioritization follows stored
`signal_order` and `priority`; signal names are not hardcoded into ordering.

`OpenAIProvider` is optional. It uses the official SDK's Responses API with a
strict JSON Schema and then applies the same local contract and safety checks.
The request shape follows OpenAI's
[Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs).
There is no default model. Configure secrets only in the local environment:

```dotenv
PULSE_LLM_MODEL=<explicit-model-id>
OPENAI_API_KEY=<secret>
```

Selecting `--provider openai` is an explicit live request. Alternatively,
`PULSE_LLM_PROVIDER=openai` is an explicit environment-level opt-in. Missing model/key or
SDK configuration produces a safe error. Live requests set `store=false`.
Pulse does not log prompts, responses,
keys, headers, or private data. Minimal instrumentation records business ID,
provider, model, question category, evidence count, latency, and validation
success. Repair/fallback metadata is limited to initial and repair validation
codes, attempted/discarded flags, deterministic fallback intent/use, fallback
validation result, and provider call count; prompts and answers are not logged.
Provider calls remain one normally and at most two on the repair/fallback path.

## Commands

The deterministic Phase 6.6A brief is unchanged:

```powershell
python -m src.intelligence.cli brief --business-id sama_cod_pilot
```

Offline grounded questions are the default:

```powershell
python -m src.intelligence.cli ask `
  --business-id sama_cod_pilot `
  --question "What should I investigate first?"

python -m src.intelligence.cli ask `
  --business-id sama_cod_pilot `
  --provider fake --format json `
  --question "How much profit am I making?"
```

Explicit live narration:

```powershell
$env:PULSE_LLM_MODEL = "<explicit-model-id>"
$env:OPENAI_API_KEY = "<secret>"
python -m src.intelligence.cli ask --provider openai `
  --business-id sama_cod_pilot --question "Why are returns high?"
```

The multi-question live smoke mode has a second guard and prints only safe
metadata:

```powershell
$env:RUN_PULSE_LLM_ACCEPTANCE = "1"
python -m src.intelligence.cli live-acceptance --business-id sama_cod_pilot
```

Ordinary tests never make a live request. The live test is skipped unless the
same guard is explicitly set.

## Deliberate limitations

This phase has no persistent chat memory, embeddings, vector database, semantic
search, agent tool loop, raw warehouse access, or write connector. It cannot
establish why returns occurred, turn measurement differences into loss/fraud
claims, combine native COD revenue with USD costs without trusted FX, or make
autonomous budget, campaign, refund, customer-contact, or source-system changes.
