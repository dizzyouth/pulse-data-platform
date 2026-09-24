# Phase 6.6A — Explainable Decision Intelligence

Phase 6.6A adds a deterministic diagnostic layer over the validated Phase
6.5A/6.5B/6.5C pilot marts. It uses no LLM, external AI API, new source, or
opaque score. LLM narration is deferred until the evidence contracts, causal
safety, evaluation criteria, and operating controls can be validated. The local
brief and dashboard render stored facts and rules without network calls.

## Architecture and signal contract

`marts.sama_pilot_business_leakage` separates six observed differences:
platform versus observed measurement, unresolved identity, matched but not
confirmed, confirmed but not shipped, returned after shipment, and shipped
without a delivered/returned terminal outcome. Measurement and identity rows
are not mislabeled as operational loss, and no row is called revenue loss.

`marts.sama_pilot_campaign_diagnostics` contains one row per target campaign.
Actual spend and outcomes come from the existing campaign-outcome mart. Every
peer baseline is computed from the sums for all *other* target campaigns in the
same business. Rates are ratios of peer sums, not averages of campaign rates.
The current campaign is subtracted from both peer numerator and denominator.

`marts.sama_pilot_intelligence_signals` implements the provider-neutral signal
contract. Each row has stable signal/business/date/scope identity; signal type,
category, priority, and confidence; observed/baseline/gap/sample evidence;
plain-language evidence, investigation guidance, and limitation; and
`causal_claim=false`. The Python `DecisionSignal` dataclass validates the same
read contract without reimplementing SQL rules.

Observed facts answer what the current marts contain. Peer benchmarks answer
how one campaign differs from the other target campaigns. Neither is a causal
claim. `confirmation_benchmark_gap_orders`, `delivery_benchmark_gap_orders`,
and `excess_returns_vs_peer` are benchmark-gap estimates only. They are not
forecasts, incremental lift, guaranteed recoverable orders, or proof that a
campaign caused an outcome.

## Evidence bands, materiality, and priority

The evidence bands are explicit heuristics, not statistical significance:

- `LOW`: denominator below 30
- `MEDIUM`: denominator from 30 through 99
- `HIGH`: denominator at least 100

Confirmation uses matched orders; delivery and return use shipped orders;
acquisition cost per observed order uses Lightfunnels orders; cost per delivered
uses delivered orders. Diagnostic rows remain visible at every sample size.
Campaign delivery and return signals require at least MEDIUM shipped evidence
and an absolute rate difference of 10 percentage points. The acquisition-cost
signal requires at least MEDIUM order evidence and cost per Lightfunnels order
at least 25% above the leave-one-out peer baseline.

Priority is deterministic. Measurement signals are LOW because the difference
requires semantic review. LOW-confidence evidence is always LOW priority. With
adequate evidence, an affected-order benchmark of at least 100 is HIGH; at
least 25 affected orders or a relative gap of at least 20% is MEDIUM; remaining
signals are LOW. Ordering is priority, affected-order estimate descending,
absolute gap descending, then stable signal and scope identity. There is no
0–100 score.

Recommendations mean investigate or review. They do not authorize pausing a
campaign, cutting budget, scaling spend, or any autonomous change.

## What Pulse can and cannot explain

Pulse can locate observed measurement, identity, confirmation, fulfillment,
return, and peer-performance differences. It can report the acquisition cohort,
campaign association, observed outcome counts, USD marketing costs, and known
USD operating costs already validated in Phase 6.5.

The current sanitized contract does not contain a proven causal return reason.
It cannot establish whether creative, fulfillment, customer quality, or another
factor caused a return. It does not invent those dimensions. Signal limitations
and the dashboard's “What Pulse Cannot Explain Yet” table state this directly.

Native COD collections remain SAR/AED/KWD while marketing and known operating
costs are USD. `FX_REQUIRED` remains in force. Cross-currency profit,
contribution, margin, MER, and business ROAS are unavailable without trusted FX.

## Existing anomaly engine reuse

`src.quality.anomaly_sources` now reads the existing
`marts.sama_pilot_unified_daily` view and emits daily target spend plus
Lightfunnels, confirmed, delivered, and returned order-volume series with
business and reporting-timezone dimensions. These series use the existing
contextual baseline strategies and persistence. No anomaly math is embedded in
the intelligence marts and no rate anomaly is forced over small daily samples.

Pilot volume policies use an explicit five-order minimum deviation; daily spend
uses USD 25. These narrowly scoped floors avoid manufacturing noise without
weakening generic policies. The existing manual pilot DAG runs one anomaly task
after dbt tests. Anomaly business results remain nonblocking because the task
does not opt into `--block-on-critical`.

## Local interfaces

Build and test the focused models:

```powershell
dbt run --project-dir dbt --profiles-dir dbt --select `
  sama_pilot_business_leakage sama_pilot_campaign_diagnostics `
  sama_pilot_intelligence_signals
dbt test --project-dir dbt --profiles-dir dbt --select `
  sama_pilot_business_leakage sama_pilot_campaign_diagnostics `
  sama_pilot_intelligence_signals
```

Read the top five current signals without a network call:

```powershell
python -m src.intelligence.cli brief --business-id sama_cod_pilot
python -m src.intelligence.cli brief --business-id sama_cod_pilot --format json
```

The separate **Pulse — Intelligence** dashboard is provisioned after the three
existing pilot dashboards and leaves them unchanged. It shows attention counts,
priority signals, a category-safe leakage map, target-campaign peer diagnostics,
persisted pilot anomaly evaluations (or “No current anomaly detected”), evidence
limitations, and the `FX_REQUIRED` safety statement.

All outputs are aggregate and PII-free. No raw phone, email, address, tracking
number, customer identity, ad-group outcome, or ad outcome is introduced.
