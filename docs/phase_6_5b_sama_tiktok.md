# Phase 6.5B — SAMA TikTok marketing and campaign-to-COD outcomes

Phase 6.5B adds the first real TikTok marketing source for the SAMA COD pilot.
It is deliberately business-specific: the shared marketing, Operations,
Economics, identity, and onboarding contracts are unchanged. All three source
systems use the existing SAMA `Etc/GMT-1` IANA timezone convention (UTC+1), so
native calendar dates are preserved without artificial shifting.

## Private source and lineage

The only accepted marketing input is:

`data/private/sama_pilot/Tiktok Ads_Daily ad level.xlsx`

The entire `data/private/sama_pilot/` tree is ignored. The workbook is read
directly from XLSX XML so campaign, ad-group, ad, and account identifiers retain
their exact source lexemes and cell storage types. No spreadsheet library or
floating-point conversion is allowed to rewrite identifiers.

The adapter expects `tiktok_ads_daily_v1` fields at campaign + ad group + ad +
day grain. It retains all valid rows, including fully inactive rows and rows
with zero spend/impressions but attributed conversions or checkouts. The export
summary row is excluded from canonical records and reconciled against the data
rows.

Source lineage is:

1. `src.pilots.sama_tiktok` parses and validates the final XLSX.
2. Each sanitized ad-day row is validated with the existing
   `MarketingRecord` contract.
3. `src.warehouse.load_sama_tiktok` transactionally replaces three isolated
   SAMA TikTok source tables.
4. dbt publishes native performance, campaign outcomes, and data-quality marts.
5. Metabase provisions the separate **Pulse — Real TikTok Performance**
   dashboard.

## Metric semantics

TikTok spend, impressions, destination clicks, platform conversions,
checkouts, and additive video-view measures are preserved as native metrics.
Provider-reported ratios remain diagnostic details. Dashboard CTR, hook rate,
hold rate, and cost metrics are recalculated from additive components where
possible.

`platform_conversion_value` is explicitly zero with an availability flag in
details because the export does not provide a trustworthy canonical conversion
value. Provider-reported Purchase ROAS is retained only as a native diagnostic;
it is not treated as business ROAS.

Lightfunnels UTM attribution is parsed without customer fields. Existing Phase
6.5A order facts are aggregated to campaign/day in memory before persistence.
Observed COD outcomes are never assigned below campaign grain, and no
customer-level rows, names, phones, email addresses, physical addresses, or
tracking numbers enter the Phase 6.5B tables.

Spend is USD while COD collections remain SAR/AED/KWD. The implementation does
not mix those currencies and does not calculate cross-currency ROAS,
contribution, or profit. The dashboard states `FX_REQUIRED` and requires trusted
FX before such economics can be produced.

## Warehouse and marts

Sanitized source tables:

- `marts.sama_pilot_tiktok_ad_daily`
- `marts.sama_pilot_tiktok_order_outcomes_daily`
- `marts.sama_pilot_tiktok_data_quality`

dbt marts:

- `marts.sama_pilot_tiktok_native_performance`
- `marts.sama_pilot_tiktok_campaign_outcomes`
- `marts.sama_pilot_tiktok_data_quality`

The native mart preserves ad-day hierarchy. The outcome mart is campaign/day
only and intentionally contains no ad-group ID, ad ID, revenue, contribution,
profit, or business-ROAS field.

## Data-quality controls

The pilot records defects for duplicate ad-day grain, missing or malformed IDs,
numeric/scientific-notation ID storage, invalid dates/currencies/metrics,
clicks above impressions, hierarchy conflicts, missing target campaigns,
source-summary disagreement, and conflicting Lightfunnels campaign mappings.
It separately observes outside-target campaigns, inactive rows, zero-spend
attributed activity, campaign-day boundary cases, and the expected difference
between platform conversions and observed Lightfunnels orders.

## Orchestration and validation

The existing `pulse_sama_real_cod_pilot` DAG remains manual-only and now runs
Phase 6.5A validation/load before Phase 6.5B validation/load, followed by the
selected dbt models and tests.

Local commands:

```powershell
python -m src.pilots.sama_tiktok validate
python -m src.pilots.sama_tiktok summary
python -m src.warehouse.load_sama_tiktok load
python -m src.warehouse.load_sama_tiktok validate
dbt run --project-dir dbt --profiles-dir dbt --select sama_pilot_tiktok_native_performance sama_pilot_tiktok_campaign_outcomes sama_pilot_tiktok_data_quality
dbt test --project-dir dbt --profiles-dir dbt --select sama_pilot_tiktok_native_performance sama_pilot_tiktok_campaign_outcomes sama_pilot_tiktok_data_quality
python -m unittest tests.test_sama_tiktok tests.test_dbt_project tests.test_bi_config -v
```

Real-source acceptance is opt-in:

```powershell
$env:RUN_SAMA_TIKTOK_FULL_ACCEPTANCE = "1"
python -m unittest tests.test_sama_tiktok.SamaTikTokRealDataAcceptanceTests -v
```

Offline CI uses only
`data/fixtures/sama_pilot/sama_tiktok_synthetic.json`; it never needs or copies
the private workbook.
