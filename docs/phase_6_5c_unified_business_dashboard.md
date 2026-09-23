# Phase 6.5C — unified real business dashboard

Phase 6.5C is a presentation and decision layer over the validated Phase 6.5A
Lightfunnels/COD facts and Phase 6.5B TikTok marts. It adds no external source,
raw/private parser, warehouse copy, or Airflow DAG. The existing manual
`pulse_sama_real_cod_pilot` DAG builds and tests the three additional dbt views
after the 6.5A and 6.5B loads.

## Scope and terminology

The view covers `sama_cod_pilot`, March 1 through July 31, 2026, using the
existing fixed UTC+1 convention (`Etc/GMT-1`). Its marketing cohort is only the
12 TikTok campaigns linked to Lightfunnels. Platform Conversions are
provider-reported TikTok activity; Lightfunnels Orders, Confirmed Orders,
Delivered Orders, Returned Orders, and Cash Collected are observed business
facts. Platform Conversions are never relabeled as orders, platform ROAS is not
observed business ROAS, and Cash Collected is not profit.

Observed outcomes remain at Campaign grain because Lightfunnels supplies a
campaign identifier but no defensible Ad Group or Ad attribution. Allocating
downstream outcomes below Campaign would invent precision. The unified outcome
marts therefore contain neither `ad_group_id` nor `ad_id`.

## Presentation marts

- `sama_pilot_unified_overview`: one row per business for the target cohort.
  It combines target campaign marketing totals and observed outcomes with
  high-confidence, TikTok-source operational costs.
- `sama_pilot_unified_daily`: one row per business and acquisition date. A full
  outer join preserves target TikTok activity without Lightfunnels orders and
  the one Lightfunnels campaign/day boundary observation without TikTok
  activity.
- `sama_pilot_unified_native_economics`: one row per business, native currency,
  and economic status for high-confidence TikTok-source facts.

The daily date is the Lightfunnels intent/acquisition cohort date. Delivered
and Returned counts are the final observed outcomes of those cohorts, not
same-day delivery events. Delivered and Returned are sibling terminal outcomes
of Shipped; Returned is not a stage after Delivered.

## Costs, collections, and FX

TikTok-source scope is taken from the existing sanitized order-fact UTM source.
The single Phase 6.5A UTM parser now handles Lightfunnels's concatenated format;
the duplicate Phase 6.5B parser was removed. This makes the existing order
facts express the validated 1,069 TikTok Lightfunnels orders while excluding
the four non-TikTok orders.

Operational USD costs use only the existing Phase 6.5A rules and
high-confidence target-cohort matches:

```text
Known Operational Cost USD = Product COGS + Call Center + Logistics
Total Known USD Cost = TikTok Marketing Spend + Known Operational Cost USD
```

Known USD Cost is not total business cost. No overhead, labor, fulfillment,
CAC, or FX assumption is introduced.

Cash Collected and COD Fee remain in SAR, AED, or KWD and are never summed
across currencies or converted to USD. `cash_after_cod_fee_native_currency` is
explicitly native cash after the COD fee, not profit or contribution.
Matched non-delivered facts that have a known USD cost but no authoritative
native commercial currency remain visible under `UNSPECIFIED`; no currency is
inferred for them.
`FX_REQUIRED` remains visible: cross-currency profit, contribution, margin,
MER, and observed business ROAS are unavailable until a trusted FX source is
supplied.

## Executive dashboard

**Pulse — Unified Business Overview** is a new aggregate-only dashboard. It
does not replace **Pulse — Real COD Pilot** or **Pulse — Real TikTok
Performance**. It presents executive KPIs, acquisition-to-outcome counts,
independent rates and acquisition costs, the additive USD cost stack with its
total on a separate card, native-currency collection, separate daily spend and
cohort-outcome charts, a Campaign decision table, the six material measurement
limitations, and the FX completeness statement. No customer/order detail or
PII is exposed.

## Validation

```powershell
python -m unittest tests.test_sama_unified tests.test_sama_pilot tests.test_sama_tiktok tests.test_dbt_project tests.test_bi_config -v
dbt run --project-dir dbt --profiles-dir dbt --select sama_pilot_unified_overview sama_pilot_unified_daily sama_pilot_unified_native_economics
dbt test --project-dir dbt --profiles-dir dbt --select sama_pilot_unified_overview sama_pilot_unified_daily sama_pilot_unified_native_economics
```

The targeted Python tests use synthetic fixtures and static contracts. CI does
not require private source files.
