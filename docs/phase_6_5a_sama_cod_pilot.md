# Phase 6.5A — SAMA Lightfunnels + COD Network pilot

This pilot is a business-specific mapping layer over Pulse Operations and
Economics. It does not change the provider-neutral contracts. Lightfunnels is
initial customer intent; COD Leads is confirmation truth; COD Orders is final
commercial, shipment, delivery/return, and collection truth.

## Source lineage and authority

| Stage | Authoritative source and fields | Treatment |
|---|---|---|
| Intent | Lightfunnels Order ID, Created at, Order Total, distinct Item IDs, SKU, country/UTM | Rows are deduplicated at Order ID + Item ID. Distinct physical Item IDs determine initial quantity. Order Total is offer revenue; Item Price and compare-at values are not. |
| Confirmation | COD Lead ID, Status, Created At/Call At | Raw provider status is retained. `Confirmed` maps to confirmed; `Wrong` to invalid/rejected; `Expired` to unreachable; `Cancelled price` to customer rejection; `Cancelled` to cancellation; `Black listed` to rejected/invalid without a fraud claim. |
| Final order | COD Reference, Lead ID, Quantity, Total, Currency | Final quantity/value/currency supersede intent for final commercial truth without overwriting the initial values. `Total USD` is only observed as ignored source metadata. |
| Fulfillment | Shipped At | A shipment exists only when Shipped At exists. Displayed/default Shipping Fees cannot create a shipment or charge. |
| Delivery/return | Status, Delivered At, Returned At | Lifecycle events after July 31 remain attached to the March–July intent cohort. |
| Collection | Delivered status plus Total/Currency | Delivered means full native-currency Total was collected. No partial collection and no remittance are fabricated. |

Both sources are interpreted in the fixed IANA UTC+1 zone `Etc/GMT-1`. The
observed roughly 60-minute intent-to-lead delay is preserved as process time;
timestamps are never shifted to force a match.

## Identity and privacy

Phone is normalized only in memory. An HMAC-SHA256 linkage key is produced with
`PULSE_PILOT_IDENTITY_HMAC_KEY`; the secret is never stored. Candidate matching
requires the HMAC phone and scores initial quantity, initial total, and timestamp
proximity. A unique score of at least 0.80 is `MATCHED_HIGH_CONFIDENCE`; close
top candidates are `AMBIGUOUS`; everything else is `UNMATCHED`. The approximate
one-hour delay earns temporal evidence but is not a correction. Unmatched and
ambiguous records never enter the matched pilot cohort. Resolution is also
one-to-one: if more than one intent selects the same COD lead, every colliding
intent is marked ambiguous rather than duplicating the lead's order/economics.

Real source phone, name, email, address, tracking number, and provider
input/comment fields do not leave the adapter. They are absent from warehouse
tables, dbt, Metabase, logs, and validation output. Order/lead/reference IDs and
the HMAC digest are pseudonymous lineage, not analytical customer dimensions.
The test fixture contains only reserved `+999` synthetic phone values needed to
exercise linkage; it contains no source PII, names, addresses, or tracking data.

## Costs and currency

Rules are versioned as `sama_pilot_business_rules_v1` and deterministic cost
IDs make reruns idempotent. They are marked canonical `ESTIMATED` with
`rule_basis=BUSINESS_RULE`, never represented as provider invoice lines.

- All three native SKUs map to one product family; unit cost is USD 2.80.
- Delivered permanent COGS is final quantity × USD 2.80. Returned inventory is
  recoverable, so permanent COGS is zero. Pending, stockout, and cancellation
  also recognize zero permanent COGS.
- Wrong leads cost USD 0. Other failed leads cost USD 0.50. Confirmed but not
  delivered costs USD 2 total; confirmed and delivered costs USD 3 total.
- Actually shipped costs USD 2.99. Delivered total logistics cost is USD 4.99.
  A never-shipped stockout/cancellation costs zero even when a source display
  field contains a default fee.
- Delivered COD fee is 5% of authoritative Total in the order currency.

Native collected revenue and native COD fees remain grouped by SAR/AED/KWD.
Product, call-center, and logistics costs remain USD. `FX_REQUIRED` is surfaced;
no USD contribution, profit, or margin is calculated until a trusted FX layer
exists.

## Outputs and execution

The adapter emits sanitized pilot facts plus canonical commerce orders, lines,
confirmation/fulfillment/delivery/collection events, shipments, effective-dated
product costs, and rule-derived variable cost events. The warehouse persists
only `analytics.sama_pilot_order_facts`,
`analytics.sama_pilot_identity_resolution`, and
`analytics.sama_pilot_data_quality`. dbt exposes aggregate funnel, order-change,
native economics, and DQ marts. Metabase creates the aggregate-only **Pulse —
Real COD Pilot** dashboard when those marts exist.

Local commands (the key belongs in ignored `.env`):

```powershell
$env:PULSE_PILOT_IDENTITY_HMAC_KEY = '<local secret>'
python -m src.pilots.sama validate
python -m src.pilots.sama summary
python -m src.warehouse.load_sama_pilot load
dbt run --project-dir dbt --profiles-dir dbt --select sama_pilot_funnel sama_pilot_order_changes sama_pilot_native_economics sama_pilot_data_quality
dbt test --project-dir dbt --profiles-dir dbt --select sama_pilot_funnel sama_pilot_order_changes sama_pilot_native_economics sama_pilot_data_quality
```

The Airflow DAG `pulse_sama_real_cod_pilot` is manual-only. Full local source
acceptance is opt-in with `RUN_SAMA_PILOT_FULL_ACCEPTANCE=1`; CI uses only the
tiny synthetic JSON fixture.

## TikTok follow-on

The earlier whole-period TikTok exports remain unsuitable for canonical daily
marketing grain and are not ingested. Phase 6.5B uses only the replacement
`Tiktok Ads_Daily ad level.xlsx`, which supplies campaign, ad-group, ad, and day
lineage. See [`phase_6_5b_sama_tiktok.md`](phase_6_5b_sama_tiktok.md).
