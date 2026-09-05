# Phase 4.4 recovery verification

Verification dates: 2026-09-04 and 2026-09-05 (America/New_York).

The earlier recovery record is retained below; the final resumed validation
appears in "Runtime interruption resolved" and "Validation completed".

## Preserved work

The recovered working tree already contained Metabase v0.63.16.5 and its isolated
PostgreSQL metadata service, a one-shot API initializer/warehouse verifier, six
BI SQL files, a manual dashboard guide, five static BI tests, and changes to
`.env.example`, `.gitignore`, and `README.md`. No existing changes were discarded
and no commit was made.

## Completed during recovery

- Extended `bi/setup_metabase.py` to reconcile the collection, six questions and
  dashboard through the running instance's API, preserving IDs on repeated runs.
- Added optional inclusive date bounds, currency and country variables and
  dashboard mappings; verified the saved questions with actual parameter values.
- Removed mixed-currency revenue and revenue ranks from customer/product BI
  queries. These cards now rank units, with stable ID tie-breakers. Revenue
  overview remains split and ordered by currency. Upstream marts were preserved.
- Restricted the UI publication to `127.0.0.1:3000:3000`.
- Added H2 metadata-file and local environment-file ignore patterns, retaining
  `.env.example`; verified bytecode, runtime, log and generated-state ignores.
- Expanded BI tests to 10 cases, including unavailable API, duplicate-name,
  unsafe target and currency-safety checks. Updated dashboard and README docs.

## Architecture and dashboard

| Responsibility | Service | Database | Named volume |
| --- | --- | --- | --- |
| Airflow metadata | airflow-postgres | airflow | airflow-postgres-data |
| Analytics | warehouse-postgres | pulse_analytics | warehouse-postgres-data |
| Metabase metadata | metabase-postgres | pulse_metabase | metabase-postgres-data |

Metabase reported v0.63.16.5 (f8e0952). Its analytics database ID is 2,
`Pulse Analytics Warehouse`, at `warehouse-postgres:5432/pulse_analytics`.
The metadata connection targets only `metabase-postgres`.

Dashboard: http://localhost:3000/dashboard/2 — **Pulse Marketplace Overview**.
Managed question IDs: revenue overview 40, revenue trend 41, funnel 42,
top customers by units 43, top products by units 44, geography 45.
Repeated provisioning retained these IDs and the same collection contents.
The earlier recovery did not use UI automation. Existing unrelated objects
were preserved; browser validation was added in the resumed session below.

## SQL and API results

All six queries succeeded through Metabase. Its metadata API listed all four
marts. No BI query reads Bronze, Silver or the analytics source tables directly.

| Dataset / query | Rows | Representative result |
| --- | ---: | --- |
| marts.revenue_by_day / revenue trend | 5 | 2026-01-01 EUR: 1868.84 revenue, 2 orders, 7 units, AOV 934.42 |
| Revenue overview | 5 currency groups | AUD: 40.54 revenue, 1 order, 1 unit; EUR remains separate |
| marts.funnel_performance / funnel | 16 | AU: 1 event at each of the five stages |
| marts.top_customers / top customers | 15 | Highest purchase-unit rank: 4 units, 1 payment, 1 distinct order |
| marts.top_products / top products | 15 | Highest purchase-unit rank: 4 units, 1 payment, 1 distinct customer |
| Geography | 7 countries | DE: 4 views, 4 cart adds, 4 checkouts, 3 orders, 2 payments; payment/order rate 2/3 |

Saved-card checks with start/end date 2026-01-01 and EUR returned exactly one
row for each revenue card. Date plus DE returned exactly one row for funnel
and geography. Lifetime unit cards returned 15 rows without unsupported filters.
A before/after object-ID snapshot and all six saved-card checks passed.
The unavailable-server smoke test exited 1 with an explicit five-attempt error.

## Tests and Docker

- `docker compose config --quiet`: exit 0.
- Full suite: `RUN_WAREHOUSE_INTEGRATION_TESTS=1` with unittest discovery:
  **84 passed**, exit 0, 369.571 seconds, including Spark and warehouse integration.
  This run loaded the original five BI tests; the expanded ten-test BI suite was
  run separately after edits and passed, covering five additional cases.
- Focused BI suite: **10 passed**, exit 0.
- `git diff --check`: exit 0.
- Initial full-stack startup succeeded. Kafka, all three PostgreSQL services,
  Airflow webserver/scheduler and Metabase were observed healthy. All three
  one-shot init/setup services exited 0.
- Metabase `/api/health` returned HTTP 200 and `{"status":"ok"}` both from the
  host and inside its container. The API queried the warehouse successfully.

## Runtime interruption resolved on the next resume

Airflow regression run ID: `phase44_recovery_20260904`.
The first five tasks succeeded. The warehouse-load task entered its automatic
second attempt after Docker DNS failed to resolve PostgreSQL. Docker Desktop
then reported `no route to host` for its internal VM; WSL commands also timed
out with `Wsl/Service/0x8007274c`. The scheduler had earlier recovered from
10-second health-probe timeouts under load.

After Docker/WSL recovery, the **same run** finished successfully at
2026-09-05 01:18:31 UTC (2026-09-04 21:18:31 America/New_York). All nine tasks
are successful, including the resumed warehouse load, warehouse validation,
four dbt models and dbt tests. The preserved dbt `run_results.json` records
**36 passed, zero failed**. No DAG was cleared, retriggered or reimplemented.

## Validation completed after the next resume

The next session inspected and retained all existing Phase 4.4 changes. It
changed no application code, SQL, tests, Compose configuration or credentials.

- Full unittest discovery with `RUN_WAREHOUSE_INTEGRATION_TESTS=1`:
  **89 tests passed, zero skipped, exit 0**, in 536.496 seconds. This includes
  all ten BI tests, producer/consumer contracts, Spark Bronze/Silver/Gold
  transformations and Parquet sinks, orchestration/dbt contracts, transactional
  warehouse rollback and repeat-load integration tests. An initial run also
  passed all 89 tests but Windows sandbox permissions blocked Spark process
  cleanup; rerunning with the required process access produced the clean exit.
- Kafka `marketplace.events`: three partitions, leader and ISR present for
  each. A live host consumer validated one retained event per partition using
  the existing deserializer, without committing offsets. Retained offset
  bounds were `[0,32)`, `[0,23)` and `[0,4)` (59 records total). The first
  sandboxed connectivity probe timed out; the authorized repeat passed.
- Before restart, **40 live query comparisons passed** against independent
  PostgreSQL executions of the six SQL files: six saved-question requests and
  34 requests through the actual dashboard/dashcard query endpoint. Compared
  column names, ordered rows and values (with numeric tolerance for upstream
  floating-point measures). Unfiltered row counts remain 5, 5, 16, 15, 15, 7
  for revenue overview, trend, funnel, customers, products and geography.
- Filter cases cover each inclusive date bound separately, combined date plus
  EUR/DE, future/past empty ranges, unknown currency/country and reversed date
  bounds. The lifetime ranking cards remain unfiltered. All four marts are
  visible in the Metabase metadata API. Warehouse targeting and every persisted
  dashboard parameter mapping were checked.
- Repeated provisioning retained database ID 2, dashboard ID 2, question IDs
  40-45 and collection contents. A full definition snapshot also confirmed
  unchanged saved SQL, visualization settings, layout and filter mappings.
  The first `docker compose run --rm metabase-setup` recreated its dependencies
  using existing named volumes. A subsequent
  `docker compose run --rm --no-deps metabase-setup` passed with native exit 0.
- A separate `docker compose restart metabase-postgres metabase` passed, then
  **all 40 query comparisons passed again, exit 0**, without any provisioning
  after restart. The complete object-definition snapshot remained identical:
  IDs, SQL, chart settings, layout, filters and mappings all persisted.
- Hidden Edge browser checks passed for unfiltered, date/EUR/DE-filtered, and
  cleared-filter dashboard states after restart. All six dashboard queries
  completed in each state with no local HTTP failures. DOM data, visible card
  bounds and screenshots confirmed the six cards, five separate currency
  series, EUR revenue 1868.84 (2 orders, 7 units, AOV 934.42), and DE funnel
  results. Lifetime customer/product cards remained at 15 rows. Initial browser
  harness attempts needed actual login cookies and foreground viewport capture;
  the corrected harness passed with exit 0. Test filter values were cleared.
- Kafka and scheduler Docker probes temporarily exceeded their 5/10-second
  timeouts during concurrent Spark testing and Metabase startup. Container
  inspection established timeout failures; Airflow's application health
  endpoint reported a healthy scheduler. All seven services recovered to
  healthy after the workload completed, without changing health thresholds.
- Final checks after the usage-limit pause, 2026-09-05 at approximately
  05:29 UTC (01:29 America/New_York): all seven long-running Compose services
  healthy; all three initializer/setup services exited 0. Metabase returned
  `{"status":"ok"}` and Airflow reported healthy metadata and scheduler
  heartbeats. `docker compose config --quiet` passed. The final working tree
  matches the preserved Phase 4.4 scope; `git diff --check` passed. Completed
  integration tests were reviewed from their logs rather than restarted.

Validation helpers and raw logs are kept under Git-ignored `tmp/`; they are
not a replacement for the existing implementation. The only project document
edited in this resumed session is `bi/VERIFICATION.md` (also normalized from
Windows-1252 to UTF-8). No commit or push was performed.

Local evidence: `tmp/phase44-resumed-tests.log`, `tmp/phase44-bi-before.log`,
`tmp/phase44-bi-after-restart.log`, `tmp/phase44-idempotency.log`,
`tmp/phase44-setup-rerun-final.log`, `tmp/phase44-metabase-snapshot.json`,
`tmp/phase44-browser.log`, and `tmp/phase44-dashboard-{unfiltered,filtered,restored}.{png,txt}`.
The two temporary helpers are `tmp/phase44_validate.py` and
`tmp/phase44_browser.cjs`; the browser's isolated profile is
`tmp/phase44-edge-profile/`.

Files already changed before this resume and preserved:
`.env.example`, `.gitignore`, `README.md`, `docker-compose.yml`,
`bi/setup_metabase.py`, `bi/DASHBOARD.md`, `bi/VERIFICATION.md`,
`bi/queries/revenue_overview.sql`, `bi/queries/revenue_trend.sql`,
`bi/queries/funnel.sql`, `bi/queries/top_customers.sql`,
`bi/queries/top_products.sql`, `bi/queries/geography.sql`,
and `tests/test_bi_config.py`.

## Remaining limitations

Customer/product marts have no currency or date dimension: BI exposes lifetime
unit rankings only. No country-level revenue or FX conversion exists. Funnel
rates are event ratios, not cohort conversion. Revenue inherits floating-point
storage from upstream. Local authentication/database traffic is unencrypted and
the existing warehouse user has broad privileges. Provisioning is verified on
the pinned version and should run serially; renaming managed objects changes
identity. Fresh first-admin creation was not re-exercised against the preserved,
already initialized metadata database.
