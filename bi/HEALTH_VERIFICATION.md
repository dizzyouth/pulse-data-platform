# Phase 5.4 verification

Validated locally on September 6–7, 2026, against the existing Docker Compose
stack, Windows virtual environment, and pinned Metabase v0.63.16.5.

## Implementation

Six ordinary, non-updatable PostgreSQL views live in `monitoring_views`:
`quality_history`, `latest_quality_status`, `check_history`,
`check_failure_summary`, `recent_critical_failures`, and `current_health`.
Their grains and status/coverage rules are documented in the README Phase 5.4
section. Initialization is repeatable and transactional. No tables, indexes,
external services, dependencies, dbt models, or quality-engine changes were added.

Direct views keep presentation independent of the downstream dbt gate. Metabase
questions select from these views and supply optional date/dimension predicates.
The new Pulse Monitoring collection contains eight managed questions and the
Pulse Platform Health dashboard. Marketplace remains a separate dashboard.

Created files:

- `src/warehouse/monitoring.py`: explicit initialization command.
- `src/warehouse/monitoring_views.sql`: view definitions and required dataset inventory.
- `bi/monitoring_dashboard.py`: question and dashboard reconciliation.
- `bi/monitoring_queries/current_health.sql`
- `bi/monitoring_queries/latest_quality_status.sql`
- `bi/monitoring_queries/quality_trend.sql`
- `bi/monitoring_queries/status_distribution.sql`
- `bi/monitoring_queries/recent_warnings.sql`
- `bi/monitoring_queries/recent_critical_failures.sql`
- `bi/monitoring_queries/failing_checks.sql`
- `bi/monitoring_queries/runs_by_layer.sql`
- `monitoring/presentation_queries.sql`: manual, configurable SQL examples.
- `tests/test_monitoring_presentation.py`: twelve new tests.
- `bi/HEALTH_VERIFICATION.md`: this report.

Modified files: `bi/setup_metabase.py`, `bi/DASHBOARD.md`, and `README.md`.
Docker Compose, CI configuration, Airflow tasks, quality calculations/persistence,
and existing dbt business marts are unchanged.

## Test results

| Validation | Result |
| --- | --- |
| Focused monitoring, BI, CI contracts | 26 passed in 23.208 seconds |
| Full Windows suite with Spark, warehouse and monitoring integration enabled | 169 run in 842.183 seconds: 166 passed, 3 Airflow-only skips |
| Those three Airflow tests in the existing Linux image | 3 passed in 46.802 seconds; seven boundary scenarios |
| Python compileall and pip check | Passed; no broken requirements |
| Docker Compose configuration | Passed |
| dbt parse and compile | Passed; 4 models, 36 data tests, 4 sources |
| Live Airflow DAG import | No import errors (`[]`) |
| CI workflow | Four contract tests and actionlint passed; optional shellcheck/pyflakes integrations disabled |
| Git whitespace check | Passed; no staged files |

The six PostgreSQL presentation tests use an isolated disposable database. They
cover latest-by-completion selection, deterministic ties, dataset/layer isolation,
PASS versus severity, warning/failure counts, UTC windows, missing coverage,
empty history, repeated initialization, rejected view writes, and execution of
all dashboard queries with and without filters. CI-safe contracts cover policy
inventory, both-dashboard wiring, idempotency, preservation of unrelated cards,
valid filter mappings, read-only queries, and sanitized initialization failures.

Airflow validation used isolated SQLite metadata and a disposable PostgreSQL
database. It verified persistence-before-summary-before-blocking at Silver,
Gold, and Warehouse boundaries, including downstream `upstream_failed` states.
Its fixtures did not add incidents to the live monitoring history.

## Live PostgreSQL and Metabase

The existing warehouse contains 10 quality runs and 182 check results. All runs
are PASS. Results include 173 PASS/CRITICAL, 5 PASS/WARNING, and 4 PASS/INFO checks.
The dashboard correctly shows zero warning outcomes and zero failed CRITICAL
checks. Latest selection matches the raw-table query and returns nine dataset/
layer assessments. Current health shows Silver 1/1, Gold 4/4, and analytics 4/4,
all PASS. PostgreSQL reports all six views as non-updatable and non-insertable.
Manual presentation examples also execute successfully.

EXPLAIN for latest status showed a small sequential scan and sort. Existing
Phase 5.3 indexes were reviewed; no new index was justified by this local history.

Metabase verification completed 64 health-dashboard query/filter comparisons
against PostgreSQL and 40 Marketplace comparisons. These include missing-match
filters, FAIL status against passing history, UTC date bounds, and dataset/layer
selection. Only compatible parameters are mapped. Current health ignores Dataset,
Run status and dates; latest dataset status ignores Run status and dates.

Hidden Edge browser checks rendered both dashboards unfiltered, filtered, and
with filters cleared, without dashboard HTTP errors. Selecting Gold/daily_sales/
PASS on September 6 reduced historical runs from 10 to 1, while the layer coverage
card retained the complete Gold 4/4 summary. Empty incident cards rendered as
"No results". No artificial incident data was inserted for appearance.

Provisioning was run twice. Both dashboards retained their object IDs, saved SQL,
visualizations, layouts, questions and parameter mappings. Metabase was then
restarted alone and returned healthy. A metadata comparison, without provisioning,
confirmed the same definitions and IDs survived the restart.

Local dashboard URLs (IDs are discovered during setup, not hardcoded in code):

- [Pulse Platform Health](http://localhost:3000/dashboard/3)
- [Pulse Marketplace Overview](http://localhost:3000/dashboard/2)

Ignored local evidence includes `tmp/phase54-focused.log`,
`tmp/phase54-full-tests.log`, `tmp/phase54-airflow-tests.log`,
`tmp/phase54-live.log`, `tmp/phase54-marketplace.log`,
`tmp/phase54-idempotency.log`, `tmp/phase54-restart-persistence.log`,
`tmp/phase54-browser.log`, `tmp/phase54-marketplace-browser.log`, and
`tmp/phase54-dashboard-{unfiltered,filtered,restored}.png`.

## Limits and later phases

This is recorded quality health, not live uptime or a freshness SLA. All execution
sources and distinct attempts are included. Empty or incomplete required coverage
is UNKNOWN; old complete history does not automatically become unhealthy. The
current live history has one day and no actual incidents; negative cases were
verified with isolated fixtures. Large-history query optimization, a dedicated BI
database role, and operational hardening remain future work.

Phase 5.5 or later may add freshness policies, dashboard refinements, alerting,
retention, and access hardening. No Grafana, Prometheus, notifications, external
observability platform, ML anomaly detection, baselines, OpenLineage, automatic
retention, or production deployment was introduced. No files were staged,
committed, or pushed during this work.
