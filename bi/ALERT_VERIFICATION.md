# Phase 5.6 acceptance record

Local validation date: 2026-09-07. Changes are intentionally unstaged and
uncommitted. No push or GitHub Actions run was triggered.

## Architecture and behavior

- `alert_service.py` owns incident creation, recurrence, strict acknowledgement
  and resolution transitions, active queries, and atomic transition audits.
  Calculation modules are unchanged.
- OPEN and ACKNOWLEDGED are active. RESOLVED is terminal. Acknowledgement is
  ownership, not recovery. Repeated operator transitions reject explicitly.
- Incident identity is versioned canonical JSON SHA-256 of source, dataset,
  layer, metric, quality check (null for anomalies), and dimensions. It excludes
  execution IDs, attempts, severity, thresholds, and observations.
- One active instance per key is enforced by a partial unique PostgreSQL index.
  Advisory transaction locks serialize competing incident operations.
- Distinct logical executions increment the active instance's count. Receipts
  exclude retry attempts and survive resolution. New executions after resolution
  create a new instance; old execution replays do not.
- First/last seen use observation time. Severity retains the highest urgency in
  the instance. Newer observation evidence updates source/execution context.
- Both anomaly and quality persistence call the same service inside their source
  transaction. Quality writes precede alerts, followed by commit, summary, task
  failure, and downstream blocking. Anomalies remain nonblocking by default.
- Resolution is manual for both sources. NORMAL, missing series,
  INSUFFICIENT_HISTORY, and passing quality retries do not alter lifecycle.

## PostgreSQL and audit

`monitoring.alert_events` gains the generated `lifecycle_status` alias of `status`,
incident key, first/last seen, occurrence count, acknowledgement/resolution UTC
timestamps, actors, and resolution note. Original columns remain available.

`monitoring.alert_event_history` records opening and valid transitions, using a
UUID foreign key, previous/new state, database UTC timestamp, actor, and note.
State and audit commit atomically. `monitoring.alert_occurrences` records distinct
logical detections with JSONB evidence and provides persistent retry deduplication.

The migration is repeatable and atomic. Legacy IDs and source evidence survive.
Duplicate old OPEN rows consolidate into the oldest active representative; other
instances retain an explicit consolidation resolution note and audit. This is
migration bookkeeping, not automatic recovery. Orphan legacy anomaly sources get
isolated identities if their metric cannot be recovered.

New read-only views are `active_alerts`, `alert_history`,
`alert_summary_by_status`, and `recurring_alerts`; the existing severity summary
remains compatible. Instance detail views expose times, counts, context, and
duration. Audits remain queryable in their underlying history table.

## CLI and dashboard

`python -m src.quality.alert_cli list` returns active alerts as JSON (`[]` when
empty). It supports status, layer, dataset, severity, and limit filters.
`acknowledge <uuid> --by <name>` and `resolve <uuid> --by <name> --note <text>`
operate without Airflow, using the existing warehouse environment.

Pulse Platform Health retains all 12 original quality/anomaly cards. Four
additional compact cards show active alerts, lifecycle totals, recurring alerts,
and recently resolved alerts. Operational fields precede long identifiers.
Lifecycle status maps only to alert cards, independently of quality Run status.
The active queue ignores date filters; new historical cards use UTC last-seen or
resolution dates as appropriate. Other compatible filters remain available.

## Validation results

| Check | Result |
| --- | --- |
| Focused lifecycle, anomaly, quality persistence, PostgreSQL, presentation | 62/62 passed, repeated after Docker recovery in 62.826 seconds |
| Complete discovery with Spark, warehouse, and monitoring integration enabled | 199 tests in 1070.319 seconds: 193 passed, 3 expected Airflow-only skips, 2 warehouse connection failures during Docker outage |
| Affected warehouse tests after recovery | Both passed in 84.660 seconds, covering rollback and rerunnable refresh |
| Actual Airflow execution | 3 tests / 7 boundary scenarios passed in 86.791 seconds using isolated SQLite metadata and a disposable PostgreSQL warehouse |
| Final service/CLI, dashboard, and CI contracts | 14/14 passed |
| dbt | Fresh parse and compile with `--no-introspect --no-populate-cache` passed: 4 models, 4 sources, 36 data-test definitions |
| Compose and DAG import | Compose configuration valid; live import errors `[]`, checked again after recovery |
| Python and whitespace | `compileall` and `git diff --check` passed |
| Metabase API after recovery | All 16 saved queries, exact SQL, preserved original IDs, parameter mappings, and six lifecycle-filtered query paths passed |
| Metabase browser after recovery | All 16 cards rendered, ACKNOWLEDGED filtering passed, filters cleared successfully, zero HTTP errors |
| Post-cleanup empty state | CLI returned `[]`; all four lifecycle cards returned zero rows through uncached Metabase queries |
| Service health after recovery | All seven services healthy |

The full discovery result is reported without hiding its two infrastructure
failures. Both affected tests passed on rerun, so all 196 enabled discovery tests
have passing results; the three skipped real Airflow tests passed separately.
CI configuration was validated locally; no remote CI run was triggered.

Docker Desktop became unresponsive during the initial combined regression/browser
workload. It required a Docker-only WSL backend restart and recovery of Desktop's
stuck stopping state. No volume was deleted. Lifecycle state and occurrence counts
survived recovery. The initial Metabase API timeout and incomplete final browser
reload are retained in local logs rather than represented as successful checks.

Live fixtures exercise OPEN creation, distinct recurrence, last-seen updates,
acknowledgement, resolution, replay after resolution, new-instance history, and
audits via the actual persistence service and CLI. All synthetic rows were removed
transactionally afterward. Before/after monitoring counts match exactly:

| Table | Before | After cleanup |
| --- | ---: | ---: |
| quality_runs | 10 | 10 |
| quality_results | 182 | 182 |
| anomaly_results | 56 | 56 |
| alert_events | 0 | 0 |
| alert_occurrences | 0 | 0 |
| alert_event_history | 0 | 0 |

Evidence is retained locally under ignored `tmp/`: `phase56-focused-final.log`,
`phase56-full-tests.log`, `phase56-warehouse-recovery.log`,
`phase56-airflow-tests.log`, `phase56-dbt.log`, `phase56-metabase-api.log`,
`phase56-browser.log`, and `phase56-live-completed.json`. Browser screenshots are
`phase56-dashboard-unfiltered.png`, `phase56-dashboard-filtered.png`, and
`phase56-dashboard-restored.png`; they intentionally capture the temporary test
data before cleanup. The local dashboard is available at
<http://localhost:3000/dashboard/3>.

## Files created and modified

Created:

- `src/quality/alert_service.py`
- `src/quality/alert_migration.py`
- `src/quality/alert_cli.py`
- `tests/test_alert_lifecycle.py`
- `bi/monitoring_queries/active_alerts.sql`
- `bi/monitoring_queries/alerts_by_status.sql`
- `bi/monitoring_queries/recurring_alerts.sql`
- `bi/monitoring_queries/resolved_alerts.sql`
- `bi/ALERT_VERIFICATION.md`

Modified:

- `src/quality/monitoring.sql`
- `src/quality/persistence.py`
- `src/quality/anomaly_persistence.py`
- `src/warehouse/monitoring_views.sql`
- `bi/monitoring_dashboard.py`
- `bi/monitoring_queries/recent_alert_events.sql`
- `bi/monitoring_queries/alerts_by_severity.sql`
- `tests/test_anomaly_postgres.py`
- `tests/test_monitoring_postgres.py`
- `tests/test_monitoring_presentation.py`
- `tests/test_quality_persistence.py`
- `README.md`

Local validation scripts, screenshots, and logs are under ignored `tmp/`.
Docker Compose, the DAG, anomaly calculations, quality rules, dbt models,
dependencies, and CI configuration required no changes.

## Limitations and deferred work

Operator names are free text. Database access is trusted local development;
direct SQL can bypass application transition/audit rules. Receipt/audit retention
is not implemented. Receipt snapshots capture first-received evidence; anomaly
source results retain their established retry-replacement behavior. Incident
identity assumes database/dataset names separate independent environments.

Automatic recovery needs explicit observation-order and freshness policy and is
deferred to Phase 5.7 planning, along with retention and delivery policy. No
Slack, email, PagerDuty, external notification providers, paging, RBAC/SSO,
Grafana/Prometheus, automated remediation, or case-management infrastructure was
added. No external messages are sent.
