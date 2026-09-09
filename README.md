# Pulse Data Platform

## CI/CD and automated quality gates

Phase 4.5 adds validation only. [CI](https://github.com/dizzyouth/pulse-data-platform/actions/workflows/ci.yml)
runs for pull requests and pushes to `main`, with no path filters or deployment.
The single **Fast quality gates** job uses `ubuntu-24.04`, Python **3.12**, and
Temurin Java **17** with a 20-minute timeout. New runs cancel obsolete runs for
the same workflow/ref. The token has only `contents: read`; checkout does not
retain credentials. No repository secrets are required.

The workflow installs only `requirements.txt`, checks dependency consistency,
and uses `actions/setup-python` pip caching keyed by that file. It does not cache
data, databases, credentials, dbt artifacts, or Spark checkpoints. Named steps
fail on Python compilation errors, unittest failures (including Spark and BI),
dbt validation errors, invalid Compose configuration, or whitespace errors.
Once the first Actions run exists, repository maintainers can require
**Fast quality gates** in branch protection/rulesets; adding the workflow alone
does not enforce merge blocking. A status badge is deferred until that first run.

### Fast CI versus full integration

The existing unittest framework and discovery are retained. New `test_*.py`
modules are discovered automatically; environment-dependent tests must have an
explicit opt-in guard and a documented reason.

| Tests/checks | Classification | What runs or is required |
| --- | --- | --- |
| `test_event_generator`, `test_kafka_producer`, `test_kafka_consumer` | FAST / CI-SAFE | Deterministic events, CLI subprocesses, and fake Kafka clients; no broker. |
| `test_spark_streaming`, `test_silver_streaming`, `test_gold_build` | FAST / CI-SAFE | Real local Spark transformations, finite file streams, deduplication, and Parquet writes using temporary fixtures. No Kafka connector download or persistent lake data. |
| `test_orchestration` | FAST / CI-SAFE | Fake Airflow DAG/operators plus real Spark dataset validation; no Airflow installation or scheduler. |
| `test_onboarding` | FAST / CI-SAFE | Registries, structured validation, source contracts, credential references, deterministic mock adapters, CLI behavior, and isolated multi-business fixtures; no external account or network. |
| `test_shopify_connector` | CI-SAFE (one Spark path) | Offline GraphQL transport fixtures, pagination, retry/failure mapping, privacy, normalization, checkpoint safety, edits/refunds/cancellations, reporting timezone, and Bronze-to-Gold behavior. Real read-only checks are opt-in. |
| `WarehouseContractTests` | FAST / CI-SAFE | Schema, column, and connection configuration contracts; no database. |
| `test_dbt_project`, `test_bi_config`, `test_ci_config` | FAST / CI-SAFE | Static project/lineage, BI SQL/configuration, mocked provisioning, and CI policy checks; no Metabase or browser. |
| `test_quality` | FAST / CI-SAFE | Typed results, reusable Spark checks, temporary Parquet CLI fixtures, and bounded snapshot reconciliation; no running services. |
| `QualityBoundaryTests`, `QualityBoundarySparkTests` | FAST / CI-SAFE | Runner policy selection, exit codes, JSON logs, and real Spark checks over warehouse snapshot fixtures with mocked database I/O. |
| Windows helper and cleanup-retry unit tests | FAST / CI-SAFE | Mocked OS/retry behavior runs on both systems. Linux helpers leave environment and builder untouched; no Windows native binaries are loaded. |
| `WarehouseIntegrationTests` (two tests) | FULL INTEGRATION | Requires a populated local PostgreSQL warehouse and matching Gold Parquet. Tests refresh the configured warehouse and verify reruns/rollback. |
| `WarehouseQualityIntegrationTests` (one test) | FULL INTEGRATION | Read-only quality validation of all four populated local warehouse tables. |
| `AirflowQualityExecutionTests` (three tests, seven scenarios) | FULL INTEGRATION | Airflow image and isolated SQLite metadata database; actual DAG dependency handling with controlled quality results. |
| `test_quality_persistence` | FAST / CI-SAFE | Execution identity, JSON types, optional sink, and persistence/summary/blocking order with mocked database I/O. |
| `MonitoringPostgresTests` (seven tests) | FULL INTEGRATION | `RUN_MONITORING_INTEGRATION_TESTS=1`; creates and removes its own disposable PostgreSQL database to verify DDL, FK, transactions, JSONB, retries, and concurrent writes. |
| Live Kafka ingestion, complete Airflow DAG, dbt execution, Metabase API/dashboard/browser, native Windows Hadoop loading | FULL INTEGRATION | Manual local acceptance checks require running services, populated data, or Windows native files. These are not additional hidden unittest skips. |

CI explicitly sets `RUN_SPARK_TESTS=1` and
`RUN_WAREHOUSE_INTEGRATION_TESTS=0`. The three warehouse integration methods and
three opt-in Airflow execution tests are skipped, along with the seven opt-in
monitoring PostgreSQL tests. Spark tests remain enabled by default locally. The existing
`RUN_SPARK_TESTS=0` option is useful for a quick non-Spark development check, but
is not equivalent to CI. Windows setup remains documented below.

### dbt and Docker validation

CI runs a fresh `dbt parse --no-partial-parse`, then
`dbt compile --no-introspect --no-populate-cache` against the committed project
and profile. The current models/macros can render all four marts and all 48 data
tests without a database. The flags disable introspection and relation-cache
population ([dbt compile documentation](https://docs.getdbt.com/reference/commands/compile)).
The required profile receives dummy values with `127.0.0.1:1` as an unused
endpoint; `ci-unused` is a placeholder, not a database credential. Anonymous dbt
usage reporting is disabled. Runtime artifacts remain Git-ignored.

This validates YAML, Jinja, references, and SQL rendering. It does **not** ask
PostgreSQL to validate SQL syntax/types/columns, execute models, or run data tests.
Database-dependent macros added later will need an explicit CI strategy; do not
silently bypass compilation failures. An ephemeral PostgreSQL service was
considered, but meaningful execution coverage needs maintained source fixtures;
the existing warehouse tests depend on a populated local snapshot. No database
service or manual integration workflow is added in this phase. Full integration
remains local until an isolated, deterministic fixture lifecycle is available.

`docker compose config --quiet` validates interpolation and Compose structure
using the repository's local defaults. CI does not start containers. Image
availability, image builds, service health, and platform interoperability remain
local checks. The pinned Ubuntu release still receives runner-image updates;
requirements pin direct dependencies, not the complete transitive dependency graph.

### Reproduce CI locally

Use Python 3.12 and Java 17, with the virtual environment activated. On Windows,
first satisfy the Hadoop prerequisites in **Local Spark on Windows** below.
Run each command from the repository root and stop if it returns nonzero:

```text
python -m pip install -r requirements.txt
python -m pip check
python -m compileall -q src tests airflow/dags bi
```

In PowerShell, set the CI test switches and run discovery:

```powershell
$env:RUN_SPARK_TESTS = '1'
$env:RUN_WAREHOUSE_INTEGRATION_TESTS = '0'
$env:SPARK_LOCAL_IP = '127.0.0.1'
python -m unittest discover -s tests -v
```

For offline dbt validation, use a separate PowerShell session so these dummy
values do not replace the connection settings used for full integration:

```powershell
$env:WAREHOUSE_HOST = '127.0.0.1'
$env:WAREHOUSE_PORT = '1'
$env:WAREHOUSE_DB = 'pulse_ci'
$env:WAREHOUSE_USER = 'ci'
$env:WAREHOUSE_PASSWORD = 'ci-unused'
$env:DBT_SCHEMA = 'marts'
$env:DBT_SEND_ANONYMOUS_USAGE_STATS = 'false'
dbt parse --project-dir dbt --profiles-dir dbt --no-partial-parse
dbt compile --project-dir dbt --profiles-dir dbt --no-introspect --no-populate-cache
```

On Ubuntu/macOS, use `export NAME=value` for the same variables, plus
`export TZ=UTC`, and run the same Python/dbt commands. Linux needs no
`winutils.exe`, `hadoop.dll`, or `HADOOP_HOME` override.

```text
docker compose config --quiet
git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol diff --check
git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol diff --cached --check
git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol diff --check origin/main...HEAD
```

The first two Git checks cover unstaged/staged changes; the last covers the
branch's committed changes. Untracked files enter Git's checks once staged.
CI checks the PR base against the tested merge commit, or the previous push SHA
against `HEAD` (all pushed commits). An initial push or unavailable force-push
base falls back to checking the entire tracked tree. Full checkout history
makes the normal comparison bases available.

`python -m unittest tests.test_ci_config -v` validates the parsed workflow's
important contracts. If installed, `actionlint .github/workflows/ci.yml` adds
GitHub Actions schema/expression and shell validation without starting services.
Only a hosted run can verify GitHub checkout, tool setup, and cache behavior.

### Full local regression

Use the real local warehouse environment from `.env.example` (host-side tools
do not automatically read `.env`). Start the existing stack, ensure Bronze,
Silver, Gold, and the warehouse contain a consistent snapshot, then run:

```powershell
docker compose up -d
docker compose ps
$env:RUN_SPARK_TESTS = '1'
$env:RUN_WAREHOUSE_INTEGRATION_TESTS = '1'
python -m unittest discover -s tests -v
python -m src.warehouse.load_gold validate
dbt debug --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
```

The integration tests refresh the local `analytics` tables, so use a local test
warehouse with Gold data matching its current contents. For complete pipeline
and dashboard acceptance, follow the Airflow instructions below and
[`bi/VERIFICATION.md`](bi/VERIFICATION.md). CI does not certify live broker
delivery, scheduler execution, dashboard rendering, or native Windows Hadoop.

## Data Quality Framework (Phase 5.1)

`src/quality/` provides read-only assessment of Spark **batch DataFrames**. It
returns typed results in memory, with optional JSON output. It does not modify
datasets. The calculation API does not write monitoring tables; Phase 5.3 adds
an optional persistence adapter. Phase 5.2 integrates it into the Airflow DAG
as described below. Existing Python
3.12 / Java 17 dependencies and unittest discovery are sufficient; no new
packages, services, or CI workflows are required.

The existing validation remains authoritative at each write boundary: Bronze
checks parsing/required fields and Kafka identity; Silver normalizes, rejects,
and deduplicates events; Gold validates aggregate sanity; PostgreSQL enforces
its serving schema and transactional checks; dbt tests its sources and marts.
The quality framework adds reusable measurements, consistent result reporting,
configurable thresholds, and snapshot reconciliation. It does not replace the
transformation rules; Phase 5.2 adds automatic quality gates around them.

### API and result contract

- `models.py`: immutable rule/context/result dataclasses, status and severity
  enums, summary, blocking decision, and JSON serialization.
- `checks.py`: rule validation, Spark aggregate expressions, and metric evaluation.
- `runner.py`: `run_quality_checks(dataset, rules, context)` and the local CLI.
- `datasets.py`: Silver and Gold/analytics policies. Allowed events reuse
  `SUPPORTED_EVENT_TYPES`; Gold bounds/nullability reuse warehouse `TABLE_SPECS`.
- `reconciliation.py`: explicit bounded-snapshot count checks between layers.

Each `QualityResult` contains `check_name`, `dataset_name`, `layer`, `status`,
`severity`, `metric_name`, `observed_value`, `expected_value`, `checked_at_utc`,
and typed `CheckDetails`. Details can include evaluated/violating row counts,
duplicate count/rate, latest UTC timestamp, and reference/deduplication counts.
No sample customer records are collected into the report.

```python
from src.quality.datasets import silver_rules
from src.quality.models import QualityContext, report_json, should_block, summarize
from src.quality.runner import run_quality_checks

context = QualityContext(dataset_name="silver_valid", layer="silver")
results = run_quality_checks(silver_dataframe, silver_rules(), context)
summary = summarize(results)
print(report_json(results))
blocking = should_block(results)  # Caller decides whether to stop downstream work.
```

`QualityContext` captures one timezone-aware UTC check time per run. Supply a
fixed `checked_at_utc` for deterministic tests and `reference_count` for a
comparable previous snapshot. Rules are configured as Python dataclasses; there
is no separate YAML rules engine.

### Status, severity, and edge cases

A satisfied rule returns **PASS**. A violated **CRITICAL** rule returns **FAIL**;
a violated **WARNING** or **INFO** rule returns **WARN**. INFO identifies an
observation that never blocks, even when its expectation is missed. An
unavailable ratio (empty sample) or absent volume baseline returns WARN at any
severity, with an explanatory detail rather than a fabricated passing metric.

`summarize` returns total checks, passed, warnings, failed, critical failures,
and overall status. Overall **FAIL** means at least one critical FAIL; otherwise
any WARN or noncritical FAIL makes the run **WARN**; otherwise it is **PASS**.
`should_block` is true only for critical FAIL results. An empty rule collection
summarizes as PASS with zero checks; it does not certify any dataset coverage.

Missing columns or incompatible numeric/timestamp/string types produce a result
for the affected check, using its configured severity. Bad rule configuration,
ambiguous duplicate column names, streaming inputs, and Spark execution errors
raise exceptions; operational failures are not disguised as data-quality passes.

| Check | Measurement and semantics |
| --- | --- |
| `RowCount` | Exact count, compared with inclusive `min_rows`. Empty data is zero. |
| `NullRatio` | Null rows / all rows for a column. `0.05` means 5%; empty samples warn. Blank strings require a separate pattern rule. |
| `Uniqueness` | Excess rows beyond one per key group / all rows. Reports both count and rate; composite and null-containing keys are grouped. Completeness is separate. Empty samples warn. |
| `AllowedValues` | Count outside the configured scalar values, with explicit nullable behavior. |
| `NumericBounds` | Count outside inclusive bounds (optionally exclusive minimum). Null handling is configurable; NaN and infinities are invalid. |
| `Pattern` | Count failing a Spark regular expression. Anchor format expressions when a whole-field match is required. |
| `Freshness` | Age in seconds of the latest non-null timestamp, relative to context UTC time. Empty/all-null timestamps violate the rule. Future timestamps beyond configurable tolerance also violate it. Pair with completeness to detect partial nulls. |
| `VolumeChange` | `abs(current - reference) / reference`, with an inclusive deviation threshold. Missing reference warns; zero-to-zero is 0; growth from zero violates the rule with an undefined (JSON null) ratio. No history is inferred or stored. |

Patterns are prevalidated with Python's regular-expression parser and executed
by Spark; use syntax supported by both engines. The supplied Pulse patterns
use simple ASCII character classes and anchors.

Allowed-value, bounds, and pattern checks also warn on empty samples. Add an
explicit row-count minimum when emptiness should block. All optional null rules
apply to nonempty datasets; intentional nulls do not become invalid values.

### Pulse dataset policies and reconciliation

Silver checks source/event-grain uniqueness; non-null business/source/event/customer/session identifiers,
timestamps and dates; nonblank identifiers; allowed event types; quantity **> 0
when present**; nonnegative optional price; and optional uppercase two-letter
country / three-letter currency formats. Zero quantity is rejected because that
is the existing Silver contract. Empty Silver is a warning by default; callers
can choose a critical row-count rule. Freshness and volume limits are opt-in so
historical local demonstration data does not acquire an invented freshness SLA.

Gold policies cover all four tables: unique business grains, required columns,
nonnegative counts/units/revenue, and nullable funnel rates in `[0, 1]`. Null
seller IDs and zero-denominator rates remain valid. Gold country/currency can
be null under the upstream contract, so their completeness checks are
**warehouse-readiness warnings**. `gold_rules(name, layer="analytics")` applies
the existing stricter warehouse nullability and nonempty-table requirements to
a caller-supplied Spark DataFrame. It does not open a PostgreSQL connection.

`reconcile_bronze_silver(..., bounded_snapshot=True)` reuses the existing Silver
classifier with deduplication disabled. If `Q` rows qualify before deduplication,
`U` distinct valid event IDs remain, and `R` rows are rejected, expected Silver
valid is `U` and rejected is `R`. Thus processed Bronze valid input reconciles
as `Silver valid + Silver rejected + (Q - U)`. Rejected rows are not deduplicated.
Bronze's separate invalid-message dataset is outside this reconciliation.

`reconcile_silver_gold(..., bounded_snapshot=True)` requires nonempty customer
and funnel outputs when Silver has rows, daily sales when it has payments, and
product metrics when it has non-null product IDs. No-payment/no-product input
can legitimately yield empty corresponding tables. Aggregate row counts are
not compared for equality with event counts; numeric/grain policies are separate.

Both helpers return normal quality results with critical count failures. They
require matching, stable, finite snapshots and explicit opt-in. Independent
streaming checkpoints, watermark eviction, concurrent writers, and differing
snapshot windows invalidate these count comparisons. Counts do not prove row
identity or content equality: equal-count substitutions require deeper checks
in a future phase. Global Silver uniqueness is a snapshot expectation, not an
unbounded exactly-once promise beyond the streaming watermark.

### Local execution and tests

From the repository root with the virtual environment activated (and the Windows
Spark prerequisites below satisfied):

```text
python -m src.quality.runner silver_valid
python -m src.quality.runner daily_sales
python -m src.quality.runner silver_valid --max-age-hours 24 --reference-count 1000 --max-volume-change 0.2
python -m src.quality.runner silver_valid --path data/silver/marketplace_events/valid --block-on-critical
python -m unittest tests.test_quality -v
python -m unittest discover -s tests -v
```

The CLI reuses configured Silver/Gold paths, the portable Parquet reader, and
Windows Spark setup. Its JSON report is written to stdout. Spark diagnostics
may appear on stderr, and the existing Windows Spark launcher can append native
process-shutdown messages to stdout after the JSON block. For a JSON-only
artifact, write the string returned by `report_json(results)` to a file from
the API; the engine's JSON serialization does not include runtime messages.
The CLI is **report-only by default**, returning 0 even for
data-quality failures; `--block-on-critical` returns 1 for critical failures.
Invalid configuration or unreadable input still exits nonzero. A missing
Parquet directory is an input error, not a fabricated empty DataFrame. Freshness
CLI options apply only to Silver event timestamps, not Gold calendar dates.

The framework batches ordinary metrics into one Spark aggregate query and uses
additional grouped aggregates for exact uniqueness. Only scalar aggregate rows
return to Python; no large collect or pandas conversion is used. Callers own
snapshot consistency and may cache expensive DataFrames around a run, then
unpersist them. The current local reader's file enumeration and exact uniqueness
shuffles remain development-scale limitations. Timestamp freshness requires a
Spark timestamp with instant semantics; timestamp-without-time-zone and strings
must be explicitly normalized by the caller.

Deterministic Spark tests use temporary/in-memory fixtures and are included
automatically by existing CI discovery with `RUN_SPARK_TESTS=1`. Phase 5.2 adds
optional live warehouse and isolated Airflow integration tests. Phase 5.3
persists results separately from this engine. Dashboards, external alerts,
catalogs/lineage, third-party quality
platforms, statistical baselines, and ML anomaly detection are intentionally
deferred.

## Data Quality Orchestration & Observability (Phase 5.2)

The manual `pulse_analytics_pipeline` now runs the Phase 5.1 engine at three
boundaries:

```text
check_bronze_available -> build_silver -> quality_check_silver
  -> build_gold -> quality_check_gold
  -> load_gold_to_warehouse -> quality_check_warehouse -> run_dbt -> test_dbt
```

The quality tasks replace the former `validate_silver`, `validate_gold`, and
`validate_warehouse` DAG tasks. Their standalone validation helpers remain
available. Transformation checks, transactional warehouse loading, dbt commands,
the manual schedule, one retry, and one active DAG run retain their existing
behavior. In particular, the former Silver validator's empty-data failure is
replaced by the Phase 5.1 empty-Silver **WARNING** policy. A later transformation
or warehouse load can still fail its own existing contract.

All three tasks execute `python -m src.quality.runner` with
`--block-on-critical --log-format jsonl --persist` (persistence added in Phase 5.3).
The runner selects `silver_rules()` for
`silver` / `silver_valid`, each table's `gold_rules(name)` for `gold`, and
`gold_rules(name, layer="analytics")` for `warehouse`. Gold and warehouse targets
check all four tables. Warehouse results use `layer="analytics"`, matching the
existing policy and PostgreSQL schema. No rules live in the DAG.

| Severity | Result when an expectation is violated | Airflow behavior |
| --- | --- | --- |
| INFO | Record WARN as an observation | Continue |
| WARNING | Record WARN | Continue |
| CRITICAL | Record FAIL | CLI exits 1; task fails, subject to the existing one retry |

A satisfied rule is PASS at any severity. The Phase 5.1 undefined-metric WARN
semantics are unchanged: severity alone does not fail a task. A critical FAIL
blocks downstream tasks through Airflow's `all_success` dependencies. While a
retry is pending downstream work waits; after retries are exhausted it becomes
`upstream_failed`, including the subsequent warehouse/dbt steps. Results are
logged before the process exits. Unreadable data, invalid schemas, connection
errors, and Spark failures also fail the task; they are never converted to a pass.

In the Airflow UI, open the DAG run, select a `quality_check_*` task, and open
**Logs** for the relevant attempt. Search for `quality_result` and
`quality_summary`. Each result is one flushed JSON line containing dataset,
layer, check name, severity, status, observed/expected values, metric, UTC
timestamp, and check details. The final summary includes explicit
`counts: {"PASS": ..., "WARN": ..., "FAIL": ...}`, total checks, critical failures,
and overall quality status. Counts describe this task attempt; retries are
separate executions. An operational exception emits `quality_execution_error`
and a summary with `completed=false`; those counts cover only checks completed
before the exception and do not certify the whole target. JSON events can be
extracted from the Airflow log prefix. Phase 5.3 adds result tables as described
below; external monitoring services and new XCom payloads remain unnecessary.

Run the same gates locally from the repository root with the virtual environment
active, or substitute `docker compose exec airflow-scheduler python` for `python`:

```text
python -m src.quality.runner silver --block-on-critical --log-format jsonl
python -m src.quality.runner gold --block-on-critical --log-format jsonl
python -m src.quality.runner warehouse --block-on-critical --log-format jsonl
python -m src.quality.runner daily_sales --path data/gold/daily_sales --block-on-critical
python -m unittest tests.test_quality tests.test_quality_orchestration tests.test_orchestration -v
python -m unittest discover -s tests -v
```

Single-dataset CLI commands and the default JSON report remain compatible with
Phase 5.1; omitting `--block-on-critical` remains explicitly report-only. Path,
freshness, and volume overrides apply to individual datasets; `gold` and
`warehouse` reject ambiguous group overrides. No historical freshness SLA or
volume baseline is invented. Snapshot reconciliation remains an explicit API
operation; the DAG does not assume independently captured Bronze/Silver reads
represent the same snapshot.

The warehouse adapter uses the existing `WAREHOUSE_*` connection settings and a
read-only, repeatable-read PostgreSQL transaction across the four `analytics`
tables. It checks column/type contracts and streams rows with server cursors to
temporary JSONL files. Explicit Spark schemas preserve empty tables, dates,
timestamps, and nullable values; the same Spark engine then checks uniqueness,
completeness, counts, bounds, and rates. Temporary files are removed after
evaluation, including on failures. This avoids collecting the warehouse in Python
memory and requires no JDBC driver, but needs temporary disk space proportional
to the warehouse snapshot and assumes the existing local Spark deployment.
As with the existing DAG, avoid concurrent standalone writers.

CI automatically discovers the service-independent policy, logging, and DAG
contract tests. `RUN_WAREHOUSE_INTEGRATION_TESTS=1` additionally runs a read-only
quality check against the populated local warehouse (the existing warehouse
integration suite also tests refresh/rollback). The optional
`tests.test_airflow_quality_integration` runs the actual DAG graph, BashOperator,
CLI, and Airflow dependency handling with controlled quality observations. Run
it inside the Airflow image with `RUN_AIRFLOW_INTEGRATION_TESTS=1`, a temporary
`AIRFLOW_HOME`, and an isolated SQLite `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, after
`airflow db migrate`. It verifies INFO/WARNING continuation and CRITICAL blocking
at all three boundaries without rebuilding platform data. Native Windows and
the Ubuntu CI suite skip those Airflow-only tests.

## Data Quality Observability & Persistence (Phase 5.3)

Quality history lives in the existing warehouse database's **monitoring** schema,
separate from `analytics` serving tables, `marts` views, and the separate Airflow
metadata database. There are two tables and no new service or dependency:

| Table | Grain and contents |
| --- | --- |
| `monitoring.quality_runs` | One completed dataset/layer assessment per execution attempt. UUID primary key; execution source/ID; DAG, Airflow run and task IDs; attempt number, map index, logical timestamp; dataset/layer; start/completion timestamps; overall status, total/PASS/WARN/FAIL counts, critical count, and `should_block`. |
| `monitoring.quality_results` | One check per run. UUID primary key, run UUID foreign key, unique `(quality_run_id, check_name)`, check/metric names, status/severity, observed and expected JSONB values, UTC check timestamp, and JSONB details. |

Times use `TIMESTAMPTZ`; counts and attempts use integers; `should_block` is a
boolean. JSONB preserves numeric, string, boolean, structured, and null values.
Constraints enforce valid statuses/severities, consistent summary counts and
blocking decisions, and ordered timestamps. Indexes support completion time,
dataset/layer/time, layer/time, and status/time. The unique run/check index also
supports the foreign key and run-result joins. A partial index on check time
supports recent CRITICAL FAIL queries without indexing every status/severity.

`src/quality/execution.py` defines database-independent execution envelopes.
`src/quality/persistence.py` owns PostgreSQL writes, using the existing warehouse
connection helper and `WAREHOUSE_*` settings. `checks.py`, policies, and
`run_quality_checks` retain their in-memory behavior. The runner's optional
dataset-completion callback is the integration point; it also supports other
sinks without changing check calculation.

For each completed dataset, the runner calculates results, persists the run and
checks, emits a `quality_persisted` event with its UUID, and emits check logs.
After the target is processed it logs the summary and applies the existing
blocking policy. Thus a CRITICAL FAIL is committed **before** the summary and
nonzero exit that blocks Airflow downstream tasks. INFO/WARNING continue; PASS,
WARN, and FAIL executions are all persisted. Gold and warehouse tasks each
produce four dataset runs; Silver produces one. Warehouse uses layer `analytics`.

Run upsert, scoped replacement of that run's checks, and all result inserts share
one transaction. A result-insert failure rolls everything back, retaining the
previous complete version if one existed. Concurrent writes for the same run
serialize on its parent row. Schema initialization uses repeatable
`CREATE ... IF NOT EXISTS` statements in `src/quality/monitoring.sql`, protected
by a transaction advisory lock against concurrent initialization races. It runs
in its own short transaction before persistence. No Alembic or destructive
schema rebuild is introduced; future schema changes will need explicit upgrades.

### Execution identity and retries

The UUID is deterministically generated from source, execution ID, DAG/run/task,
mapped-task index, attempt number, dataset, and layer. Airflow exports DAG/run/task
context; the DAG passes templated attempt number, map index, and logical date
through the environment with `append_env=True`. Context is never interpolated
into shell commands or SQL. Incomplete Airflow identity fails explicitly.

Repeated writes for **the same attempt** update one run and replace only its
checks, including removal of obsolete checks. A genuine retry gets one separate
attempt record, preserving an earlier critical failure even if the retry passes.
Normal retries therefore create at most one record per dataset per actual
attempt, rather than one record per insert. A manual task clear/re-execution that
increments Airflow's attempt number also creates a new attempt. There is no
automatic deletion of history. Query trends can include all attempts or select
the latest attempt per logical execution, depending on the desired measure.

Local persistence is **off by default**, including under unittest/CI. `--persist`
uses source `cli` and a new execution UUID unless `--execution-id` is supplied.
Reuse that explicit ID to rewrite the same local attempt; use `--attempt-number`
with the same ID to retain separate attempts. Omit persistence for report-only
development; `--block-on-critical` still independently controls quality exit codes.

```text
python -m src.quality.runner silver --persist --block-on-critical --log-format jsonl
python -m src.quality.runner gold --persist --execution-id local-review-001
python -m src.quality.runner gold --persist --execution-id local-review-001
python -m src.quality.runner gold --persist --execution-id local-review-001 --attempt-number 2
python -m src.quality.runner warehouse --persist --block-on-critical --log-format jsonl
python -m src.quality.persistence
```

The final command only ensures the monitoring schema/tables exist. Host commands
use the existing host warehouse settings; Compose runs use the container settings.
No new passwords or CI secrets are required. Initialization requires schema/table
creation rights. Persistence errors expose a sanitized message, fail the task,
and are not silently ignored even if quality passed. They do not log connection
settings or PostgreSQL diagnostics that might contain sensitive values.

### Queries, validation, and limits

[`monitoring/queries.sql`](monitoring/queries.sql) provides: latest quality status
per layer across each dataset's latest completed Airflow assessment; daily warning
and failure counts; frequently failing checks; recent critical failures; and one
dataset's history with duration. Run it in a PostgreSQL SQL client connected to
the warehouse, or from PowerShell:

```powershell
Get-Content monitoring/queries.sql | docker compose exec -T warehouse-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1'
python -m unittest tests.test_quality_persistence tests.test_orchestration.AirflowDagContractTests -v
$env:RUN_MONITORING_INTEGRATION_TESTS = '1'
python -m unittest tests.test_monitoring_postgres -v
```

PostgreSQL tests require permission to create/drop their own uniquely named
disposable test database; they never clear existing monitoring history. Existing
Airflow execution tests can additionally enable `RUN_MONITORING_INTEGRATION_TESTS=1`
to use real persistence and verify committed rows before summaries. Run those
with isolated Airflow metadata and a dedicated test warehouse database. The usual
CI discovery keeps database/Airflow tests opt-in and adds no services or secrets.

Timing measures quality calculation for each dataset, not the entire task's Spark
startup, snapshot extraction, persistence, or whole pipeline duration. Completed
dataset writes are independent: if a later dataset fails operationally, earlier
completed datasets remain queryable. Calculation failures before completion and
unavailable persistence are still observable in Airflow logs, but have no fabricated
completed monitoring record. Latest completed status alone cannot prove freshness
or detect an absent run; queries expose timestamps and dataset coverage for that
reason. Rewriting the same attempt retains its last committed assessment, not
every write; concurrent rewrites use the same last-committer-wins behavior.

`pipeline_runs` is deliberately deferred: Airflow remains authoritative for DAG
start/end/state, and quality rows already retain correlation IDs and timing.
Monitoring history grows over time; automatic retention is deferred to operational
hardening. Phase 5.4 adds monitoring dashboards on this history; alerting remains
deferred. Phase 5.3 itself adds no Metabase dashboards, external observability platform,
notifications, statistical baselines, ML anomaly detection, or lineage system.

## Local Spark on Windows

Java 17 must be installed and `JAVA_HOME` must point to it. The Spark entry
point automatically configures the remaining Windows compatibility settings
for its own process:

- `HADOOP_HOME` resolves to `<project-root>/tmp/hadoop`.
- `TEMP` and `TMP` resolve to `<project-root>/tmp/spark`.
- `<project-root>/tmp/hadoop/bin` is prepended to the process-local `PATH` so
  Hadoop can load `hadoop.dll`.
- Spark's Ivy dependency cache resolves to `<project-root>/tmp/spark/ivy`.
- Spark's local working directory resolves to `<project-root>/tmp/spark/local`.

Keep `winutils.exe` and `hadoop.dll` in `tmp/hadoop/bin/`. The entire `tmp/`
directory is Git-ignored, including these machine-local compatibility files
and all Spark runtime output.

No user or system environment variables are changed. Existing process-level
values are respected, so a developer can still override them for one shell.

From the project root, run:

```powershell
python -m src.streaming.spark_streaming
```

## Bronze marketplace events

The Spark entry point persists Kafka records as append-only Parquet files in
two independently checkpointed streams:

```text
data/bronze/marketplace_events/
|-- valid/
|   `-- ingestion_date=YYYY-MM-DD/
`-- invalid/
    `-- ingestion_date=YYYY-MM-DD/

data/checkpoints/bronze/marketplace_events/
|-- valid/
`-- invalid/
```

Each record retains the parsed marketplace fields, Kafka key/topic/partition/
offset/timestamp, the original `raw_json`, `validation_errors`, and a Spark-
generated UTC ingestion timestamp. `ingestion_date` is derived from that UTC
timestamp and is used as a low-cardinality partition for practical local file
layout. Invalid messages retain any fields Spark could recover and are never
silently discarded.

The four output and checkpoint locations are configured in `.env.example`.
Relative values are resolved from the project root; absolute overrides are
also supported. Generated Bronze data and checkpoints remain covered by the
existing `data/*` Git ignore rule.

Spark checkpoints record source progress and file-sink commits, preventing
normal restarts of the same query from reprocessing committed offsets. This is
not a general exactly-once guarantee for arbitrary external side effects,
manual checkpoint deletion, or output/checkpoint path changes.

## Silver marketplace events

Run the bounded, test-friendly Silver stream from the project root with:

```powershell
python -m src.streaming.silver_streaming
```

The job reads only Bronze valid Parquet and writes two independently
checkpointed streams:

```text
data/silver/marketplace_events/
|-- valid/
|   `-- event_date=YYYY-MM-DD/
`-- rejected/
    `-- event_date=YYYY-MM-DD/

data/checkpoints/silver/marketplace_events/
|-- valid/
`-- rejected/
```

Silver trims identifiers, lowercases event types, uppercases country and
currency codes, preserves typed timestamps/numbers, and derives `event_date`
in UTC. Valid output retains Kafka and ingestion lineage but omits Bronze
`raw_json` and validation errors. Rejected rows retain `raw_json` and add
`silver_validation_errors` for diagnosis.

Quality rules reject missing core identifiers or timestamps, unsupported event
types, non-positive quantities, negative prices, non-two-letter country codes,
and non-three-letter currency codes. Records are quarantined rather than
silently dropped.

Quality-valid records use a seven-day event-time watermark by default and
`dropDuplicatesWithinWatermark` on `event_id`. This removes repeated event IDs
while bounding streaming state; a duplicate arriving after the watermark
horizon is not guaranteed to be recognized. `event_date` is used for storage
partitioning because it supports common time-range queries without creating
the tiny partitions that high-cardinality customer or event IDs would cause.

Paths and the watermark are configurable through `.env.example`. Checkpoints
track each sink independently; changing or deleting them changes replay
behavior and may produce duplicate Parquet rows. The local job defaults to
four shuffle partitions to avoid excessive state-store files for this
development-scale dataset.

## Gold marketplace analytics

Build all Gold tables from the current Silver valid snapshot with:

```powershell
python -m src.analytics.gold_build
```

The batch build reads only `data/silver/marketplace_events/valid` and replaces
four query-ready Parquet datasets:

- `daily_sales`: successful payments grouped by UTC event date, country, and
  currency. Distinct paid order IDs define completed orders; revenue is the
  sum of `quantity * unit_price`, and average order value divides gross revenue
  by distinct completed orders.
- `customer_metrics`: lifetime event counts, first/last activity, paid units,
  gross paid revenue, and distinct paid orders per customer.
- `product_metrics`: product activity, paid units/revenue, and distinct
  customers. `seller_id` is retained only when the observed product-to-seller
  mapping is unambiguous.
- `funnel_metrics`: daily country-level event counts and adjacent-stage
  conversion ratios. A zero denominator produces null rather than division by
  zero.

Refunds are counted separately and do not reduce gross revenue in this first
version. Gold validates non-negative sales measures and conversion rates in
the range `[0, 1]` before writing.

`daily_sales` and `funnel_metrics` are partitioned by `event_date` for common
date-range filtering. Customer and product tables are not partitioned by their
high-cardinality identifiers. Output paths are configurable in `.env.example`
and generated data remains ignored by Git.

This phase performs a full overwrite from a consistent Silver snapshot. The
table builders and writers are separate so a future orchestrator can replace
the full refresh with partition-scoped incremental builds. Currency values are
not converted, refunds are not netted from revenue, and funnel rates are based
on event counts rather than cohort/session attribution.

## PostgreSQL analytics warehouse

PostgreSQL 16.4 is the local serving/query layer for the Gold snapshot. It is
deliberately separate from Airflow's PostgreSQL instance:

- `airflow-postgres` stores Airflow metadata only and is not host-published.
- `warehouse-postgres` (container `pulse-warehouse-postgres`) stores the
  `analytics` serving schema and is published at `localhost:5433`.

Host-side commands use `localhost:5433`. Docker services use
`warehouse-postgres:5432`. Both connect to database `pulse_analytics` as user
`pulse` by default. These are local-development credentials from
`.env.example`; override them in the untracked `.env` file outside local use.

Start the warehouse and load or validate it manually from the host with:

```powershell
docker compose up -d warehouse-postgres
python -m src.warehouse.load_gold load
python -m src.warehouse.load_gold validate
```

The loader creates four explicitly typed relational tables:

- `analytics.daily_sales`, indexed by `event_date`
- `analytics.customer_metrics`, indexed by `customer_id`
- `analytics.product_metrics`, indexed by `product_id`
- `analytics.funnel_metrics`, indexed by `event_date`

Each run reads all four Gold Parquet datasets into staging tables, checks their
required columns and aggregate constraints, and publishes all four datasets in
one PostgreSQL transaction. Existing tables are truncated and refilled in place
so dependent dbt views remain valid; first-time tables are promoted from staging.
A failure rolls back the whole refresh, leaving the previous serving snapshot
available. This is a rerunnable full refresh, not incremental loading or CDC.

Connect with any PostgreSQL client (for example,
`psql -h localhost -p 5433 -U pulse -d pulse_analytics`) and query:

```sql
SELECT * FROM analytics.daily_sales ORDER BY event_date LIMIT 10;

SELECT customer_id, total_revenue
FROM analytics.customer_metrics
ORDER BY total_revenue DESC
LIMIT 10;

SELECT product_id, gross_revenue
FROM analytics.product_metrics
ORDER BY gross_revenue DESC
LIMIT 10;
```

## dbt warehouse marts and lineage

dbt adds warehouse-native presentation models, tests, documentation, and
lineage after the Spark-owned Gold snapshot reaches PostgreSQL. Responsibilities
remain deliberately separated:

- Spark owns Bronze/Silver transformations, Gold business aggregations, and
  Parquet outputs.
- The warehouse loader owns the transactional `analytics` serving snapshot.
- dbt treats those four tables as read-only sources and builds lightweight
  PostgreSQL views in `marts`; it does not reproduce the Spark aggregations.

The project lives in `dbt/`: `models/sources.yml` describes and tests the four
`analytics` sources, `models/marts/` contains the four presentation views and
their documentation, `macros/` contains the two small reusable range tests,
and `profiles.yml` reads connection values exclusively from environment
variables. Generated `target/` and `logs/` directories are ignored.

The lineage graph is intentionally compact:

```text
analytics.daily_sales       -> marts.revenue_by_day
analytics.customer_metrics  -> marts.top_customers
analytics.product_metrics   -> marts.top_products
analytics.funnel_metrics    -> marts.funnel_performance
```

All marts are views. `revenue_by_day` combines countries only within the same
date and currency, retaining `currency` so unlike monetary units are never
summed. Customer and product marts add descending revenue ranks. Funnel metrics
remain at date-country grain with their existing event-count conversion
semantics.

Install `requirements.txt`, export the warehouse values shown in
`.env.example`, and run from the repository root:

```powershell
dbt debug --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
dbt docs generate --project-dir dbt --profiles-dir dbt
```

Source tests cover required identifiers/dates, uniqueness at customer and
product grain, non-negative aggregates, and nullable funnel rates constrained
to `[0, 1]`. Mart tests repeat important presentation-layer identity, revenue,
and rate contracts. Generated documentation includes direct source-to-mart
lineage and column descriptions.

Current limitations follow the upstream snapshot: dbt does not perform
incremental processing, currency conversion, refund netting, or cohort/session
funnel attribution. Documentation is generated locally but is not committed.

## Metabase BI consumption

Metabase `v0.63.16.5` provides the local BI UI at
[http://localhost:3000](http://localhost:3000). It consumes PostgreSQL dbt marts,
not Parquet or the Bronze/Silver layers, so dashboard users see documented and
tested presentation contracts without changing the Spark-to-warehouse design.

The three PostgreSQL responsibilities remain physically separate:

- `airflow-postgres` stores only Airflow metadata.
- `warehouse-postgres` stores Pulse `analytics` tables and `marts` views.
- `metabase-postgres` stores only Metabase users, questions, dashboards, and
  other application metadata in the `pulse_metabase` database and an isolated
  Docker named volume. It is not published to the host.

Copy `.env.example` to the ignored `.env` file and replace the local-only
passwords if desired. Start the stack and inspect service health with:

```powershell
docker compose config --quiet
docker compose up -d
docker compose ps
```

Before provisioning dashboards, initialize the monitoring tables and views using
the commands in [Phase 5.4](#phase-54-monitoring-dashboard-and-operational-health).
On a fresh stack, populate the dbt marts and complete that initialization, then
rerun `docker compose run --rm metabase-setup` if the initial setup job ran early.

The one-shot `metabase-setup` service uses the pinned Metabase API to create the
first local admin and register the analytics connection idempotently. Its local
defaults are `admin@pulse.local` / `PulseLocal!4xN7qB2v`; override
`METABASE_ADMIN_EMAIL` and `METABASE_ADMIN_PASSWORD` in `.env` before the first
startup. The registered database is named `Pulse Analytics Warehouse` and uses:

```text
type: PostgreSQL
host: warehouse-postgres
port: 5432
database: pulse_analytics
user/password: WAREHOUSE_USER / WAREHOUSE_PASSWORD
```

If Metabase was initialized earlier with different admin credentials, update the
two admin variables to match and rerun `docker compose up metabase-setup`. The
setup job verifies its connection by querying all four non-empty marts. Its
application-database settings (`MB_DB_*`) point only to `metabase-postgres` and
must not be changed to the analytics warehouse. Sample content, anonymous usage
tracking, update checks, and AI features are disabled for this local BI service.
Its JVM is capped at 512 MB and uses reduced local-development thread pools so
it can remain online while the Spark/Airflow pipeline runs.

### Pulse Marketplace Overview dashboard

The setup service also reconciles the `Pulse Marketplace` collection, six saved
questions, and **Pulse Marketplace Overview** dashboard through the pinned API.
Rerun with `docker compose run --rm metabase-setup`. Existing IDs and unrelated
dashboard cards are preserved. See `bi/DASHBOARD.md` for layout and filter scope;
verified plain PostgreSQL queries live in `bi/queries/`.

The provisioned dashboard contains:

- revenue totals, orders, units, weighted average order value, and a date trend;
- funnel stage volumes and adjacent-stage conversion rates;
- top-customer operational rankings and purchase measures;
- top-product operational rankings and product measures; and
- country-level funnel volume and weighted conversion performance.

Use event-date and currency filters for revenue cards, and event-date and country
filters for funnel/geography cards. Revenue cards must retain currency as a
group or require a single-currency filter; Pulse does not have exchange rates and
must never present unlike currencies as one monetary total. The current upstream
customer/product lifetime marts do not carry currency. Their BI cards therefore
omit revenue and revenue rank, and rank units purchased instead.

The four core queries are:

```sql
SELECT event_date, currency, gross_revenue, completed_orders, units_sold,
       avg_order_value
FROM marts.revenue_by_day
ORDER BY event_date, currency;

SELECT customer_id, total_units_purchased, payments_completed, distinct_orders
FROM marts.top_customers
ORDER BY total_units_purchased DESC, customer_id
LIMIT 20;

SELECT product_id, seller_id, units_sold, payments_completed, distinct_customers
FROM marts.top_products
ORDER BY units_sold DESC, product_id
LIMIT 20;

SELECT event_date, country, product_views, cart_adds, checkouts_started,
       orders_created, payments_completed, view_to_cart_rate,
       cart_to_checkout_rate, checkout_to_order_rate, order_to_payment_rate
FROM marts.funnel_performance
ORDER BY event_date, country;
```

Current limitations: customer/product cards are lifetime unit rankings; date,
country, and currency filters do not apply to them. Authentication
and database traffic are unencrypted local-development connections, Metabase
uses the existing broad local warehouse user rather than a dedicated read-only
role, and no currency conversion or country-revenue mart exists yet.

## Phase 5.4: Monitoring dashboard and operational health

**Pulse Platform Health** exposes persisted quality history in Metabase alongside
the existing **Pulse Marketplace Overview** dashboard. It measures recorded
quality assessments, not service uptime or whether a currently running pipeline
has finished. Airflow quality gates, their blocking behavior, and business dbt
marts are unchanged.

The read-only PostgreSQL presentation schema `monitoring_views` consumes
`monitoring.quality_runs` and `monitoring.quality_results`. Direct views keep
monitoring available when a CRITICAL gate prevents downstream dbt from running.
View initialization is explicit and transactional, uses existing `WAREHOUSE_*`
configuration, and does not write quality history. Metabase setup only reads
these views and reconciles its own saved questions/dashboard metadata.

| View | Grain and meaning |
| --- | --- |
| `quality_history` | One persisted dataset execution/attempt, with UTC completion date and duration in seconds |
| `latest_quality_status` | Latest completed execution per dataset/layer, across all execution sources and attempts |
| `check_history` | One check result, preserving JSONB observations/expectations and execution context |
| `check_failure_summary` | All-time counts per dataset/layer/check; windowed questions aggregate `check_history` before grouping |
| `recent_critical_failures` | Only check status FAIL **and** severity CRITICAL; callers choose window, ordering, and limit |
| `current_health` | One row for each required layer: `silver`, `gold`, and `analytics` (Warehouse) |

Latest means greatest `completed_at_utc`, with UUID as a deterministic tie-breaker;
it does not mean most recently inserted, greatest logical date, or worst historical
status. Separate retry attempts count as separate runs; repeated persistence of
the same attempt does not. A run is one dataset assessment, not a whole DAG run.

Current health expects `silver_valid` in Silver and the four registered Gold
datasets (`daily_sales`, `customer_metrics`, `product_metrics`, `funnel_metrics`)
in each of Gold and analytics. Missing coverage is **UNKNOWN**, never PASS.
A known blocking failure takes precedence over UNKNOWN; otherwise complete
coverage is WARN if any latest dataset run warns, and PASS if all pass. The
oldest of the latest dataset timestamps exposes uneven coverage. The latest
successful check timestamp means one passing check, not a fully successful run.
Custom datasets remain visible in history/latest views but do not change this
required-pipeline coverage summary. Keep the expected inventory aligned if
pipeline policies change.

Severity is a check's importance; status is its outcome. A PASS with WARNING or
CRITICAL severity is successful. WARN outcomes include nonblocking violations
and undefined metrics. Only FAIL + CRITICAL blocks processing. Historical check
counts and run counts are labeled separately; a single run can contain many checks.

Initialize and provision (safe to rerun, one setup process at a time):

```powershell
# Existing Phase 5.3 initialization also supports an empty warehouse history.
python -m src.quality.persistence
python -m src.warehouse.monitoring
docker compose run --rm metabase-setup
```

Use the activated local virtual environment and existing warehouse environment
variables. Alternatively, run both Python module commands with
`docker compose exec -T airflow-scheduler python -m ...` to use the container's
warehouse configuration. Business marts must already exist for Marketplace's
existing setup verification. Missing monitoring views produce an actionable
setup error. No database migrations, new services, dependencies, or secrets are
required. PostgreSQL rejects DML through these presentation views; the existing
broad local warehouse role still has access to underlying tables.

Open the setup command's dashboard URL, or **Pulse Monitoring → Pulse Platform
Health** at [Metabase](http://localhost:3000). Eight cards show current health by
layer, latest status by dataset, a daily run trend, a run-status distribution,
recent warnings, recent failed CRITICAL checks, the top 20 warning/failing checks,
and run/incident counts by layer. Incident tables show at most the latest 100
matches. Empty incident cards mean no matching recorded incidents, not missing
or broken queries.

| Filter | Supported cards |
| --- | --- |
| Layer (`silver`, `gold`, `analytics`) | All cards |
| Dataset (exact registered name) | All except the whole-layer coverage summary |
| Run status (`PASS`, `WARN`, `FAIL`) | Historical run trend, status distribution, run/incident counts by layer |
| Start / end date (inclusive UTC date range) | Historical run and incident cards; never current health or latest dataset status |

Unset dates mean all history. Incident dates use check time; run cards use
completion time. Fixed WARN/FAIL incident cards intentionally do not accept the
Run status filter. Filters are parameterized, and only supported mappings are
provisioned. Clear filters to return to all recorded history. Rerunning setup
reuses managed object IDs, refreshes questions/mappings, and preserves unrelated
cards and layouts. Existing persistent Metabase metadata storage is unchanged.

For manual SQL, see [presentation queries](monitoring/presentation_queries.sql)
and the [Phase 5.3 examples](monitoring/queries.sql). The prepared window example
accepts configurable dates and an optional layer. Views contain no fixed period
or retention rule. The Phase 5.3 dataset/layer/completion and partial failed-CRITICAL
indexes were reviewed. The live latest-status plan uses a small sequential scan
and sort; no additional indexes were needed for the local history. Presentation
queries scan history, so larger deployments should revisit plans and filtering
before adding indexes or materialization.

Focused validation: set `RUN_MONITORING_INTEGRATION_TESTS=1`, then run
`python -m unittest tests.test_monitoring_presentation -v`. PostgreSQL semantics
tests create and remove their own disposable database; ordinary CI runs the
configuration/provisioning contracts without requiring PostgreSQL.

Limitations: no staleness SLA or live service probes, all execution sources are
included, local BI uses the existing shared warehouse role, and history grows
without automatic retention. Dashboard refinements, alerts, operational access hardening,
retention, and freshness policies may evolve in Phase 5.5 or later. This phase
adds no notifications, Grafana, Prometheus, external observability, ML anomaly
detection, statistical baselines, OpenLineage, or production deployment.

## Phase 5.5: Explainable anomalies and internal alerts

Phase 5.5 adds deterministic behavior monitoring without changing the fixed data
contracts. The consolidated Airflow `anomaly_check` runs after
`quality_check_warehouse` and before dbt. It reads PostgreSQL in read-only mode,
evaluates quality history plus current analytics series, then atomically persists
the evaluation and any internal alert events. It performs no Spark work and is
nonblocking by default, including CRITICAL heuristic anomalies. Local operators
may explicitly opt into blocking with `--block-on-critical`; the production DAG
does not set that flag.

The initial anomaly series are:

- dataset row counts from the persisted `row_count` quality metric;
- warning and failure check counts per logical dataset quality execution;
- daily completed-order volume, aggregated across the daily-sales rows;
- daily gross revenue separately for each currency (currencies are never summed);
- each non-null funnel conversion rate separately for each country.

Airflow retries are collapsed to the latest attempt before quality baselines are
built. Business series use their event date. Schema, required columns, null and
duplicate limits, allowed values, nonnegative measures, rate bounds, freshness,
and reconciliation remain fixed Phase 5.1 rules because they are contracts rather
than learned behavior.

Each series requires seven prior observations by default, configurable locally
with `--minimum-history`. Fewer observations produce `INSUFFICIENT_HISTORY` with
INFO severity; that state is neither NORMAL nor ANOMALY. With enough history, the
baseline is the median and dispersion is median absolute deviation (MAD). A
nonzero MAD uses a modified z-score with inclusive WARNING/CRITICAL boundaries
of 3.5/6.0. A flat nonzero baseline uses absolute percentage deviation; volume
and revenue boundaries are 50%/90%, while funnel rates use 25%/50%. A flat zero
baseline uses absolute change: warning/failure counts use 1/3 checks. Every row
stores current and baseline values, signed deviation, percent deviation when
defined, method, thresholds, history count, score/details, dimensions, timestamp,
severity, and a plain-language explanation.

`NORMAL`, `ANOMALY`, and `INSUFFICIENT_HISTORY` describe statistical outcome.
`INFO`, `WARNING`, and `CRITICAL` describe importance. NORMAL and insufficient
results are INFO. Anomalies cross the configured WARNING or CRITICAL magnitude;
heuristic severity does not silently inherit Phase 5.2 blocking behavior.

PostgreSQL adds two tables under the existing `monitoring` schema:

| Table | Meaning and idempotency |
| --- | --- |
| `anomaly_results` | One metric series in one logical evaluation. Evaluation/result UUIDs exclude retry attempt; a retry transaction replaces the same evaluation and records the latest attempt. |
| `alert_events` | Internal conditions requiring attention. Sources are anomaly WARNING/CRITICAL or fixed-rule FAIL+CRITICAL. Deterministic identity prevents duplicate events for the same DAG run/task/series or quality check across retries. |

Phase 5.5 introduced alerts with an `OPEN` status and deferred acknowledgement
and resolution operations. Phase 5.6 retains lifecycle and audit history when a
retry replaces the anomaly evaluation inside one transaction. There is no
external delivery. For existing critical quality failures, the
transaction writes the quality run and results first, then its CRITICAL alert.
Only after commit does the runner log the summary and return failure, preserving:

```text
quality result persisted -> internal alert persisted -> task fails -> downstream blocked
```

An anomaly evaluation and all of its derived alerts are one transaction. A failed
write leaves the previous complete retry state in place. Re-running the same local
`--execution-id`, or retrying the same Airflow run/task, replaces that logical
evaluation rather than appending duplicates. Distinct DAG runs remain distinct
history.

Initialize and run locally with existing `WAREHOUSE_*` configuration:

```powershell
python -m src.quality.persistence
python -m src.warehouse.monitoring
python -m src.quality.anomaly_runner --log-format json
python -m src.quality.anomaly_runner --persist --execution-id local-anomaly-review
```

Persistence is opt-in locally; ordinary unit tests and report-only runs do not
write PostgreSQL. The Airflow command always persists and emits structured JSONL
events for each result and a NORMAL/ANOMALY/INSUFFICIENT_HISTORY summary.

The read-only `monitoring_views` schema adds `recent_anomalies`,
`recent_alert_events`, `anomaly_summary_by_metric`, and
`alert_summary_by_severity`. “Recent” views deliberately contain no fixed period;
queries choose their inclusive UTC window. Manual examples remain in
`monitoring/presentation_queries.sql`.

**Pulse Platform Health** retains its existing eight cards and filters, and adds
four cards: recent anomalies, recent internal alerts, anomalies by metric, and
alerts by severity. Layer, Dataset, Severity, Start date, and End date map to
these cards. Run status remains mapped only to quality-run cards because alert
status and anomaly status have different meanings. Incident tables show the
latest 100 matching records and the metric chart shows the top 20 series.

The current local history is intentionally sparse, so a real evaluation normally
produces `INSUFFICIENT_HISTORY`. This is safe and expected. Deterministic isolated
PostgreSQL fixtures cover stable baselines, spikes, drops, actual alert generation,
and retry replacement without modifying live quality history.

Current limitations: there is no seasonality, day-of-week adjustment, forecasting,
multivariate model, freshness SLA, alert delivery, escalation, suppression window,
automatic acknowledgement, remediation, or retention. Sparse and irregularly
spaced observations are compared as ordered values. Phase 5.6 adds local lifecycle operations below. External delivery remains deferred. Slack,
email, PagerDuty, Grafana, Prometheus, external ML, neural networks, complex
forecasting, OpenLineage, and production paging remain out of scope.

## Phase 5.6: Alert lifecycle and operational response

Internal alerts now have an operational lifecycle independent of quality and
anomaly calculation. No external notification service is involved.

| State | Exact meaning | Valid next states |
| --- | --- | --- |
| `OPEN` | Requires attention | `ACKNOWLEDGED`, `RESOLVED` |
| `ACKNOWLEDGED` | An operator has accepted ownership; the condition may persist and the alert is still active | `RESOLVED` |
| `RESOLVED` | Operator has closed this incident instance; it is no longer active | None |

Repeated acknowledgements/resolutions and all other transitions fail explicitly.
There is no reopen or suppression state. Resolution does not rewrite a quality
result, unblock a failed Airflow task, or turn an anomaly into NORMAL.

### Incident identity and recurrence

`src/quality/alert_service.py` is the shared transactional service. `incident_key`
is `v1:` plus SHA-256 of UTF-8 JSON containing this ordered array:
`["pulse-incident-v1", source_type, dataset_name, layer, metric_name, check_name, dimensions]`.
JSON keys are sorted, separators are compact, and non-finite numbers are rejected.
For anomalies, check name is JSON null and dimensions retain the exact series
values (for example currency or country). For quality failures, check name is
required and dimensions are currently `{}` because checks assess whole datasets.
Names are case-sensitive. Severity, observed values, thresholds, timestamps,
execution source, DAG/task/run IDs, map index, and retry attempt are excluded.
Consequently, the same dataset contract or series shares one incident across
local and Airflow executions. Different datasets/layers/checks/dimensions remain
separate. Policy changes do not by themselves create another incident.

A PostgreSQL partial unique index permits at most one OPEN or ACKNOWLEDGED
instance per key. Transaction advisory locks serialize concurrent creation,
recurrence, and operator transitions. A new logical occurrence updates first/last
observation times using min/max and increments `occurrence_count`. ACKNOWLEDGED
remains ACKNOWLEDGED. Severity retains the highest urgency seen in the instance;
latest evidence follows observation time. Older arrivals count without replacing
newer evidence. Observation timestamps, rather than retry wall-clock timestamps,
drive first/last seen. These fields describe detections, not polling heartbeats.

`monitoring.alert_occurrences` stores one receipt per logical occurrence. It uses
the existing Phase 5.5 UUID identities: quality's `pulse-quality-alert-v1` plus
dataset/layer/check, or anomaly's `pulse-anomaly-alert-v1` plus anomaly result UUID,
within the execution context. These identities include source/run/task/map index
but exclude attempt number. Replaying the same logical execution updates active
evidence without incrementing the count or adding a transition. Its receipt
survives resolution, so an old retry cannot open a new incident. A genuinely new
logical execution after resolution creates a new UUID instance with count 1 and
the same incident key. Counts measure distinct logical executions, not retries,
unique observation timestamps, or elapsed time.

### Persistence, audit, and recovery policy

`monitoring.alert_events` retains its original columns and UUID/source context.
The existing `status` column now accepts all three states; `lifecycle_status` is a
stored generated alias, so these values cannot disagree. Added fields are
`incident_key` (TEXT), first/last seen and acknowledgement/resolution timestamps
(TIMESTAMPTZ), occurrence count (BIGINT), operator names and resolution note
(TEXT). Source UUID/context/details describe the latest evidence. Source IDs are
polymorphic references without a foreign key because source evaluations can be
replaced by retries. JSONB occurrence snapshots retain first-received evidence.

`monitoring.alert_event_history` is a minimal transition audit: generated history
ID, alert UUID foreign key, previous/new status, UTC database timestamp, actor,
and optional note. Opening, acknowledgement, and resolution are recorded.
Recurrences use occurrence receipts rather than noisy status-to-same-status audit
entries. Source persistence, receipt/count changes, and opening audit share one
transaction. Operator state changes and audit insertion also share one transaction;
a failed audit write rolls back the state change.

Quality ordering remains: **quality rows written ? alert created/updated ? atomic
commit ? quality result/summary logged ? blocking exit ? Airflow downstream blocked**.
Only FAIL+CRITICAL quality checks alert. WARNING/CRITICAL anomalies use the same
service and remain nonblocking by default. The DAG graph and quality/anomaly
calculation policies are unchanged. Anomaly retries replace evaluation results
but never delete incident history or ownership.

**Resolution is manual for both sources.** NORMAL, INSUFFICIENT_HISTORY, a missing
series, and a passing quality retry do not resolve alerts. A NORMAL observation
can be historical or revised during retry; safe automatic recovery needs explicit
observation-order and freshness policy. Operators should inspect current evidence
before resolving, and use a resolution note. Automatic resolution is deferred.

Initialization is repeatable and transactional:

```powershell
python -m src.quality.persistence
python -m src.warehouse.monitoring
```

The migration preserves old event IDs and evidence, backfills fingerprints and
receipts, and audits import. If Phase 5.5 contains several OPEN events for one
condition, the oldest becomes the active representative with the combined count.
Other rows remain queryable as RESOLVED with an explicit migration consolidation
note pointing to that representative; this records consolidation, not recovery.
An orphan legacy anomaly whose metric cannot be recovered receives an isolated
legacy identity instead of being merged speculatively. No historic quality or
anomaly result is rewritten by the migration. New raw inserts must supply the
new incident fields; application producers should use the shared service.

### Local operations

Commands use the existing `WAREHOUSE_*` environment configuration; Airflow is not
required. `--by` is a required nonblank free-text name, not an authenticated identity.

```powershell
python -m src.quality.alert_cli list
python -m src.quality.alert_cli list --status ACKNOWLEDGED --layer analytics
python -m src.quality.alert_cli list --status ALL --dataset daily_sales --severity CRITICAL --limit 100
python -m src.quality.alert_cli acknowledge <alert_id> --by alice
python -m src.quality.alert_cli resolve <alert_id> --by alice --note "Validated current data and recovery"
```

List defaults to active alerts, newest last-seen first, limit 100 (maximum 1000).
An empty result is `[]`. Commands output JSON, exit 0 on success, and exit 1 for
missing alerts, invalid transitions, or sanitized persistence failures. Argument
syntax errors exit 2. Listing is read-only and assumes initialization is complete.
Service callers can use `record_alert(cursor, ...)`, `acknowledge_alert`,
`resolve_alert`, and `list_alerts`; producers must commit the supplied cursor's
transaction. Supported operations go through this service. Direct database writes
can bypass application transition/audit rules; DB access is trusted local development.

### Operational views and dashboard

All presentation views remain read-only and have no implicit retention window.

| View | Meaning |
| --- | --- |
| `monitoring_views.active_alerts` | OPEN and ACKNOWLEDGED instances |
| `monitoring_views.alert_history` | Every incident instance, including resolved history |
| `monitoring_views.alert_summary_by_status` | Instance and occurrence totals by lifecycle/source/severity/dataset/layer |
| `monitoring_views.alert_summary_by_severity` | Existing compatible instance totals by source/severity/status |
| `monitoring_views.recurring_alerts` | Instances with more than one logical occurrence, including resolved instances |

Detail views include key, lifecycle, severity, dataset/layer, first/last seen,
count, operator context, and duration in seconds from first detection to resolution
(or current time for active incidents). Transition-level audit remains queryable
in `monitoring.alert_event_history`. Summary counts are incident instances;
occurrence counts count detections. Historical migration consolidation is visible.

Pulse Platform Health preserves all 12 quality/anomaly cards and adds four cards
in a compact two-by-two section: Active alerts, Alerts by lifecycle status,
Recurring alerts, and Recently resolved alerts. Tables display the latest 100
matches. Provision through `docker compose run --rm --no-deps metabase-setup`.
Layer, Dataset, and Severity map to supported cards. The separate Lifecycle status
filter maps only to alert cards; Run status keeps its quality meaning. Dates never
filter the active queue. The new status/recurrence cards filter inclusive UTC
last-seen dates; recently resolved filters resolution date. Existing historical
alert cards keep their creation-date semantics. Status/severity charts count
instances after filtering; selecting an incompatible lifecycle for a fixed-state
card correctly returns an empty result. No date selection means all history.

### Validation and limitations

```powershell
python -m unittest tests.test_alert_lifecycle tests.test_quality_persistence tests.test_anomaly -v
$env:RUN_MONITORING_INTEGRATION_TESTS='1'
python -m unittest tests.test_alert_lifecycle tests.test_anomaly_postgres tests.test_monitoring_postgres tests.test_monitoring_presentation -v
python -m unittest discover -s tests -v
```

Live SQL tests create/drop disposable databases and cover migration, creation,
recurrence, concurrency, retries before/after resolution, ownership, terminal
transitions, audit rollback, quality/anomaly integration, read-only/empty views,
and dashboard filter semantics. CI discovers the deterministic service/CLI and
query contracts; PostgreSQL and real Airflow execution remain explicit local
integration checks. See [Phase 5.6 validation](bi/ALERT_VERIFICATION.md).

Limitations: manual operator verification, no authenticated ownership, no receipt
or audit retention policy, no automatic recovery, and no guarantee against direct
SQL bypass. Incident identity does not separate tenants/environments that share
the same database/dataset names; use separate configured databases. A new CLI
execution ID counts as a new occurrence even if it inspects the same observation.
Receipt snapshots are first-received evidence, while anomaly source rows retain
the established replace-on-retry semantics.

Phase 5.7 below defines retention and local delivery/basic-escalation policy.
Recovery/freshness rules, Slack, email, PagerDuty, external providers, production
paging, RBAC/SSO, Grafana/Prometheus, automated remediation, and complex case
management remain intentionally unimplemented. Nothing in Phase 5.6 itself sends
external notifications.

## Phase 5.7: Alert delivery, escalation, and retention

Phase 5.7 adds an operational delivery layer around the Phase 5.6 lifecycle. It
does not change quality checks, anomaly classification, incident identity,
manual acknowledgement/resolution, or analytics-pipeline failure behavior.

### Delivery architecture and provider

`src/quality/notifications.py` defines the provider-neutral contract
`send(alert, context) -> DeliveryResult`. `src/quality/delivery.py` discovers due
work, claims an attempt in PostgreSQL, calls the provider outside the database
transaction, and finalizes the attempt. Lifecycle producers do not import or
call a concrete provider.

The only Phase 5.7 provider is `log`, and it is the default. It emits identifiers,
routing fields, severity, dataset, and layer to the `pulse.alert_delivery` Python
logger; it performs no network I/O and deliberately omits the alert message and
details. No webhook, SMTP server, credentials, or CI secrets are configured.
`ALERT_DELIVERY_PROVIDER` therefore accepts only `log` in this phase.

Airflow runs `python -m src.quality.delivery_cli sweep --limit 100` in the
independent `pulse_alert_operations` DAG every five minutes. The analytics DAG
has no dependency on this DAG. Provider exceptions become FAILED rows and the
sweep still exits successfully, so provider availability cannot fail or block
Bronze, Silver, Gold, warehouse, anomaly, dbt, or Metabase processing. Storage or
configuration errors fail only the operations task and are sanitized.

`monitoring.alert_deliveries` stores one row per physical attempt:
delivery/alert UUIDs, logical key, provider and non-secret destination key,
delivery kind/version, escalation level, status, UTC attempt/completion times,
attempt number, optional external reference/error, and JSONB details.

| Delivery status | Meaning |
| --- | --- |
| `PENDING` | The attempt is durably claimed and provider execution has not been finalized |
| `SENT` | The provider reported success |
| `FAILED` | The provider raised or reported failure; the attempt remains visible and retryable within policy |
| `SKIPPED` | The provider deliberately performed no delivery; this is terminal for the logical delivery |

### Triggers, idempotency, retry, and escalation

An OPEN WARNING or CRITICAL incident is eligible for one INITIAL delivery.
RESOLVED incidents never deliver. ACKNOWLEDGED incidents do not receive an
initial notification or escalation if they are acknowledged before the sweep.
NORMAL and INSUFFICIENT_HISTORY anomaly observations do not create alert events,
so they are ineligible. Fixed quality policy remains unchanged: only
FAIL+CRITICAL quality results alert; WARNING delivery is exercised by anomaly
alerts and synthetic fixtures.

The exact logical key is `v1:` plus SHA-256 of compact UTF-8 JSON containing:

```text
["pulse-delivery-v1", alert_event_id, provider, destination_key, delivery_kind, delivery_version]
```

INITIAL uses version 1, ESCALATION uses level/version 1, and RECURRENCE uses the
configured occurrence threshold as its version. Attempt UUID is UUIDv5 of the
logical key plus attempt number. PostgreSQL uniquely constrains
`(logical_delivery_key, attempt_number)` and an advisory lock serializes claims.
Repeated or concurrent sweeps cannot add a second successful logical delivery.
This is database idempotency, not a claim of exactly-once behavior in an external
system that has accepted work immediately before a process crash.

FAILED attempts retry after `ALERT_DELIVERY_RETRY_MINUTES` (default 5), up to
`ALERT_DELIVERY_MAX_ATTEMPTS` total attempts (default 3). SENT, SKIPPED, and
PENDING attempts are not automatically duplicated. An explicit CLI retry
bypasses the delay but not the attempt bound, terminal-state rule, or current
lifecycle eligibility.

WARNING has initial delivery only. CRITICAL receives exactly one level-1
escalation when its initial delivery is SENT, its first-seen age reaches
`ALERT_CRITICAL_ESCALATION_MINUTES` (default 60), and it remains OPEN. An
ACKNOWLEDGED or RESOLVED alert never escalates. Escalation state is derived from
delivery rows; `alert_events` has no duplicate escalation columns.

Recurrence delivery is disabled by default with `ALERT_RECURRENCE_THRESHOLD=0`.
Setting it to an integer of at least 2 permits one RECURRENCE delivery once the
incident reaches that occurrence count. This is the only delivery allowed for an
ACKNOWLEDGED incident. It does not page repeatedly at every multiple; changing
the configured threshold creates a distinct versioned recurrence notification.

```powershell
python -m src.quality.delivery_cli sweep
python -m src.quality.delivery_cli pending
python -m src.quality.delivery_cli retry <delivery_id>
```

Commands output JSON. `pending` lists PENDING and FAILED attempts oldest first.
Provider failures are reflected in the sweep summary with exit code 0. Invalid
configuration, database errors, unknown IDs, ineligible alerts, non-latest
attempts, and exhausted retries exit 1; argument errors exit 2.

### Retention policy and CLI

Retention is manual and defaults to `MONITORING_RETENTION_DAYS=90` (valid range
1 to 3650). Nothing is deleted by Airflow or initialization. Eligibility uses the
following UTC timestamps:

| History | Eligibility |
| --- | --- |
| Quality runs/results | Run completion is older than cutoff and no retained alert references a result in the run |
| Anomaly results | Evaluation is older than cutoff and no retained alert references the result |
| Alert events/history/occurrences/deliveries | The parent alert is RESOLVED and its resolution is older than cutoff |

Every OPEN or ACKNOWLEDGED alert is preserved, together with its occurrences,
audit, deliveries, and referenced quality/anomaly source evidence. Recent
resolved alerts and their source evidence are also retained. Apply locks eligible
alert parents, reports the pre-delete counts, deletes child rows before parents,
and commits all relations together. A failure rolls back the whole operation;
repeated preview/apply calls are idempotent.

```powershell
python -m src.quality.retention_cli preview
python -m src.quality.retention_cli preview --days 120
python -m src.quality.retention_cli apply --confirm
python -m src.quality.retention_cli apply --days 120 --confirm
```

`apply` without `--confirm` exits 1 and deletes nothing. Preview is read-only and
returns per-relation and total row counts. The read-only
`monitoring_views.retention_eligible_counts` view uses 90 days by default; SQL
clients may set the session key `pulse.monitoring_retention_days` before querying
it. The CLI computes its report directly from its configured cutoff.

### Delivery views and dashboard

The presentation schema adds `recent_deliveries`, `failed_deliveries`,
`delivery_summary_by_provider`, `escalation_summary`, and
`retention_eligible_counts`. All are ordinary non-updatable views and none mutate
operational history.

Pulse Platform Health retains its existing 16 cards and adds one compact
two-by-two operations section: Recent alert deliveries, Failed alert deliveries,
Delivery status by provider, and Escalated active alerts. Layer, Dataset,
Severity, Lifecycle status, Provider, Delivery status, and inclusive UTC date
filters map only to queries containing compatible fields. Run status remains a
quality-only filter. Re-running Metabase setup updates the managed questions and
preserves their IDs, unrelated cards, and custom dashboard parameters.

Configuration defaults are documented in `.env.example` and passed only to the
Airflow services by Compose. Destination keys are routing aliases, not endpoint
URLs or credentials. No secret is printed or stored by the log provider.

Phase 5.7 intentionally has no external provider, PagerDuty/Opsgenie/SMS,
on-call rotations, distributed queue, automatic remediation, RBAC/SSO, secret
manager, or incident case system. A crash after a PENDING claim requires operator
inspection. Stale-claim recovery, an optional real webhook with downstream
idempotency support, richer routing, and multi-level escalation remain deferred
to a future alert-operations phase.

## Phase 5.8: Contextual anomaly baselines

Phase 5.8 improves expected-value modeling without changing anomaly status,
severity, alert identity, lifecycle, delivery, escalation, or pipeline blocking.
`src/quality/baselines.py` defines the provider-neutral `BaselineStrategy`
contract. `evaluate` walks the metric's ordered strategy policy and selects the
first strategy with enough prior-only history. Every contextual result records
the requested and selected strategy, attempted fallback path, expected value,
WARNING bounds, current residual, trend slope, seasonal reference count,
training-window size, training error, fallback flag, and operational confidence.

| Strategy | Deterministic expected value |
| --- | --- |
| `robust_history` | Median of the bounded history window; the Phase 5.5 MAD / modified-z, percentage, and zero-baseline absolute fallbacks remain unchanged |
| `day_of_week` | Median of prior observations matching the current UTC weekday |
| `trend` | Robust Theil-Sen-style median pairwise slope plus a median intercept, forecast to the current timestamp |
| `seasonal_trend` | Robust trend plus the median detrended level from matching prior UTC weekdays |

Trend slope is value units per day when source timestamps are available. All
strategies use at most the latest 56 observations by default. The weekday model
needs four matching weekdays; trend needs at least the configured general
minimum (seven by default); combined seasonal/trend needs at least 28 total
observations and four matching weekdays. Missing requirements cause an explicit
fallback, never interpolation or silent model substitution. If no configured
strategy qualifies, the result is `INSUFFICIENT_HISTORY`.

Metric policies are explicit in `src/quality/anomaly_runner.py`:

- `gross_revenue`: `seasonal_trend -> day_of_week -> trend -> robust_history`;
- `completed_order_volume`: `day_of_week -> trend -> robust_history`;
- `row_count`, `warning_check_count`, and `failed_check_count`: robust history;
- funnel conversion rates: robust history plus a current denominator of at least
  100 (`product_views`, `cart_adds`, `checkouts_started`, or `orders_created`, as
  appropriate). Historical rate points below that denominator are excluded from
  training; a missing or smaller current denominator yields `INSUFFICIENT_HISTORY`.

These choices reflect the present sources: daily commerce totals have credible
weekly/trend context, while quality executions are irregular and country rate
series are comparatively sparse. They can be revised as explicit policy rather
than by modifying a baseline implementation.

### Decisions, guardrails, and confidence

An observation at or beyond its inclusive WARNING bound is anomalous. Bounds use
robust residual MAD when it is nonzero, then preserve the established percentage
or absolute fallback for flat histories. The order-volume minimum absolute
deviation is 5 orders, revenue is 100 currency units, and rates are 0.02. Median
levels, median pairwise slopes, a bounded window, minimum history, rate
denominators, and flat-variance fallbacks limit the influence of one outlier and
avoid hypersensitive zero-width forecasts. The existing WARNING/CRITICAL score
thresholds remain available and severity stays nonblocking by default.

`LOW`, `MEDIUM`, and `HIGH` are operational reliability labels, not statistical
coverage claims. Confidence is derived deterministically from training/reference
count and median absolute residual relative to expected magnitude. A selected
fallback cannot be labeled HIGH. Exact inputs and the label remain visible in
the result details.

The PostgreSQL write schema is unchanged. Contextual metadata uses the existing
`monitoring.anomaly_results.details` JSONB column, while `baseline_value` remains
the selected expected value for backward compatibility. New typed, read-only
views are `anomaly_baseline_history`, `anomalies_by_strategy`,
`anomaly_confidence_summary`, and `baseline_fallback_summary`.

### Explain and backtest locally

The explain command reads current warehouse series but does not require Airflow
and does not persist:

```powershell
python -m src.quality.anomaly_cli explain gross_revenue --dimension currency=USD
python -m src.quality.anomaly_cli explain completed_order_volume
```

It prints the selected strategy, history and seasonal counts, expected value,
bounds, confidence, observed value, decision, residual, and explanation.
`src/quality/anomaly_backtest.py` provides `backtest_series`: each timestamp T is
evaluated using only observations strictly before T. Its report contains
evaluation, anomaly, and insufficient-history counts, alert rate, and an optional
false-positive proxy when deterministic truth labels are supplied. Test fixtures
cover stable, growing trend, weekly, weekly-plus-trend, sudden drop, sudden spike,
persistent level shift, and noisy healthy histories, including a direct
no-future-leakage assertion.

Pulse Platform Health retains all 20 Phase 5.7 cards and adds a final two-by-two
model-quality section: Anomalies by baseline strategy, Baseline confidence
distribution, Baseline fallback usage, and Recent contextual anomalies. Baseline
strategy and Confidence filters map only to compatible cards; Layer, Dataset,
Severity, and inclusive UTC dates retain their compatible mappings.

Phase 5.8 deliberately adds no Prophet, ARIMA, neural network, external service,
LLM scoring, automated root-cause analysis/remediation, holiday calendar, feature
store, or future-data training. Persistent level shifts remain visible as repeated
walk-forward anomalies but do not silently rewrite history. Explicit change-point
signals, holiday/business-calendar effects, automatic regime adaptation,
multivariate context, and calibrated statistical intervals are deferred to a later
phase. Forecasts are lightweight operational heuristics, not financial forecasts or
probabilistic guarantees.

## Phase 5.9: Business onboarding and multi-source contracts

Phase 5.9 adds a version-controlled onboarding control plane and business-aware
data grains without connecting any external account. `business_id` is the stable
lowercase identifier; a display name is metadata and is never used as a key.
Source identity is `(business_id, source_id)`, so a source ID may repeat safely
for different businesses. `config/businesses/` contains business JSON and
`config/sources/` contains source JSON. The tracked `pulse_demo_store` is entirely
synthetic and enables Shopify-like orders, Meta-Ads-like daily facts, and
CSV/manual events.

Business config records status, IANA timezone, ISO-style currency/country,
reporting timezone, enabled source IDs, and optional nonsensitive metadata.
Each source declares its type/ID, owner business, enabled state, ingestion mode,
five-field cron schedule, schema version, credential reference, and optional
metadata. A credential reference is an environment-variable *name* only; the
registry never stores or prints a secret value. A separate runtime resolver reads
only the named variable on explicit request and raises a sanitized error when it
is absent. `.env.example` contains placeholders, not credentials.

`src/onboarding/` separates registry loading, structured validation, source
contracts, ingestion envelopes, local adapters, and CLI commands. Validation
reports stable issue codes and paths for missing businesses/sources, duplicate
per-business source IDs, missing credential references, unsupported types or
versions, and malformed schedules, identifiers, timezones, currencies, or
countries. Conceptual source types are `shopify`, `meta_ads`, `tiktok_ads`,
`google_ads`, and `csv_manual`; only the three sample types have v1 contracts and
local adapters. Unsupported versions fail closed. New or incompatible source
fields require a new explicit contract version rather than silent coercion.

The base ingestion envelope carries `business_id`, `source_type`, `source_id`,
`ingestion_id`, `record_id`, extraction/source-update UTC timestamps,
`schema_version`, and payload. Contracts define required/optional fields, types,
source timestamp, currency fields, timezone expectation, and unique grain for
`shopify_orders_v1`, `meta_ads_daily_v1`, and `csv_business_events_v1`.
`SourceAdapter` defines `validate_config`, `extract`, `normalize`, and
`healthcheck`. Mock adapters use UUIDv5 identities, fixed timestamps, overlapping
sample identifiers, and no network calls.

Use the local CLI from the repository root:

```powershell
python -m src.onboarding.cli list
python -m src.onboarding.cli show pulse_demo_store
python -m src.onboarding.cli validate pulse_demo_store
python -m src.onboarding.cli validate-all
python -m src.onboarding.cli source-check pulse_demo_store demo_shopify
python -m src.onboarding.cli demo pulse_demo_store
```

`show` displays only config and reference names. `source-check` validates and
extracts fixed local fixtures. `demo` exercises registry validation, three
business-aware envelopes (Bronze contract), normalization (Silver contract),
business-scoped Gold metrics, a local in-memory serving/query boundary, identity
quality validation, and a nonpersisted anomaly evaluation. Its short history
intentionally returns `INSUFFICIENT_HISTORY`; live PostgreSQL loading remains in
the normal analytics DAG.

Marketplace producer records include the four source identity fields and use
`business_id|customer_id` as the Kafka key. Bronze preserves them and rejects
partial identity, invalid IDs, unsupported marketplace versions, or a mismatched
key. Silver preserves identity and deduplicates on
`(business_id, source_type, source_id, event_id)`. Gold grains are:

| Output | Grain |
| --- | --- |
| `daily_sales` | business, UTC event date, country, currency |
| `customer_metrics` | business, customer |
| `product_metrics` | business, product |
| `funnel_metrics` | business, UTC event date, country |

PostgreSQL adds non-null `business_id` to every analytics output and indexes it.
dbt source/mart tests use composite uniqueness, revenue groups by business and
currency, and customer/product rankings partition by business. Marketplace BI
queries retain a business column and one Business filter replaces per-business
dashboard copies.

Anomaly source queries group analytics history by business and persist
`business_id`; source type/ID remain available on Silver records where attribution
is exact. Alert and delivery context remains in existing JSONB. Monitoring views
project business identity as a typed read-only column, and Pulse Platform Health
maps its Business filter only to compatible anomaly/alert/delivery cards.
Dataset-level quality runs still describe the whole pipeline snapshot and are not
falsely attributed to one business; per-business quality execution is deferred.

Backward compatibility is explicit. A record with none of the four identity
fields maps to `pulse_demo_store` / `csv_manual` /
`pulse_marketplace_demo` / `marketplace_events_v1`. A partially populated identity
is rejected. New producer records are explicit, and legacy Gold input receives
only that same demo business. Unknown history cannot silently mix with a newly
onboarded business.

The manual `pulse_business_onboarding` DAG discovers active/enabled configs at
import and creates one stable local check task per source after registry
validation. It does not hardcode a DAG per business or call external systems.
Source schedules are validated future-connector metadata, not separate production
schedules in this phase.

This is contract-level tenancy, not production multi-tenancy. Phase 5.9 adds no
real Shopify/Meta/TikTok API, OAuth, production secret manager, RLS/RBAC/SSO,
customer UI, billing, connector cursor/backfill state, distributed ingestion
queue, or tenant deployment isolation. Those connector, credential,
access-control, and operational concerns are intentionally deferred to Phase 6.0.

## Phase 6.0: Real Shopify orders

Phase 6.0 adds the first read-only external connector while retaining every
Phase 5.9 mock. A Shopify source with `metadata.adapter: "admin_api"` selects
`ShopifyAdminApiAdapter`; a synthetic Shopify source still selects
`MockShopifyAdapter`. No Meta Ads, TikTok Ads, or Google Ads network connector is
included.

The adapter uses the Shopify Admin **GraphQL** API over HTTPS. The default and
tracked template version is `2026-07`, the current stable version when Phase 6.0
was prepared. It is explicit and configurable as `metadata.api_version` so a
future quarterly Shopify version can be tested and adopted without changing
generic platform code. Do not use `unstable` in production. Review Shopify's
[API versioning policy](https://shopify.dev/docs/api/usage/versioning) before
upgrading. Authentication uses the `X-Shopify-Access-Token` header documented by
[Shopify](https://shopify.dev/docs/api/usage/authentication). The connector never
scrapes storefront or admin HTML.

The runtime flow is:

```text
business/source registry
  -> Shopify Admin GraphQL (updated_at filter + cursor pagination)
  -> Phase 5.9 IngestionEnvelope
  -> privacy-minimized Bronze Parquet
  -> latest-version Silver snapshot
  -> supported Gold commerce metrics
  -> transactional PostgreSQL load -> dbt -> existing Metabase Business filter
```

Bronze retains `business_id`, `source_type`, `source_id`, `ingestion_id`, stable
order-version `record_id`, `extracted_at_utc`, `source_updated_at_utc`, schema
version, and the selected Shopify payload. Silver preserves those lineage fields,
normalizes types, rejects invalid identity/currency/quantity/money values, and
uses the newest `source_updated_at_utc` for each stable projected event. Repeated
delivery of the same business/source/order/update is idempotent. Order edits emit
tombstones for projections that disappeared, and cancellation makes all current
order projections inactive. The finite Silver snapshot used by the analytics DAG
therefore replaces analytical state instead of treating orders as immutable.

### Authentication and registry template

Copy and edit, but do not rename into the registry until all placeholders have
been replaced:

- `config/templates/business.shopify.json.example` ->
  `config/businesses/<business_id>.json`
- `config/templates/source.shopify.json.example` ->
  `config/sources/<business_id>.shopify.json`

The template captures business ID/display name, operational and reporting
timezones, currency, country, source ID, environment-variable references, API
version, page size, timeout, and bounded retry policy. Actual business names do
not appear in generic Python code.

Only environment-variable **names** are tracked. Put values in local `.env`:

```dotenv
SHOPIFY_FIRST_STORE_SHOP_DOMAIN=your-store.myshopify.com
SHOPIFY_FIRST_STORE_ACCESS_TOKEN=<local value only>
SHOPIFY_FIRST_STORE_BACKFILL_START_DATE=2026-01-01T00:00:00Z
```

The domain must be a `myshopify.com` hostname with no scheme or path. The token
must grant `read_orders`; orders older than Shopify's normal order window can
also require approved `read_all_orders` access. Use a backend/offline token
suitable for scheduled work and grant no write scope merely for Pulse. `.env` is
ignored, Compose passes only the documented Shopify values to Airflow, and token
values are never placed in registry JSON, output, errors, samples, or logs.

### Data contract and privacy

Pulse requests order ID/name, created/updated/processed/cancelled timestamps,
financial and fulfillment statuses, shop currency, original totals/subtotal,
discounts/tax/shipping/refunds, customer ID when present, shipping country code,
and line-item ID/product ID/variant ID/SKU/quantity/unit price/discount. Refunds
retain refund ID/time/amount and product/variant/quantity allocation when Shopify
provides it. Money stays in source currency; Phase 6.0 performs no FX conversion.

The query and a second Bronze allowlist intentionally exclude customer name,
email, phone, billing address, street address, postal code, city, province,
geolocation, IP address, marketing consent, order notes, staff identity, payment
details, and free-form refund notes. A shipping **country code** is the only
address-derived field. Guest orders can have no customer ID; they remain in daily
and product sales but are omitted from customer metrics rather than receiving a
fabricated identity. SKU is product operational data, not customer data.

Shopify orders support `daily_sales`, `customer_metrics` when a customer ID is
available, and `product_metrics`. They never produce product-view, cart, or
checkout facts, and all Shopify rows are excluded from `funnel_metrics`.

Revenue semantics are conservative and explicit. Cancelled or void/unpaid orders
do not contribute. For a paid, partially paid, partially refunded, or refunded
order, recognized revenue is `max(original order total - total refunded, 0)` and
is allocated proportionally across current line items. A full refund therefore
contributes zero revenue and a partial refund contributes only the retained
amount. Refund events remain available for refund counts, but are not subtracted
again. Completed-order/refund counts remain lifecycle counts, and `units_sold`
remains original paid quantity rather than a net-returned-units measure. The
existing `gross_revenue` output column contains this recognized amount
for Shopify; legacy marketplace events retain their historical payment-event
semantics. Shipping, tax, and order-level refunds are proportionally allocated,
so Phase 6.0 is not an accounting ledger.

Operational timestamps stay UTC. Shopify `event_date` is derived from the
business `reporting_timezone`, so a late-evening UTC order lands on the correct
business day. Legacy marketplace event dates keep their existing UTC semantics.

### Pagination, retry, checkpoints, and state

The order connection follows `pageInfo.hasNextPage/endCursor` until exhausted and
uses `sortKey: UPDATED_AT` with an inclusive `updated_at:>=<checkpoint>` filter.
The inclusive boundary intentionally permits harmless redelivery for timestamp
ties. A page size of 50 is the documented default and is configurable up to 100.
Order, order-line, and refund-line connections all follow their cursors to
completion; no connection silently stops at its first page.

HTTP 429, GraphQL `THROTTLED`, timeouts, DNS/network failures, and Shopify 5xx
responses use exponential backoff, honor `Retry-After`, and stop after the
configured retry bound (default three retries). Authentication, permission, 404,
malformed JSON/data, and other application errors fail immediately with sanitized
operator messages.

`data/state/connectors.sqlite3` stores one row per `(business_id, source_id)`:
last attempt, last success, UTC checkpoint, extracted count, latest sanitized
error, and health. It also remembers projected event IDs needed to tombstone
removed order lines. It is local generated state and is ignored by Git. The
checkpoint is advanced only after all records normalize, pass critical quality
checks, and Bronze persistence succeeds. A failed extraction/write leaves the old
checkpoint and previous data intact. Each business/source has independent state.

The first run has no checkpoint and therefore requires the explicit environment
backfill start. Later runs use the persisted watermark. `--limit` is accepted only
for dry runs; limiting a persisted timestamp-only extraction could strand records
that share the boundary timestamp.

### Connect the first store safely

1. In Shopify's Dev Dashboard/admin, create or install a read-only app for the
   store and obtain an offline/background-capable Admin API token with
   `read_orders` (and approved `read_all_orders` only if the chosen history needs
   it). Do not grant write scopes to Pulse.
2. Copy the exact `*.myshopify.com` hostname; do not use a storefront vanity URL.
3. Put the domain, token, and an intentionally bounded ISO-8601 UTC backfill start
   in local `.env` using the three placeholder names above.
4. Copy the two templates, replace the business/source placeholders, and verify
   timezone, reporting timezone, currency, country, source ID, credential
   references, API version, and schedule.
5. Run `python -m src.onboarding.cli validate <business_id>`.
6. Run `python -m src.onboarding.cli source-check <business_id> <source_id>` and
   confirm configuration, credential, authentication, reachability, permission,
   and rate-limit fields without revealing a token.
7. Run `python -m src.onboarding.cli extract <business_id> <source_id> --limit 5 --dry-run`.
   This authenticates, fetches, normalizes, and quality-checks at most five orders
   without writing Bronze or checkpoint state.
8. Inspect the minimal sample for IDs, UTC timestamps, amounts, statuses,
   currencies, line items, refunds, and absence of PII.
9. Trigger the manual `pulse_business_onboarding` DAG, or run
   `python -m src.onboarding.cli extract <business_id> <source_id>`, for the
   controlled initial backfill. This is the first persistent operation.
10. Run/trigger `pulse_analytics_pipeline`; validate Bronze, Silver, Gold, and
    quality outputs before relying on metrics.
11. Inspect `analytics.daily_sales`, `analytics.customer_metrics`, and
    `analytics.product_metrics` filtered by `business_id`, then run dbt tests.
12. Open the existing Metabase dashboard and select the new value in its Business
    filter. No dashboard copy is required.

Use `python -m src.onboarding.cli source-status <business_id> <source_id>` to see
operational state. A failing Shopify task is a connector failure, not a business
anomaly; no new metric history is fabricated, and short real history continues to
produce `INSUFFICIENT_HISTORY` in the unchanged Phase 5.8 anomaly engine.

### Offline and opt-in tests

CI is fully offline and does not define Shopify credentials. Deterministic tests
cover multiple pages, throttling, timeout, invalid authentication, malformed
orders/responses, cancellation, partial/full refunds, duplicate versions, edits,
currencies, privacy, idempotency, and checkpoint rollback. The real read-only
smoke test skips unless all of these are explicitly set:

```powershell
$env:RUN_SHOPIFY_INTEGRATION_TESTS = "1"
$env:SHOPIFY_INTEGRATION_SHOP_DOMAIN = "your-store.myshopify.com"
$env:SHOPIFY_INTEGRATION_ACCESS_TOKEN = "<local value>"
$env:SHOPIFY_INTEGRATION_BACKFILL_START_DATE = "2026-01-01T00:00:00Z"
python -m unittest tests.test_shopify_connector.RealShopifyReadOnlyTests -v
```

The smoke test performs only health and a maximum two-order query. It does not
mutate Shopify or persist Pulse data. Common failures map to explicit states:
`configuration_invalid`, `credentials_missing`, `authentication_failed`,
`permission_failure`, `unreachable`, `rate_limited`, `not_found`, `server_error`,
or `unhealthy`. Check the shop hostname, token lifetime/scopes, API version, and
network access in that order.

Phase 6.0 deliberately omits OAuth UI, token refresh/rotation automation, secret
manager infrastructure, webhooks, bulk operations, FX conversion,
accounting-grade returns allocation,
customer PII, and all other real advertising/source connectors. These are Phase
6.1-or-later concerns.

## Phase 6.1: Performance Marketing data layer

Phase 6.1 adds a provider-agnostic advertising domain without connecting to an
advertising account. The implementation reuses the Phase 5.9 business/source
registry, `SourceConfig`, `IngestionEnvelope`, `SourceAdapter`, quality engine,
contextual anomaly evaluator, Airflow deployment, analytics warehouse, dbt
project, and Metabase instance. It does not introduce a second connector control
plane. All fixtures in `data/fixtures/marketing/` are small, deterministic,
synthetic files and CI remains fully offline.

The flow is:

```text
business/source registry
  -> Meta / TikTok / Google / generic offline MarketingSourceAdapter
  -> data/bronze/marketing_daily (full native payload + Phase 5.9 envelope)
  -> data/silver/marketing_daily (canonical typed latest daily revision)
  -> four Gold marketing snapshots
  -> analytics PostgreSQL tables -> dbt presentation views
  -> Pulse Marketing Performance dashboard
  -> existing quality and contextual anomaly/alert framework
```

`MarketingSourceAdapter` subclasses the existing local adapter implementation.
It owns the shared envelope, deterministic logical identity, configuration,
health, lookback, fixture extraction, and canonical validation behavior. Provider
classes only map native names and units. `adapter_for` remains the single adapter
factory used by the registry CLI and source-discovery DAG. Phase 6.1 adapters
accept only `metadata.adapter: "mock"` and make no network request.

### Contracts, terminology, and canonical fields

The strict v1 contracts are `meta_ads_daily_v1`, `tiktok_ads_daily_v1`,
`google_ads_daily_v1`, and `generic_ads_daily_v1`. Each declares required and
optional types, native ad-daily grain, report-date field, source update timestamp,
reporting-timezone field, currency field, and additive/non-additive or
platform-attributed metric semantics. Unknown fields fail closed; intentional
provider extensions belong in the optional `details` object or a new contract
version.

| Provider term | Canonical term |
| --- | --- |
| Meta campaign / ad set / ad | campaign / ad group / ad |
| TikTok campaign / ad group / ad | campaign / ad group / ad |
| Google campaign / ad group / ad | campaign / ad group / ad |
| Generic campaign / ad group / ad | campaign / ad group / ad |

Silver records contain business, source, ingestion and schema identity;
`platform`; account, campaign, ad-group, ad and optional creative IDs/names;
`report_date`, `reporting_timezone`, and `currency`; spend, impressions, optional
reach/frequency, clicks/link clicks, explicitly named `platform_conversions` and
`platform_conversion_value`, optional video/landing-page views; and serialized
provider details. Providers are not forced to fabricate unsupported metrics.

The supported canonical grains are:

| Grain | Required logical key |
| --- | --- |
| Campaign daily | business, source identity, platform, account, campaign, report date |
| Ad-group daily | campaign-daily key + `ad_group_id` |
| Ad daily | ad-group-daily key + `ad_id` |

Reporting timezone, currency, source/schema context, and source identity are also
retained in storage/serving grains. Names and `creative_id` are attributes, not
entity keys. Reused IDs such as campaign `123`, ad group `group_shared`, and ad
`ad_shared` in the two fixture businesses and several platforms therefore never
collide.

### Metrics, attribution, currency, and dates

Spend, impressions, clicks, platform conversions, and platform conversion value
are the primary additive metrics. Link clicks, video views, and landing-page views
are additive only when supplied with compatible provider semantics. Reach and
frequency are non-additive: Phase 6.1 retains provider ad-level values in Silver
but does not sum them into higher grains. Gold calculates ratios from aggregate
numerators and denominators, never by averaging row ratios:

```text
CTR           = clicks / impressions
CPC           = spend / clicks
CPM           = spend / impressions * 1000
CPA           = spend / platform_conversions
platform_roas = platform_conversion_value / spend
```

A zero denominator produces SQL/Python null, never infinity, NaN, or a fabricated
zero. `platform_conversions`, `platform_conversion_value`, and
`platform_roas` are platform-reported attribution outputs. They are not true
orders, delivered orders, actual revenue, margin, profit, or accounting ROAS.
Marketing performance stays separate from commerce economics until a later phase
defines an explicit attribution/join model.

Currency is mandatory and remains in every monetary grain. No FX conversion is
performed, and no model or dashboard query sums different currencies into one
number. `report_date` remains the provider account date in its IANA
`reporting_timezone`; it is not shifted to UTC or silently remapped to a business
date. Extraction and update lineage timestamps remain UTC.

### Late attribution and idempotency

Daily advertising facts are mutable. `record_id` is UUIDv5 over business, source
type/ID, schema version, platform, account, campaign, ad group, ad, and report
date; it deliberately excludes extraction/update timestamps and metric values.
Re-extracting an unchanged row or a revised attribution value therefore addresses
the same logical record. Silver keeps the greatest `source_updated_at_utc` (then
extraction timestamp), so a correction updates one daily grain rather than
duplicating it. The Meta fixture revises the prior day's conversions from 2 to 3
and proves that behavior.

`metadata.lookback_days` defaults to 3. A future real connector will query from
`watermark_report_date - lookback_days` inclusively and advance its report-date
watermark only after the complete Bronze/Silver/quality transaction succeeds.
The inclusive overlap intentionally permits idempotent redelivery and delayed
attribution corrections. Phase 6.1 demonstrates the calculation but does not add
an API checkpoint or claim historical platform data is immutable.

### Gold, warehouse, dbt, quality, and anomaly outputs

Gold and `analytics` contain `marketing_daily`, `campaign_performance`,
`ad_group_performance`, and `ad_performance`. All preserve business, source,
platform, timezone, and currency context. The transactional loader validates
exact schemas, nonnegative metrics, currencies, and composite uniqueness before
replacing existing rows. dbt exposes `marketing_overview`,
`campaign_performance`, `ad_group_performance`, and `ad_performance`; source and
mart tests enforce business/platform/currency completeness, composite grains,
nonnegative raw metrics, and bounded CTR.

Marketing rules run through the existing Spark quality engine and classify
missing identity/platform/currency, invalid currency, negative metrics,
duplicates, clicks above impressions, and invalid CTR with existing
INFO/WARNING/CRITICAL semantics. `analytics.marketing_daily` also feeds daily
spend, impressions, clicks, platform conversions, CTR, and CPA into the existing
contextual baseline engine with business/source/platform/account/currency/timezone
dimensions. Short series continue to return `INSUFFICIENT_HISTORY`.

The separate **Pulse Marketing Performance** Metabase dashboard contains Spend
by platform, Spend trend, Campaign performance, CTR/CPC/CPM,
Platform-reported conversions/CPA, Platform-reported ROAS, Top campaigns, and Top
ads. Its Business, Platform, Campaign, Currency, and Date filters are mapped only
to compatible cards. The existing marketplace and platform-health dashboards are
preserved.

### Offline commands and limitations

```powershell
python -m src.onboarding.cli validate-all
python -m src.onboarding.cli source-check marketing_demo_a meta_primary
python -m src.onboarding.cli extract marketing_demo_a meta_primary --limit 2 --dry-run
python -m src.marketing.pipeline demo
python -m src.marketing.pipeline build
python -m src.quality.runner marketing_silver --block-on-critical
python -m src.quality.runner marketing_gold --block-on-critical
python -m src.warehouse.load_marketing load
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
```

The persistent marketing build is a full deterministic offline snapshot; source-level
CLI extraction is intentionally dry-run only. Real Meta/TikTok/Google APIs,
OAuth and credentials, webhooks, production checkpoints, FX, cross-channel or
commerce attribution, marketing-mix modeling, incrementality, automatic budget
optimization, creative AI, profit recommendations, and COD operations are
deferred beyond Phase 6.1.

## Airflow orchestration

Apache Airflow 2.11.2 runs entirely in Docker; no native Windows Airflow
installation is required. The local topology uses a webserver, scheduler with
`LocalExecutor`, one-shot initialization service, and PostgreSQL metadata
database. Celery and Redis are intentionally omitted for this single-machine
development deployment.

Bronze remains a continuously operating upstream Spark/Kafka service and is
not started or supervised by this DAG. The manually triggered
`pulse_analytics_pipeline` orchestrates only finite downstream work:

```text
check_bronze_available
  -> build_silver
  -> quality_check_silver
  -> build_gold
  -> quality_check_gold
  -> load_gold_to_warehouse
  -> quality_check_warehouse
  -> build_marketing
  -> quality_check_marketing_silver
  -> quality_check_marketing_gold
  -> load_marketing_to_warehouse
  -> quality_check_marketing_warehouse
  -> anomaly_check
  -> run_dbt
  -> test_dbt
```

`build_silver` uses the explicit `--orchestrated-snapshot` mode. It reads the
current Bronze valid dataset as a finite snapshot, reuses the existing Silver
normalization and quality classification, deterministically deduplicates by the
business/source/event grain, and replaces Silver valid/rejected outputs. The existing default
available-now streaming mode and `--continuous` mode remain available for
standalone use. The orchestration snapshot intentionally does not reuse
host-created streaming checkpoints because checkpoint file URIs are not
portable between Windows and the Linux Airflow containers.

Copy the local defaults from `.env.example` into an untracked `.env` if you
want to override them, then initialize and start the services:

```powershell
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Open [http://localhost:8080](http://localhost:8080) and sign in with the local
development defaults `airflow` / `airflow`. Override
`AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, database credentials, and
the webserver secret in `.env` when desired; these defaults are not suitable
for production.

Trigger the workflow in the UI or from the scheduler container:

```powershell
docker compose exec airflow-scheduler airflow dags trigger pulse_analytics_pipeline
```

The Airflow containers mount `airflow/dags`, `src`, `dbt`, `config`, and `data`. Project-relative
host data is exposed as `/opt/pulse/data`; source is exposed read-only at
`/opt/pulse/src`, and the dbt project is exposed read-only at `/opt/pulse/dbt`.
Airflow logs, Airflow metadata, warehouse data, and Metabase metadata use
separate Docker named volumes; generated pipeline data and database dumps remain
covered by `.gitignore`.

This phase has no automatic schedule (`schedule=None`), permits only one active
DAG run, and gives each task one short retry. Silver orchestration is a full
snapshot rather than an incremental partition refresh, so it must not run
concurrently with the standalone Silver writer. Airflow does not yet manage
Bronze availability beyond validating its persisted Parquet input.
