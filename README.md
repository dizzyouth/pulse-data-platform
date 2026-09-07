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
and profile. The current models/macros can render all four marts and all 36 data
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

Silver checks event-ID uniqueness; non-null event/customer/session identifiers,
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

Alerts have an `OPEN` status; acknowledgement and resolution lifecycle operations
are deferred. A retry replaces the anomaly evaluation and its derived OPEN events
inside one transaction. There is no external delivery. For existing critical quality failures, the
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
spaced observations are compared as ordered values. Phase 5.6 may add controlled
delivery and lifecycle operations after operational policy is defined. Slack,
email, PagerDuty, Grafana, Prometheus, external ML, neural networks, complex
forecasting, OpenLineage, and production paging remain out of scope.

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
  -> anomaly_check
  -> run_dbt
  -> test_dbt
```

`build_silver` uses the explicit `--orchestrated-snapshot` mode. It reads the
current Bronze valid dataset as a finite snapshot, reuses the existing Silver
normalization and quality classification, deterministically deduplicates by
`event_id`, and replaces Silver valid/rejected outputs. The existing default
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

The Airflow containers mount `airflow/dags`, `src`, `dbt`, and `data`. Project-relative
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
