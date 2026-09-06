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
| Windows helper and cleanup-retry unit tests | FAST / CI-SAFE | Mocked OS/retry behavior runs on both systems. Linux helpers leave environment and builder untouched; no Windows native binaries are loaded. |
| `WarehouseIntegrationTests` (two tests) | FULL INTEGRATION | Requires a populated local PostgreSQL warehouse and matching Gold Parquet. Tests refresh the configured warehouse and verify reruns/rollback. |
| Live Kafka ingestion, complete Airflow DAG, dbt execution, Metabase API/dashboard/browser, native Windows Hadoop loading | FULL INTEGRATION | Manual local acceptance checks require running services, populated data, or Windows native files. These are not additional hidden unittest skips. |

CI explicitly sets `RUN_SPARK_TESTS=1` and
`RUN_WAREHOUSE_INTEGRATION_TESTS=0`. Only the two warehouse integration methods
are skipped. Spark tests remain enabled by default locally. The existing
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
  -> validate_silver
  -> build_gold
  -> validate_gold
  -> load_gold_to_warehouse
  -> validate_warehouse
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
