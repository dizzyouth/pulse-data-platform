# Pulse Data Platform

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
