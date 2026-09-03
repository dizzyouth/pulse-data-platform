"""Project-local Windows environment setup for PySpark.

The variables configured here affect only the current Python process and its
children. They are intentionally not persisted as Windows user or system
environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import MutableMapping, TypeVar

SparkBuilder = TypeVar("SparkBuilder")


def configure_windows_spark_environment(
    *,
    project_root: Path | None = None,
    environ: MutableMapping[str, str] | None = None,
) -> None:
    """Configure project-local Hadoop and temporary paths on Windows."""

    if os.name != "nt":
        return

    root = project_root or Path(__file__).resolve().parents[2]
    hadoop_home = root / "tmp" / "hadoop"
    hadoop_bin = hadoop_home / "bin"
    spark_temp = root / "tmp" / "spark"
    spark_temp.mkdir(parents=True, exist_ok=True)

    environment = os.environ if environ is None else environ
    environment.setdefault("HADOOP_HOME", str(hadoop_home))
    environment.setdefault("TEMP", str(spark_temp))
    environment.setdefault("TMP", str(spark_temp))
    path_entries = environment.get("PATH", "").split(os.pathsep)
    if str(hadoop_bin).casefold() not in {entry.casefold() for entry in path_entries}:
        environment["PATH"] = os.pathsep.join((str(hadoop_bin), *path_entries))


def configure_windows_spark_builder(
    builder: SparkBuilder, *, project_root: Path | None = None
) -> SparkBuilder:
    """Keep Spark's Ivy dependency cache inside the ignored project temp tree."""

    if os.name != "nt":
        return builder

    root = project_root or Path(__file__).resolve().parents[2]
    ivy_dir = root / "tmp" / "spark" / "ivy"
    local_dir = root / "tmp" / "spark" / "local"
    ivy_dir.mkdir(parents=True, exist_ok=True)
    local_dir.mkdir(parents=True, exist_ok=True)
    return builder.config("spark.jars.ivy", str(ivy_dir)).config(
        "spark.local.dir", str(local_dir)
    )
