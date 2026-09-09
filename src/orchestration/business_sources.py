"""Deterministic discovery of version-controlled, enabled business sources."""

from __future__ import annotations

from dataclasses import dataclass
import re

from src.onboarding.registry import BusinessRegistry, validate_business


ONBOARDING_DAG_ID = "pulse_business_onboarding"


@dataclass(frozen=True, slots=True)
class SourceTaskDescriptor:
    business_id: str
    source_id: str
    source_type: str
    schema_version: str
    schedule: str
    task_id: str
    adapter: str


def _task_segment(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()


def discover_enabled_sources(
    registry: BusinessRegistry | None = None,
) -> tuple[SourceTaskDescriptor, ...]:
    """Return stable task metadata, failing closed on an invalid active business."""

    selected = registry or BusinessRegistry()
    output = []
    for business in selected.list_businesses():
        if business.status != "ACTIVE":
            continue
        report = validate_business(selected, business.business_id)
        if not report.valid:
            codes = ", ".join(issue.code for issue in report.issues)
            raise ValueError(f"Invalid onboarding for {business.business_id}: {codes}")
        enabled = set(business.enabled_sources)
        for source in selected.sources_for(business.business_id):
            if source.enabled and source.source_id in enabled:
                output.append(SourceTaskDescriptor(
                    business_id=business.business_id, source_id=source.source_id,
                    source_type=source.source_type, schema_version=source.schema_version,
                    schedule=source.schedule,
                    task_id=f"ingest_{_task_segment(business.business_id)}_{_task_segment(source.source_id)}",
                    adapter=str(source.metadata.get("adapter", "mock")),
                ))
    return tuple(sorted(output, key=lambda item: (item.business_id, item.source_id)))
