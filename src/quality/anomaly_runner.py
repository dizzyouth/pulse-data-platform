"""Shared local/Airflow anomaly runner; heuristic anomalies are nonblocking by default."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json

from src.quality.anomaly import AnomalyPolicy, AnomalyStatus, evaluate
from src.quality.execution import execution_context
from src.quality.models import Severity
from src.quality.observability import emit_event


def policy_for(metric_name: str, minimum_history: int) -> AnomalyPolicy:
    if metric_name in ("warning_check_count", "failed_check_count"):
        return AnomalyPolicy(minimum_history=minimum_history, warning_absolute=1, critical_absolute=3)
    if metric_name.endswith("_rate"):
        return AnomalyPolicy(minimum_history=minimum_history, warning_ratio=.25, critical_ratio=.5,
                             warning_absolute=.1, critical_absolute=.25)
    return AnomalyPolicy(minimum_history=minimum_history, warning_ratio=.5, critical_ratio=.9)


def evaluate_all(series, context, minimum_history: int):
    results = []
    for item in series:
        dimensions = json.dumps(item.dimensions, sort_keys=True, separators=(",", ":"))
        identity = context.logical_id("pulse-anomaly-result-v1", item.dataset_name, item.layer,
                                      item.metric_name, dimensions)
        results.append(evaluate(item, policy_for(item.metric_name, minimum_history), identity))
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description="Evaluate explainable Pulse anomaly policies")
    parser.add_argument("--minimum-history", type=int, default=7)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--execution-id")
    parser.add_argument("--attempt-number", type=int)
    parser.add_argument("--block-on-critical", action="store_true",
                        help="Opt-in only: block when a heuristic CRITICAL anomaly is found")
    parser.add_argument("--log-format", choices=("json", "jsonl"), default="jsonl")
    args = parser.parse_args(argv)
    if args.minimum_history < 2:
        parser.error("--minimum-history must be at least two")
    if not args.persist and (args.execution_id is not None or args.attempt_number is not None):
        parser.error("Identity options require --persist")
    context = execution_context(execution_id=args.execution_id, attempt_number=args.attempt_number)
    from src.quality.anomaly_sources import load_metric_series
    results = evaluate_all(load_metric_series(), context, args.minimum_history)
    if args.persist:
        from src.quality.anomaly_persistence import persist_anomalies
        evaluation_id = persist_anomalies(results, context)
        if args.log_format == "jsonl":
            emit_event("anomaly_persisted", evaluation_id=str(evaluation_id), result_count=len(results),
                       alert_count=sum(r.status == AnomalyStatus.ANOMALY for r in results),
                       attempt_number=context.attempt_number)
    counts = {status.value: sum(result.status == status for result in results) for status in AnomalyStatus}
    if args.log_format == "jsonl":
        for result in results:
            emit_event("anomaly_result", **asdict(result))
        emit_event("anomaly_summary", evaluated_at_utc=datetime.now(timezone.utc), counts=counts,
                   critical_anomalies=sum(r.status == AnomalyStatus.ANOMALY and r.severity == Severity.CRITICAL
                                          for r in results), block_policy=args.block_on_critical)
    else:
        print(json.dumps({"summary": counts, "results": [asdict(r) for r in results]},
                         default=lambda value: str(value), allow_nan=False, indent=2))
    critical = any(r.status == AnomalyStatus.ANOMALY and r.severity == Severity.CRITICAL for r in results)
    return int(args.block_on_critical and critical)


if __name__ == "__main__":
    raise SystemExit(main())
