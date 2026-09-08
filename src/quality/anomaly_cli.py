"""Airflow-independent inspection of the latest baseline for one metric."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from uuid import NAMESPACE_URL, uuid5

from src.quality.anomaly import evaluate
from src.quality.anomaly_runner import policy_for


def main(argv=None):
    parser = argparse.ArgumentParser(description="Explain the latest contextual baseline for a metric")
    parser.add_argument("command", choices=("explain",))
    parser.add_argument("metric")
    parser.add_argument("--dataset")
    parser.add_argument("--layer")
    parser.add_argument("--dimension", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--minimum-history", type=int, default=7)
    args = parser.parse_args(argv)
    if args.minimum_history < 2:
        parser.error("--minimum-history must be at least two")
    dimensions = {}
    for value in args.dimension:
        if "=" not in value or not all(part.strip() for part in value.split("=", 1)):
            parser.error("--dimension must be KEY=VALUE")
        key, item = value.split("=", 1)
        dimensions[key] = item
    from src.quality.anomaly_sources import load_metric_series
    matches = [item for item in load_metric_series()
               if item.metric_name == args.metric
               and (args.dataset is None or item.dataset_name == args.dataset)
               and (args.layer is None or item.layer == args.layer)
               and all(item.dimensions.get(key) == value for key, value in dimensions.items())]
    if not matches:
        parser.error("No matching metric series found")
    results = []
    for item in matches:
        identity = uuid5(NAMESPACE_URL, "explain|" + item.metric_name + "|" +
                         json.dumps(item.dimensions, sort_keys=True))
        results.append(evaluate(item, policy_for(item.metric_name, args.minimum_history), identity))
    payload = [asdict(result) for result in results]
    print(json.dumps(payload[0] if len(payload) == 1 else payload,
                     default=str, allow_nan=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

