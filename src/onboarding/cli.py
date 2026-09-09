"""Local business registry inspection and validation CLI."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json

from src.onboarding.demo import run_demo
from src.onboarding.registry import BusinessRegistry, RegistryError, validate_business


def main(argv=None):
    parser = argparse.ArgumentParser(description="Inspect and validate Pulse business onboarding")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    for name in ("show", "validate", "demo"):
        command = sub.add_parser(name)
        command.add_argument("business_id")
    sub.add_parser("validate-all")
    source_check = sub.add_parser("source-check")
    source_check.add_argument("business_id")
    source_check.add_argument("source_id")
    args = parser.parse_args(argv)
    registry = BusinessRegistry()
    try:
        if args.command == "list":
            payload = [{"business_id": item.business_id, "business_name": item.business_name,
                        "status": item.status} for item in registry.list_businesses()]
            success = True
        elif args.command == "show":
            payload, success = registry.show(args.business_id), True
        elif args.command == "validate":
            report = validate_business(registry, args.business_id)
            payload, success = report.to_dict(), report.valid
        elif args.command == "validate-all":
            business_ids = {item.business_id for item in registry.list_businesses()}
            business_ids.update(item.business_id for item in registry.list_sources())
            reports = [validate_business(registry, business_id)
                       for business_id in sorted(business_ids)]
            payload, success = [item.to_dict() for item in reports], all(item.valid for item in reports)
        elif args.command == "source-check":
            from src.onboarding.adapters import adapter_for
            matches = [item for item in registry.sources_for(args.business_id)
                       if item.source_id == args.source_id]
            if len(matches) != 1:
                raise RegistryError(
                    f"Source {args.source_id!r} was not found exactly once for {args.business_id!r}"
                )
            adapter = adapter_for(matches[0])
            health = adapter.healthcheck()
            payload = {"business_id": args.business_id, "source_id": args.source_id,
                       "healthy": health.healthy, "message": health.message,
                       "record_count": len(adapter.extract()) if health.healthy else 0}
            success = health.healthy
        else:
            payload, success = asdict(run_demo(args.business_id, registry)), True
    except (RegistryError, ValueError) as error:
        payload, success = {"error": str(error)}, False
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
