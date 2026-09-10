"""Local business registry inspection and validation CLI."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json

from src.onboarding.demo import run_demo
from src.onboarding.registry import BusinessRegistry, RegistryError, validate_business


def _source(registry, business_id, source_id):
    matches = [item for item in registry.sources_for(business_id)
               if item.source_id == source_id]
    if len(matches) != 1:
        raise RegistryError(
            f"Source {source_id!r} was not found exactly once for {business_id!r}"
        )
    return matches[0]


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
    extract = sub.add_parser("extract")
    extract.add_argument("business_id")
    extract.add_argument("source_id")
    extract.add_argument("--limit", type=int)
    extract.add_argument("--dry-run", action="store_true")
    status = sub.add_parser("source-status")
    status.add_argument("business_id")
    status.add_argument("source_id")
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
            config = _source(registry, args.business_id, args.source_id)
            adapter = adapter_for(config)
            health = adapter.healthcheck()
            payload = {"business_id": args.business_id, "source_id": args.source_id,
                       **asdict(health)}
            if config.metadata.get("adapter") != "admin_api":
                payload["record_count"] = len(adapter.extract()) if health.healthy else 0
            success = health.healthy
        elif args.command == "extract":
            if args.limit is not None and not args.dry_run:
                raise ValueError("--limit is restricted to --dry-run so a timestamp tie cannot strand records")
            config = _source(registry, args.business_id, args.source_id)
            if config.source_type in {"commerce_orders", "confirmation_events", "fulfillment_events",
                                      "delivery_events", "cod_collections", "remittances"}:
                if not args.dry_run:
                    raise ValueError(
                        "Operations source extraction is dry-run only here; use "
                        "python -m src.operations.pipeline build for the deterministic snapshot"
                    )
                from src.operations.pipeline import extract_registered_operations
                extractions = extract_registered_operations(
                    registry, business_id=args.business_id, source_id=args.source_id)
                records = [record.to_dict() for _, batch in extractions for record in batch]
                payload = {"business_id": args.business_id, "source_id": args.source_id,
                           "dry_run": True, "record_count": len(records),
                           "records": records[:args.limit] if args.limit is not None else records}
                success = True
            elif config.source_type in {"meta_ads", "tiktok_ads", "google_ads", "generic_ads"}:
                from src.marketing.pipeline import extract_registered_marketing

                if args.limit is not None and not args.dry_run:
                    raise ValueError("--limit is restricted to --dry-run")
                if args.dry_run:
                    extractions = extract_registered_marketing(
                        registry, business_id=args.business_id, source_id=args.source_id
                    )
                    records = [record.to_dict() for _, batch in extractions for record in batch]
                    payload = {"business_id": args.business_id, "source_id": args.source_id,
                               "dry_run": True, "record_count": len(records),
                               "records": records[:args.limit] if args.limit is not None else records}
                else:
                    raise ValueError(
                        "Marketing source extraction is dry-run only here; use "
                        "python -m src.marketing.pipeline build for the full deterministic snapshot"
                    )
                success = True
            else:
                from src.onboarding.shopify_ingestion import run_shopify_ingestion

                if config.source_type != "shopify" or config.metadata.get("adapter") != "admin_api":
                    raise ValueError("extract supports marketing mocks and Shopify admin_api sources")
                business = registry.get_business(args.business_id)
                report = run_shopify_ingestion(
                    config, business, limit=args.limit, dry_run=args.dry_run
                )
                payload, success = report.to_dict(), True
        elif args.command == "source-status":
            from src.onboarding.connector_state import ConnectorStateStore

            _source(registry, args.business_id, args.source_id)
            payload = ConnectorStateStore().get(
                args.business_id, args.source_id
            ).to_dict()
            success = True
        else:
            source_types = {item.source_type for item in registry.sources_for(args.business_id)}
            if source_types & {"commerce_orders", "confirmation_events", "fulfillment_events",
                               "delivery_events", "cod_collections", "remittances"}:
                from src.operations.pipeline import (build_gold_rows, extract_registered_operations,
                                                     normalize_operations)
                extractions = extract_registered_operations(registry, business_id=args.business_id)
                silver = normalize_operations(extractions, registry)
                gold = build_gold_rows(silver)
                payload = {"business_id": args.business_id,
                           "bronze": sum(len(batch) for _, batch in extractions),
                           **{name: len(rows) for name, rows in silver.items()},
                           **{name: len(rows) for name, rows in gold.items()},
                           "quality_status": "PASS", "network_access": False}
                success = True
            else:
                payload, success = asdict(run_demo(args.business_id, registry)), True
    except (RegistryError, ValueError, RuntimeError) as error:
        payload, success = {"error": str(error)}, False
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
