"""Local operations for alert delivery sweeps and bounded retries."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys
from uuid import UUID

from src.quality.delivery import (
    DeliveryError,
    deliver_due,
    list_deliveries,
    retry_delivery,
)
from src.quality.persistence import PersistenceError


def main(argv=None):
    parser = argparse.ArgumentParser(description="Deliver Pulse alerts")
    commands = parser.add_subparsers(dest="command", required=True)
    sweep = commands.add_parser("sweep", help="Attempt due initial, recurrence, and escalation deliveries")
    sweep.add_argument("--limit", type=int, default=100)
    pending = commands.add_parser("pending", help="List pending and failed attempts")
    pending.add_argument("--limit", type=int, default=100)
    retry = commands.add_parser("retry", help="Immediately retry the latest failed attempt")
    retry.add_argument("delivery_id", type=UUID)
    args = parser.parse_args(argv)
    try:
        if args.command == "sweep":
            output = asdict(deliver_due(limit=args.limit))
        elif args.command == "pending":
            output = list_deliveries(limit=args.limit)
        else:
            output = retry_delivery(args.delivery_id)
        print(json.dumps(output, default=str, indent=2))
    except (DeliveryError, PersistenceError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
