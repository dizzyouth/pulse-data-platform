"""Local lifecycle operations using the existing warehouse environment."""

import argparse
import json
import sys
from uuid import UUID

from src.quality.alert_service import STATES, acknowledge_alert, list_alerts, resolve_alert
from src.quality.persistence import PersistenceError


def main(argv=None):
    parser = argparse.ArgumentParser(description="Manage Pulse internal alerts")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="List active alerts by default")
    listing.add_argument("--status", choices=(*STATES, "ALL"))
    listing.add_argument("--dataset")
    listing.add_argument("--layer")
    listing.add_argument("--severity", choices=("WARNING", "CRITICAL"))
    listing.add_argument("--limit", type=int, default=100)
    for action in ("acknowledge", "resolve"):
        command = commands.add_parser(action)
        command.add_argument("alert_id", type=UUID)
        command.add_argument("--by", required=True)
        if action == "resolve":
            command.add_argument("--note")
    args = parser.parse_args(argv)
    try:
        if args.command == "list":
            output = list_alerts(status=args.status, dataset=args.dataset, layer=args.layer,
                                 severity=args.severity, limit=args.limit)
        else:
            if args.command == "acknowledge":
                acknowledge_alert(args.alert_id, by=args.by)
                status = "ACKNOWLEDGED"
            else:
                resolve_alert(args.alert_id, by=args.by, note=args.note)
                status = "RESOLVED"
            output = {"alert_event_id": str(args.alert_id), "lifecycle_status": status}
        print(json.dumps(output, default=str, indent=2))
    except (ValueError, PersistenceError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
