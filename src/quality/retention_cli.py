"""Explicit preview/apply CLI for monitoring retention."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys

from src.quality.persistence import PersistenceError
from src.quality.retention import (
    RetentionConfig,
    RetentionError,
    apply_retention,
    preview_retention,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Preview or apply Pulse monitoring retention")
    commands = parser.add_subparsers(dest="command", required=True)
    preview = commands.add_parser("preview", help="Report eligible rows without deleting")
    preview.add_argument("--days", type=int)
    apply = commands.add_parser("apply", help="Delete eligible rows transactionally")
    apply.add_argument("--days", type=int)
    apply.add_argument("--confirm", action="store_true", help="Required explicit deletion confirmation")
    args = parser.parse_args(argv)
    try:
        config = RetentionConfig.from_environ() if args.days is None else RetentionConfig(args.days)
        if not 1 <= config.days <= 3650:
            raise RetentionError("retention days must be between 1 and 3650")
        report = preview_retention(config=config) if args.command == "preview" else apply_retention(
            confirm=args.confirm, config=config
        )
        output = asdict(report)
        output["total_rows"] = report.total_rows
        print(json.dumps(output, default=str, indent=2))
    except (RetentionError, PersistenceError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
