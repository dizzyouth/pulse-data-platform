"""Explicit, repeatable initialization of the monitoring presentation views.

No quality-history writes; the quality engine and BI provisioner are independent.
Run after Phase 5.3 monitoring schema initialization.
"""

from pathlib import Path
import sys

import psycopg

from src.warehouse.load_gold import connection_kwargs


def ensure_monitoring_views():
    """Create/replace all views atomically using the existing warehouse config."""
    try:
        with psycopg.connect(**connection_kwargs()) as connection:
            connection.execute("SELECT pg_advisory_xact_lock(5454001)")
            for statement in Path(__file__).with_name("monitoring_views.sql").read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    connection.execute(statement)
    except Exception:
        raise RuntimeError("Monitoring view initialization failed; initialize the Phase 5.3 tables first "
                           "and check warehouse availability and permissions") from None


def main():
    try:
        ensure_monitoring_views()
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        return 1
    print("Monitoring presentation views ready in monitoring_views.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
