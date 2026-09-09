"""Small local SQLite persistence for connector checkpoints and health."""

from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
from typing import Iterable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONNECTOR_STATE_PATH = "data/state/connectors.sqlite3"


def _utc_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.utcoffset() is None:
        raise ValueError("Connector timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectorState:
    business_id: str
    source_id: str
    last_attempted_at_utc: datetime | None = None
    last_successful_at_utc: datetime | None = None
    checkpoint_utc: datetime | None = None
    records_extracted: int = 0
    latest_error: str | None = None
    health: str = "unknown"

    def to_dict(self) -> dict:
        return {
            "business_id": self.business_id,
            "source_id": self.source_id,
            "last_attempted_at_utc": _utc_text(self.last_attempted_at_utc),
            "last_successful_at_utc": _utc_text(self.last_successful_at_utc),
            "checkpoint_utc": _utc_text(self.checkpoint_utc),
            "records_extracted": self.records_extracted,
            "latest_error": self.latest_error,
            "health": self.health,
        }


class ConnectorStateStore:
    def __init__(
        self,
        path: Path | str | None = None,
        *,
        environ: Mapping[str, str] | None = None,
    ):
        environment = os.environ if environ is None else environ
        selected = Path(path or environment.get("CONNECTOR_STATE_PATH", DEFAULT_CONNECTOR_STATE_PATH))
        self.path = selected if selected.is_absolute() else (PROJECT_ROOT / selected).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def _session(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._session() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS connector_state (
                    business_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    last_attempted_at_utc TEXT,
                    last_successful_at_utc TEXT,
                    checkpoint_utc TEXT,
                    records_extracted INTEGER NOT NULL DEFAULT 0,
                    latest_error TEXT,
                    health TEXT NOT NULL DEFAULT 'unknown',
                    PRIMARY KEY (business_id, source_id)
                );
                CREATE TABLE IF NOT EXISTS shopify_projection (
                    business_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    order_id TEXT NOT NULL,
                    event_ids_json TEXT NOT NULL,
                    PRIMARY KEY (business_id, source_id, order_id)
                );
            """)

    def get(self, business_id: str, source_id: str) -> ConnectorState:
        with self._session() as connection:
            row = connection.execute(
                """SELECT last_attempted_at_utc,last_successful_at_utc,checkpoint_utc,
                          records_extracted,latest_error,health
                   FROM connector_state WHERE business_id=? AND source_id=?""",
                (business_id, source_id),
            ).fetchone()
        if row is None:
            return ConnectorState(business_id=business_id, source_id=source_id)
        return ConnectorState(
            business_id=business_id,
            source_id=source_id,
            last_attempted_at_utc=_parse(row[0]),
            last_successful_at_utc=_parse(row[1]),
            checkpoint_utc=_parse(row[2]),
            records_extracted=int(row[3]),
            latest_error=row[4],
            health=row[5],
        )

    def record_attempt(self, business_id: str, source_id: str, at: datetime) -> None:
        with self._session() as connection:
            connection.execute(
                """INSERT INTO connector_state
                       (business_id,source_id,last_attempted_at_utc,health)
                   VALUES (?,?,?,'running')
                   ON CONFLICT(business_id,source_id) DO UPDATE SET
                       last_attempted_at_utc=excluded.last_attempted_at_utc,
                       health='running'""",
                (business_id, source_id, _utc_text(at)),
            )

    def record_failure(
        self, business_id: str, source_id: str, error: str, *, health: str = "unhealthy"
    ) -> None:
        with self._session() as connection:
            connection.execute(
                """INSERT INTO connector_state
                       (business_id,source_id,latest_error,health)
                   VALUES (?,?,?,?)
                   ON CONFLICT(business_id,source_id) DO UPDATE SET
                       latest_error=excluded.latest_error,health=excluded.health""",
                (business_id, source_id, error[:500], health),
            )

    def projection_ids(self, business_id: str, source_id: str, order_id: str) -> set[str]:
        with self._session() as connection:
            row = connection.execute(
                """SELECT event_ids_json FROM shopify_projection
                   WHERE business_id=? AND source_id=? AND order_id=?""",
                (business_id, source_id, order_id),
            ).fetchone()
        return set(json.loads(row[0])) if row else set()

    def record_success(
        self,
        business_id: str,
        source_id: str,
        at: datetime,
        checkpoint: datetime | None,
        records_extracted: int,
        projections: Iterable[tuple[str, set[str]]],
    ) -> None:
        """Advance checkpoint and projection state in one local transaction."""

        with self._session() as connection:
            connection.execute(
                """INSERT INTO connector_state
                       (business_id,source_id,last_attempted_at_utc,last_successful_at_utc,
                        checkpoint_utc,records_extracted,latest_error,health)
                   VALUES (?,?,?,?,?, ?,NULL,'healthy')
                   ON CONFLICT(business_id,source_id) DO UPDATE SET
                       last_attempted_at_utc=excluded.last_attempted_at_utc,
                       last_successful_at_utc=excluded.last_successful_at_utc,
                       checkpoint_utc=COALESCE(excluded.checkpoint_utc,connector_state.checkpoint_utc),
                       records_extracted=excluded.records_extracted,
                       latest_error=NULL,health='healthy'""",
                (business_id, source_id, _utc_text(at), _utc_text(at),
                 _utc_text(checkpoint), records_extracted),
            )
            for order_id, event_ids in projections:
                connection.execute(
                    """INSERT INTO shopify_projection
                           (business_id,source_id,order_id,event_ids_json)
                       VALUES (?,?,?,?)
                       ON CONFLICT(business_id,source_id,order_id) DO UPDATE SET
                           event_ids_json=excluded.event_ids_json""",
                    (business_id, source_id, order_id, json.dumps(sorted(event_ids))),
                )
