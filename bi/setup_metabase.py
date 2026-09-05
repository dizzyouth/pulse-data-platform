"""Reconcile local Metabase, its warehouse connection, and the Pulse dashboard."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any


METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@pulse.local")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "PulseLocal!4xN7qB2v")
SITE_NAME = os.environ.get("METABASE_SITE_NAME", "Pulse Analytics")
WAREHOUSE_NAME = os.environ.get("METABASE_WAREHOUSE_NAME", "Pulse Analytics Warehouse")


def _warehouse_details() -> dict[str, Any]:
    return {
        "host": os.environ.get("METABASE_WAREHOUSE_HOST", "warehouse-postgres"),
        "port": int(os.environ.get("METABASE_WAREHOUSE_PORT", "5432")),
        "dbname": os.environ.get("METABASE_WAREHOUSE_DB", "pulse_analytics"),
        "user": os.environ.get("METABASE_WAREHOUSE_USER", "pulse"),
        "password": os.environ.get(
            "METABASE_WAREHOUSE_PASSWORD", "pulse-local-development-only"
        ),
        "ssl": False,
    }


def _request(
    method: str,
    path: str,
    payload: Mapping[str, Any] | None = None,
    session_id: str | None = None,
) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{METABASE_URL}{path}", data=data, method=method
    )
    request.add_header("Content-Type", "application/json")
    if session_id:
        request.add_header("X-Metabase-Session", session_id)
    # Retrying a timed-out create can duplicate an object whose response was lost.
    attempts = 5 if method == "GET" else 1
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
            break
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Metabase {method} {path} failed ({error.code}): {detail}"
            ) from error
        except (OSError, TimeoutError) as error:
            if attempt == attempts:
                raise RuntimeError(
                    f"Metabase {method} {path} was unavailable after {attempt} attempts"
                ) from error
            print(f"Metabase {method} {path} unavailable; retrying ({attempt}/5).")
            time.sleep(attempt * 2)
    return json.loads(body) if body else None


def _login() -> str:
    response = _request(
        "POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
    )
    return str(response["id"])


def _complete_initial_setup(token: str) -> None:
    _request(
        "POST",
        "/api/setup",
        {
            "token": token,
            "user": {
                "first_name": "Pulse",
                "last_name": "Admin",
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "site_name": SITE_NAME,
            },
            "prefs": {
                "site_name": SITE_NAME,
                "site_locale": "en",
                "allow_tracking": False,
            },
            # Register after setup with the regular database endpoint. That
            # endpoint reports connection failures clearly and is idempotent.
            "database": None,
        },
    )
    print("Completed initial Metabase setup.")


def _databases(session_id: str) -> list[dict[str, Any]]:
    response = _request("GET", "/api/database", session_id=session_id)
    return response.get("data", []) if isinstance(response, dict) else response


def _ensure_warehouse(session_id: str) -> int:
    databases = _databases(session_id)
    warehouse = _unique(databases, WAREHOUSE_NAME)
    expected = _warehouse_details()
    if warehouse is None:
        warehouse = _request(
            "POST",
            "/api/database",
            {"engine": "postgres", "name": WAREHOUSE_NAME, "details": expected},
            session_id,
        )
        print("Registered the Pulse warehouse in the existing Metabase instance.")
    else:
        actual = warehouse.get("details", {})
        for key in ("host", "port", "dbname"):
            if str(actual.get(key)) != str(expected[key]):
                raise RuntimeError(
                    f"Existing {WAREHOUSE_NAME!r} has unexpected {key}; "
                    "update or remove it in Admin > Databases before retrying."
                )
        print("Pulse warehouse connection already exists with the expected target.")
    return int(warehouse["id"])


def _verify_marts(session_id: str, database_id: int) -> None:
    query = """
        SELECT 'funnel_performance' AS mart, count(*) AS row_count FROM marts.funnel_performance
        UNION ALL SELECT 'revenue_by_day', count(*) FROM marts.revenue_by_day
        UNION ALL SELECT 'top_customers', count(*) FROM marts.top_customers
        UNION ALL SELECT 'top_products', count(*) FROM marts.top_products
        ORDER BY mart
    """
    result = _request(
        "POST",
        "/api/dataset",
        {
            "database": database_id,
            "type": "native",
            "native": {"query": query, "template-tags": {}},
            "parameters": [],
        },
        session_id,
    )
    if result.get("status") != "completed":
        raise RuntimeError(f"Metabase mart verification failed: {result.get('error', result.get('status'))}")
    rows = result.get("data", {}).get("rows", [])
    if len(rows) != 4 or any(int(row[1]) <= 0 for row in rows):
        raise RuntimeError(f"Expected four non-empty dbt marts, received: {rows!r}")
    print("Metabase queried marts successfully: " + ", ".join(f"{r[0]}={r[1]}" for r in rows))


def _unique(items: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    matches = [item for item in items if item.get("name") == name]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple objects named {name!r}; resolve duplicates before rerunning.")
    return matches[0] if matches else None


def _ensure_dashboard(session_id: str, database_id: int) -> None:
    """Reconcile the six managed questions; preserve unrelated dashboard cards."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    collection = _unique(api("GET", "/api/collection"), "Pulse Marketplace")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Marketplace"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    specs = [
        ("revenue_overview", "Revenue overview", "table"),
        ("revenue_trend", "Revenue trend", "line"),
        ("funnel", "Funnel", "table"),
        ("top_customers", "Top customers by units", "table"),
        ("top_products", "Top products by units", "table"),
        ("geography", "Geography", "table"),
    ]
    cards = []
    for filename, title, display in specs:
        sql = (Path(__file__).parent / "queries" / f"{filename}.sql").read_text()
        dimensions = (["start_date", "end_date", "currency"] if filename.startswith("revenue")
                      else ["start_date", "end_date", "country"] if filename in ("funnel", "geography") else [])
        tags = {name: {"id": name, "name": name, "display-name": name.replace("_", " ").title(),
                       "type": "date" if name.endswith("date") else "text", "required": False}
                for name in dimensions}
        clauses = []
        for name in dimensions:
            column = "event_date" if name.endswith("date") else name
            operator = ">=" if name == "start_date" else "<=" if name == "end_date" else "="
            clauses.append(f"[[AND {column} {operator} {{{{{name}}}}}]]")
        if clauses:
            lines = sql.splitlines()
            index = next(i for i, line in enumerate(lines) if line.startswith("FROM marts.")) + 1
            lines[index:index] = ["WHERE 1=1", *clauses]
            sql = "\n".join(lines)
        query = {"database": database_id, "type": "native",
                 "native": {"query": sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"BI query {filename} failed: {result.get('error', result.get('status'))}")
        print(f"Verified {filename}: {len(result['data']['rows'])} rows; sample={result['data']['rows'][:2]}")
        settings = {"graph.dimensions": ["event_date", "currency"], "graph.metrics": ["gross_revenue"]} if display == "line" else {}
        payload = {"name": title, "collection_id": collection_id, "display": display,
                   "dataset_query": query, "visualization_settings": settings}
        existing = _unique([i for i in items if i.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append((card, dimensions))

    title = "Pulse Marketplace Overview"
    dashboard = _unique([i for i in items if i.get("model") == "dashboard"], title)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": title, "collection_id": collection_id})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    parameters = [{"id": name, "name": name.replace("_", " ").title(), "slug": name,
                   "type": "date/single" if name.endswith("date") else "string/="}
                  for name in ("start_date", "end_date", "currency", "country")]
    dashcards = dashboard.get("dashcards", [])
    for index, (card, dimensions) in enumerate(cards):
        existing = next((d for d in dashcards if d.get("card_id") == card["id"]), None)
        mappings = [{"parameter_id": name, "card_id": card["id"],
                     "target": ["variable", ["template-tag", name]]} for name in dimensions]
        if existing:
            existing["parameter_mappings"] = mappings
        else:
            dashcards.append({"id": -(index + 1), "card_id": card["id"], "row": (index // 2) * 8,
                              "col": (index % 2) * 12, "size_x": 12, "size_y": 8,
                              "parameter_mappings": mappings, "visualization_settings": {}})
    managed = {p["id"] for p in parameters}
    parameters.extend(p for p in dashboard.get("parameters", []) if p["id"] not in managed)
    api("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": dashcards, "parameters": parameters})
    print(f"Dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def main() -> int:
    properties = _request("GET", "/api/session/properties")
    setup_token = properties.get("setup-token")
    if setup_token:
        _complete_initial_setup(str(setup_token))
    session_id = _login()
    database_id = _ensure_warehouse(session_id)
    _verify_marts(session_id, database_id)
    _ensure_dashboard(session_id, database_id)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Metabase setup failed: {error}", file=sys.stderr)
        raise SystemExit(1)
