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
        UNION ALL SELECT 'marketing_overview', count(*) FROM marts.marketing_overview
        UNION ALL SELECT 'campaign_performance', count(*) FROM marts.campaign_performance
        UNION ALL SELECT 'ad_group_performance', count(*) FROM marts.ad_group_performance
        UNION ALL SELECT 'ad_performance', count(*) FROM marts.ad_performance
        UNION ALL SELECT 'operations_overview', count(*) FROM marts.operations_overview
        UNION ALL SELECT 'operations_daily', count(*) FROM marts.operations_daily
        UNION ALL SELECT 'confirmation_operations', count(*) FROM marts.confirmation_operations
        UNION ALL SELECT 'delivery_operations', count(*) FROM marts.delivery_operations
        UNION ALL SELECT 'cod_performance', count(*) FROM marts.cod_performance
        UNION ALL SELECT 'remittance_operations', count(*) FROM marts.remittance_operations
        UNION ALL SELECT 'commerce_economics', count(*) FROM marts.commerce_economics
        UNION ALL SELECT 'unit_economics', count(*) FROM marts.unit_economics
        UNION ALL SELECT 'economics_daily', count(*) FROM marts.economics_daily
        UNION ALL SELECT 'campaign_economics', count(*) FROM marts.campaign_economics
        UNION ALL SELECT 'cod_economics', count(*) FROM marts.cod_economics
        UNION ALL SELECT 'olist_orders_by_status', count(*) FROM marts.olist_orders_by_status
        UNION ALL SELECT 'olist_commerce_daily', count(*) FROM marts.olist_commerce_daily
        UNION ALL SELECT 'olist_payment_methods', count(*) FROM marts.olist_payment_methods
        UNION ALL SELECT 'olist_data_quality', count(*) FROM marts.olist_data_quality
        UNION ALL SELECT 'olist_economic_completeness', count(*) FROM marts.olist_economic_completeness
        UNION ALL SELECT 'uci_retail_daily', count(*) FROM marts.uci_retail_daily
        UNION ALL SELECT 'uci_invoice_summary', count(*) FROM marts.uci_invoice_summary
        UNION ALL SELECT 'uci_line_classification', count(*) FROM marts.uci_line_classification
        UNION ALL SELECT 'uci_country_distribution', count(*) FROM marts.uci_country_distribution
        UNION ALL SELECT 'uci_data_quality', count(*) FROM marts.uci_data_quality
        UNION ALL SELECT 'uci_economic_completeness', count(*) FROM marts.uci_economic_completeness
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
    if len(rows) != 30 or any(int(row[1]) <= 0 for row in rows):
        raise RuntimeError(f"Expected thirty non-empty dbt marts, received: {rows!r}")
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
        dimensions = (["business", "start_date", "end_date", "currency"] if filename.startswith("revenue")
                      else ["business", "start_date", "end_date", "country"] if filename in ("funnel", "geography")
                      else ["business"])
        tags = {name: {"id": name, "name": name, "display-name": name.replace("_", " ").title(),
                       "type": "date" if name.endswith("date") else "text", "required": False}
                for name in dimensions}
        clauses = []
        for name in dimensions:
            column = "event_date" if name.endswith("date") else "business_id" if name == "business" else name
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
                  for name in ("business", "start_date", "end_date", "currency", "country")]
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


def _ensure_marketing_dashboard(session_id: str, database_id: int) -> None:
    """Reconcile a separate marketing dashboard with explicit attribution labels."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    collection = _unique(api("GET", "/api/collection"), "Pulse Marketing")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Marketing"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    specs = [
        ("spend_by_platform", "Spend by platform", "bar", False),
        ("spend_trend", "Spend trend", "line", False),
        ("campaign_performance", "Campaign performance", "table", True),
        ("efficiency", "CTR / CPC / CPM", "line", False),
        ("conversions_cpa", "Platform-reported conversions / CPA", "line", False),
        ("platform_roas", "Platform-reported ROAS", "line", False),
        ("top_campaigns", "Top campaigns", "table", True),
        ("top_ads", "Top ads", "table", True),
    ]
    cards = []
    for filename, title, display, campaign_filter in specs:
        base_sql = (Path(__file__).parent / "marketing_queries" / f"{filename}.sql").read_text().rstrip(";\n")
        dimensions = ["business", "platform", "currency", "start_date", "end_date"]
        if campaign_filter:
            dimensions.append("campaign")
        columns = {"business": "business_id", "start_date": "report_date", "end_date": "report_date",
                   "campaign": "campaign_id"}
        clauses = []
        for name in dimensions:
            column = columns.get(name, name)
            operator = ">=" if name == "start_date" else "<=" if name == "end_date" else "="
            clauses.append(f"[[AND {column} {operator} {{{{{name}}}}}]]")
        query_sql = "SELECT * FROM (\n" + base_sql + "\n) AS marketing_card\nWHERE 1=1\n" + "\n".join(clauses)
        tags = {name: {"id": name, "name": name, "display-name": name.replace("_", " ").title(),
                       "type": "date" if name.endswith("date") else "text", "required": False}
                for name in dimensions}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"Marketing BI query {filename} failed: {result.get('error', result.get('status'))}")
        payload = {"name": title, "collection_id": collection_id, "display": display,
                   "dataset_query": query, "visualization_settings": {}}
        existing = _unique([item for item in items if item.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append((card, dimensions))

    title = "Pulse Marketing Performance"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": title, "collection_id": collection_id})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    parameters = [{"id": name, "name": name.replace("_", " ").title(), "slug": name,
                   "type": "date/single" if name.endswith("date") else "string/="}
                  for name in ("business", "platform", "campaign", "currency", "start_date", "end_date")]
    dashcards = dashboard.get("dashcards", [])
    for index, (card, dimensions) in enumerate(cards):
        mappings = [{"parameter_id": name, "card_id": card["id"],
                     "target": ["variable", ["template-tag", name]]} for name in dimensions]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        if existing:
            existing["parameter_mappings"] = mappings
        else:
            dashcards.append({"id": -(100 + index), "card_id": card["id"], "row": (index // 2) * 8,
                              "col": (index % 2) * 12, "size_x": 12, "size_y": 8,
                              "parameter_mappings": mappings, "visualization_settings": {}})
    managed = {parameter["id"] for parameter in parameters}
    parameters.extend(parameter for parameter in dashboard.get("parameters", [])
                      if parameter["id"] not in managed)
    api("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": dashcards, "parameters": parameters})
    print(f"Marketing dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def _ensure_operations_dashboard(session_id: str, database_id: int) -> None:
    """Reconcile a separate operations/COD dashboard with precise measure labels."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    collection = _unique(api("GET", "/api/collection"), "Pulse Commerce Operations")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Commerce Operations"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    specs = [
        ("orders_by_current_status", "Orders by current status", "bar", ("business","payment_type","currency","operational_status")),
        ("confirmation_rate", "Confirmation rate", "line", ("business","provider","payment_type","currency","start_date","end_date")),
        ("shipment_volume", "Shipment volume", "line", ("business","courier","payment_type","currency","start_date","end_date")),
        ("delivery_rate", "Delivery rate", "line", ("business","courier","payment_type","currency","start_date","end_date")),
        ("delivered_orders", "Delivered orders", "line", ("business","courier","payment_type","currency","start_date","end_date")),
        ("refusal_rate", "Refusal rate", "line", ("business","courier","payment_type","currency","start_date","end_date")),
        ("return_rate", "Return rate", "line", ("business","courier","payment_type","currency","start_date","end_date")),
        ("unreachable_rate", "Unreachable rate", "line", ("business","provider","payment_type","currency","start_date","end_date")),
        ("delivery_attempts", "Delivery attempts", "line", ("business","courier","payment_type","currency","start_date","end_date")),
        ("courier_performance", "Courier performance", "table", ("business","courier","payment_type","currency","start_date","end_date")),
        ("cod_cash_collected", "COD cash collected", "line", ("business","provider","currency","start_date","end_date")),
        ("pending_remittance", "Pending remittance", "line", ("business","provider","currency","settlement_status","start_date","end_date")),
        ("net_remitted", "Net remitted", "line", ("business","provider","currency","settlement_status","start_date","end_date")),
        ("orders_stuck", "Orders stuck by lifecycle stage", "table", ("business","payment_type","currency","operational_status")),
    ]
    date_columns = {"confirmation_rate":"cohort_date", "shipment_volume":"cohort_date",
                    "delivery_rate":"cohort_date", "delivered_orders":"cohort_date", "refusal_rate":"cohort_date",
                    "return_rate":"cohort_date", "unreachable_rate":"cohort_date",
                    "delivery_attempts":"cohort_date", "courier_performance":"cohort_date", "cod_cash_collected":"collection_date",
                    "pending_remittance":"period_end", "net_remitted":"period_end"}
    cards = []
    for filename, title, display, dimensions in specs:
        base_sql = (Path(__file__).parent / "operations_queries" / f"{filename}.sql").read_text().rstrip(";\n")
        columns = {"business":"business_id", "operational_status":"current_operational_status"}
        clauses = []
        for name in dimensions:
            column = date_columns.get(filename) if name.endswith("date") else columns.get(name, name)
            operator = ">=" if name == "start_date" else "<=" if name == "end_date" else "="
            clauses.append(f"[[AND {column} {operator} {{{{{name}}}}}]]")
        query_sql = "SELECT * FROM (\n" + base_sql + "\n) AS operations_card\nWHERE 1=1\n" + "\n".join(clauses)
        tags = {name: {"id": name, "name": name, "display-name": name.replace("_", " ").title(),
                       "type": "date" if name.endswith("date") else "text", "required": False}
                for name in dimensions}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"Operations BI query {filename} failed: {result.get('error', result.get('status'))}")
        payload = {"name": title, "collection_id": collection_id, "display": display,
                   "dataset_query": query, "visualization_settings": {}}
        existing = _unique([item for item in items if item.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append((card, dimensions))
    title = "Pulse Commerce Operations"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": title, "collection_id": collection_id})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    parameter_names = ("business","provider","courier","payment_type","currency",
                       "operational_status","settlement_status","start_date","end_date")
    parameters = [{"id": name, "name": name.replace("_", " ").title(), "slug": name,
                   "type": "date/single" if name.endswith("date") else "string/="} for name in parameter_names]
    dashcards = dashboard.get("dashcards", [])
    for index, (card, dimensions) in enumerate(cards):
        mappings = [{"parameter_id": name, "card_id": card["id"],
                     "target": ["variable", ["template-tag", name]]} for name in dimensions]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        if existing: existing["parameter_mappings"] = mappings
        else: dashcards.append({"id": -(200 + index), "card_id": card["id"], "row": (index // 2) * 8,
                                "col": (index % 2) * 12, "size_x": 12, "size_y": 8,
                                "parameter_mappings": mappings, "visualization_settings": {}})
    managed = {parameter["id"] for parameter in parameters}
    parameters.extend(parameter for parameter in dashboard.get("parameters", []) if parameter["id"] not in managed)
    api("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": dashcards, "parameters": parameters})
    print(f"Commerce operations dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def _ensure_economics_dashboard(session_id: str, database_id: int) -> None:
    """Reconcile contribution-economics cards without implying accounting profit."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    collection = _unique(api("GET", "/api/collection"), "Pulse Commerce Economics")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Commerce Economics"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    cohort_filters = ("business", "currency", "start_date", "end_date")
    specs = [
        ("economic_value_lenses", "Order / delivered / cash / remittance values", "line", cohort_filters),
        ("marketing_spend", "Marketing spend", "line", cohort_filters),
        ("product_cogs", "Product COGS", "line", cohort_filters),
        ("operational_costs", "Variable operational costs", "line", cohort_filters),
        ("contribution_before_marketing", "Contribution before marketing", "line", cohort_filters),
        ("contribution_after_marketing", "Contribution after marketing", "line", cohort_filters),
        ("cost_per_order", "Marketing cost per order", "line", cohort_filters),
        ("cost_per_confirmed_order", "Marketing cost per confirmed order", "line", cohort_filters),
        ("cost_per_delivered_order", "Marketing cost per delivered order", "line", cohort_filters),
        ("contribution_per_delivered_order", "Contribution per delivered order", "line", cohort_filters),
        ("contribution_margin", "Contribution margin", "line", cohort_filters),
        ("pending_remittance", "Pending remittance", "line", cohort_filters),
        ("delivered_unremitted", "Delivered but unremitted", "line", cohort_filters),
        ("attributed_campaign_economics", "Attributed campaign economics", "table",
         ("business", "platform", "campaign", "currency", "start_date", "end_date")),
        ("unattributed_orders", "Unattributed orders", "table",
         ("business", "payment_type", "currency", "start_date", "end_date")),
        ("incomplete_economics", "Incomplete economics / missing COGS", "table",
         ("business", "payment_type", "currency", "economic_completeness", "start_date", "end_date")),
    ]
    date_columns = {
        "economic_value_lenses": "event_date",
        "attributed_campaign_economics": "marketing_date",
        "pending_remittance": "order_created_date",
        "unattributed_orders": "order_created_date",
        "incomplete_economics": "order_created_date",
    }
    cards = []
    for filename, title, display, dimensions in specs:
        base_sql = (Path(__file__).parent / "economics_queries" / f"{filename}.sql").read_text().rstrip(";\n")
        columns = {"business": "business_id", "campaign": "campaign_id",
                   "currency": "marketing_currency" if filename == "attributed_campaign_economics" else "currency",
                   "economic_completeness": "economic_status"}
        clauses = []
        for name in dimensions:
            column = (date_columns.get(filename, "cohort_date") if name.endswith("date")
                      else columns.get(name, name))
            operator = ">=" if name == "start_date" else "<=" if name == "end_date" else "="
            clauses.append(f"[[AND {column} {operator} {{{{{name}}}}}]]")
        query_sql = "SELECT * FROM (\n" + base_sql + "\n) AS economics_card\nWHERE 1=1\n" + "\n".join(clauses)
        tags = {name: {"id": name, "name": name,
                       "display-name": name.replace("_", " ").title(),
                       "type": "date" if name.endswith("date") else "text", "required": False}
                for name in dimensions}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"Economics BI query {filename} failed: {result.get('error', result.get('status'))}")
        payload = {"name": title, "collection_id": collection_id, "display": display,
                   "dataset_query": query, "visualization_settings": {}}
        existing = _unique([item for item in items if item.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append((card, dimensions))

    title = "Pulse Commerce Economics"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": title, "collection_id": collection_id})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    parameter_names = ("business", "currency", "payment_type", "platform", "campaign",
                       "economic_completeness", "start_date", "end_date")
    parameters = [{"id": name, "name": name.replace("_", " ").title(), "slug": name,
                   "type": "date/single" if name.endswith("date") else "string/="}
                  for name in parameter_names]
    dashcards = dashboard.get("dashcards", [])
    for index, (card, dimensions) in enumerate(cards):
        mappings = [{"parameter_id": name, "card_id": card["id"],
                     "target": ["variable", ["template-tag", name]]} for name in dimensions]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        if existing:
            existing["parameter_mappings"] = mappings
        else:
            dashcards.append({"id": -(300 + index), "card_id": card["id"],
                              "row": (index // 2) * 8, "col": (index % 2) * 12,
                              "size_x": 12, "size_y": 8, "parameter_mappings": mappings,
                              "visualization_settings": {}})
    managed = {parameter["id"] for parameter in parameters}
    parameters.extend(parameter for parameter in dashboard.get("parameters", [])
                      if parameter["id"] not in managed)
    api("PUT", f"/api/dashboard/{dashboard['id']}",
        {"dashcards": dashcards, "parameters": parameters})
    print(f"Commerce economics dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def _ensure_olist_dashboard(session_id: str, database_id: int) -> None:
    """Reconcile the benchmark-only health and semantics dashboard."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    collection = _unique(api("GET", "/api/collection"), "Pulse Olist Benchmark")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Olist Benchmark"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    specs = (
        ("orders_by_status", "Orders by provider-native status", "bar"),
        ("commerce_daily", "Commerce and payment totals", "line"),
        ("payment_methods", "Payment method distribution", "bar"),
        ("data_quality", "Benchmark data-quality observations", "table"),
        ("economic_completeness", "Economics completeness", "table"),
    )
    cards = []
    for filename, title, display in specs:
        base = (Path(__file__).parent / "olist_queries" / f"{filename}.sql").read_text().rstrip(";\n")
        query_sql = "SELECT * FROM (\n" + base + "\n) AS olist_card\nWHERE 1=1\n[[AND business_id = {{business}}]]"
        tags = {"business": {"id": "business", "name": "business", "display-name": "Business",
                             "type": "text", "required": False}}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"Olist BI query {filename} failed: {result.get('error', result.get('status'))}")
        payload = {"name": title, "collection_id": collection_id, "display": display,
                   "dataset_query": query, "visualization_settings": {}}
        existing = _unique([item for item in items if item.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append(card)

    title = "Pulse Olist Benchmark"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": title, "collection_id": collection_id})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    dashcards = dashboard.get("dashcards", [])
    for index, card in enumerate(cards):
        mappings = [{"parameter_id": "business", "card_id": card["id"],
                     "target": ["variable", ["template-tag", "business"]]}]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        if existing:
            existing["parameter_mappings"] = mappings
        else:
            dashcards.append({"id": -(400 + index), "card_id": card["id"],
                              "row": (index // 2) * 8, "col": (index % 2) * 12,
                              "size_x": 12, "size_y": 8, "parameter_mappings": mappings,
                              "visualization_settings": {}})
    parameter = {"id": "business", "name": "Business", "slug": "business", "type": "string/="}
    others = [item for item in dashboard.get("parameters", []) if item["id"] != "business"]
    api("PUT", f"/api/dashboard/{dashboard['id']}",
        {"dashcards": dashcards, "parameters": [parameter, *others]})
    print(f"Olist benchmark dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def _ensure_uci_dashboard(session_id: str, database_id: int) -> None:
    """Reconcile the compact UCI portability benchmark dashboard."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    collection = _unique(api("GET", "/api/collection"), "Pulse UCI Retail Benchmark")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse UCI Retail Benchmark"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    specs = (
        ("daily_invoice_volume", "Daily invoice volume", "line"),
        ("daily_ledger_value", "Daily signed ledger values", "line"),
        ("anonymous_customer_rate", "Anonymous customer rate", "line"),
        ("invoice_summary", "Worksheet-scoped invoice semantics", "table"),
        ("line_classification", "Source-line classifications", "bar"),
        ("country_distribution", "Country distribution", "bar"),
        ("data_quality", "Benchmark data-quality observations", "table"),
        ("economic_completeness", "Economics completeness", "table"),
    )
    cards = []
    for filename, title, display in specs:
        base = (Path(__file__).parent / "uci_queries" / f"{filename}.sql").read_text().rstrip(";\n")
        query_sql = "SELECT * FROM (\n" + base + "\n) AS uci_card\nWHERE 1=1\n[[AND business_id = {{business}}]]"
        tags = {"business": {"id": "business", "name": "business", "display-name": "Business",
                             "type": "text", "required": False}}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"UCI BI query {filename} failed: {result.get('error', result.get('status'))}")
        payload = {"name": title, "collection_id": collection_id, "display": display,
                   "dataset_query": query, "visualization_settings": {}}
        existing = _unique([item for item in items if item.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append(card)

    title = "Pulse UCI Retail Benchmark"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": title, "collection_id": collection_id})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    dashcards = dashboard.get("dashcards", [])
    for index, card in enumerate(cards):
        mappings = [{"parameter_id": "business", "card_id": card["id"],
                     "target": ["variable", ["template-tag", "business"]]}]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        if existing:
            existing["parameter_mappings"] = mappings
        else:
            dashcards.append({"id": -(500 + index), "card_id": card["id"],
                              "row": (index // 2) * 8, "col": (index % 2) * 12,
                              "size_x": 12, "size_y": 8, "parameter_mappings": mappings,
                              "visualization_settings": {}})
    parameter = {"id": "business", "name": "Business", "slug": "business", "type": "string/="}
    others = [item for item in dashboard.get("parameters", []) if item["id"] != "business"]
    api("PUT", f"/api/dashboard/{dashboard['id']}",
        {"dashcards": dashcards, "parameters": [parameter, *others]})
    print(f"UCI retail benchmark dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def _ensure_sama_pilot_dashboard(session_id: str, database_id: int) -> None:
    """Create the executive, aggregate-only real COD pilot dashboard."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    def field_format(name: str, *, decimals: int, style: str | None = None,
                     currency: str | None = None) -> dict[str, Any]:
        formatting: dict[str, Any] = {"decimals": decimals}
        if style is not None:
            formatting["number_style"] = style
        if currency is not None:
            formatting.update({"currency": currency, "currency_style": "symbol",
                               "currency_in_header": True})
        return {json.dumps(["name", name], separators=(",", ":")): formatting}

    def count_settings(field: str) -> dict[str, Any]:
        return {"scalar.field": field,
                "column_settings": field_format(field, decimals=0)}

    def rate_settings(field: str) -> dict[str, Any]:
        return {"scalar.field": field,
                "column_settings": field_format(field, decimals=2, style="percent")}

    available = api("POST", "/api/dataset", {
        "database": database_id, "type": "native", "parameters": [],
        "native": {"query": "SELECT to_regclass('marts.sama_pilot_funnel') IS NOT NULL",
                   "template-tags": {}},
    })
    rows = available.get("data", {}).get("rows", []) if available.get("status") == "completed" else []
    if not rows or not rows[0][0]:
        print("SAMA pilot marts are not loaded; skipping the optional pilot dashboard.")
        return

    collection = _unique(api("GET", "/api/collection"), "Pulse Real COD Pilot")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Real COD Pilot"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])

    specs = (
        {"filename": "initial_orders_kpi", "title": "Initial Orders", "display": "scalar",
         "columns": "initial_orders", "settings": count_settings("initial_orders"),
         "description": "Initial Lightfunnels orders in the validated pilot cohort.",
         "layout": (0, 0, 4, 4)},
        {"filename": "confirmed_orders_kpi", "title": "Confirmed COD Orders", "display": "scalar",
         "columns": "confirmed_orders", "settings": count_settings("confirmed_orders"),
         "description": "Matched leads confirmed for COD fulfillment.",
         "layout": (0, 4, 4, 4)},
        {"filename": "delivered_orders_kpi", "title": "Delivered Orders", "display": "scalar",
         "columns": "delivered_orders", "settings": count_settings("delivered_orders"),
         "description": "Final COD orders with delivered status.",
         "layout": (0, 8, 4, 4)},
        {"filename": "return_rate_kpi", "title": "Return Rate", "display": "scalar",
         "columns": "return_rate", "settings": rate_settings("return_rate"),
         "description": "Returned orders as a percentage of shipped orders.",
         "layout": (0, 12, 4, 4)},
        {"filename": "collected_revenue_kpi", "title": "Collected Revenue — Native Currency",
         "display": "table", "columns": "currency, collected_revenue",
         "settings": {"column_settings": field_format("collected_revenue", decimals=2)},
         "description": "Cash collected, kept separate for every native currency.",
         "layout": (0, 16, 8, 4)},
        {"filename": "confirmation_rate_kpi", "title": "Confirmation Rate", "display": "scalar",
         "columns": "confirmation_rate", "settings": rate_settings("confirmation_rate"),
         "description": "Confirmed leads as a percentage of high-confidence matches.",
         "layout": (4, 0, 8, 4)},
        {"filename": "delivery_rate_kpi", "title": "Delivery Rate", "display": "scalar",
         "columns": "delivery_rate", "settings": rate_settings("delivery_rate"),
         "description": "Delivered orders as a percentage of shipped orders.",
         "layout": (4, 8, 8, 4)},
        {"filename": "changed_orders_kpi", "title": "Changed Orders", "display": "scalar",
         "columns": "changed_orders", "settings": count_settings("changed_orders"),
         "description": "Matched final orders with a quantity or value change.",
         "layout": (4, 16, 8, 4)},
        {"filename": "funnel", "title": "Order Lifecycle — Counts", "display": "bar",
         "columns": "stage_name, order_count", "order_by": "stage_order",
         "settings": {"graph.dimensions": ["stage_name"], "graph.metrics": ["order_count"],
                      "graph.show_values": True, "graph.legend_type": "none",
                      "column_settings": field_format("order_count", decimals=0)},
         "description": "Count-only lifecycle view; rates and exceptions are presented separately.",
         "layout": (8, 0, 24, 8),
         "aliases": ("Funnel — intent through delivery/return",
                     "Funnel \ufffd intent through delivery/return")},
        {"filename": "native_revenue", "title": "Revenue & COD Fee — Native Currency",
         "display": "bar", "columns": "currency, collected_revenue, cod_fee",
         "settings": {"column_settings": {
                          **field_format("collected_revenue", decimals=2),
                          **field_format("cod_fee", decimals=2)},
                      "graph.dimensions": ["currency"],
                      "graph.metrics": ["collected_revenue", "cod_fee"],
                      "graph.show_values": True, "graph.legend_type": "compact"},
         "description": "Collected revenue and COD fee by native currency; currencies are never summed.",
         "layout": (16, 0, 12, 8),
         "aliases": ("Revenue / Collection by Native Currency",)},
        {"filename": "usd_costs", "title": "USD Operational Cost Breakdown", "display": "bar",
         "columns": "cost_category, amount_usd", "order_by": "cost_order",
         "settings": {"column_settings": field_format(
                          "amount_usd", decimals=2, style="currency", currency="USD"),
                      "graph.dimensions": ["cost_category"],
                      "graph.metrics": ["amount_usd"], "graph.show_values": True,
                      "graph.legend_type": "none"},
         "description": "Explicit USD cost categories; Known Operational Cost is the reported total.",
         "layout": (16, 12, 12, 8), "aliases": ("Operational Costs in USD",)},
        {"filename": "order_changes", "title": "Initial vs Final Order Changes", "display": "table",
         "columns": "quantity_change, currency, order_count, quantity_changed_orders, "
                    "value_changed_orders, initial_value, final_value",
         "settings": {"column_settings": {
             **field_format("order_count", decimals=0),
             **field_format("quantity_changed_orders", decimals=0),
             **field_format("value_changed_orders", decimals=0),
             **field_format("initial_value", decimals=2),
             **field_format("final_value", decimals=2),
         }},
         "description": "Readable initial-to-final quantity and native-currency value changes.",
         "layout": (24, 0, 14, 9)},
        {"filename": "identity_data_quality", "title": "Identity & Data Quality", "display": "table",
         "columns": "identity_status, record_count, disposition", "order_by": "display_order",
         "settings": {"column_settings": field_format("record_count", decimals=0)},
         "description": "Aggregate identity outcomes and cohort exclusions; no customer-level data or PII.",
         "layout": (24, 14, 10, 9)},
        {"filename": "economic_completeness", "title": "FX REQUIRED — Economic Completeness",
         "display": "table", "columns": "currency, economic_status, economic_limitation",
         "settings": {},
         "description": "Cross-currency contribution is intentionally unavailable without trusted FX.",
         "layout": (33, 0, 24, 5),
         "aliases": ("Economic Completeness / FX Limitation",)},
    )

    managed_cards = []
    stale_card_ids: set[int] = set()
    for spec in specs:
        filename = spec["filename"]
        title = spec["title"]
        base = (Path(__file__).parent / "sama_pilot_queries" / f"{filename}.sql").read_text(
            encoding="utf-8"
        ).rstrip(";\n")
        query_sql = (f"SELECT {spec['columns']} FROM (\n" + base
                     + "\n) AS pilot_card\nWHERE 1=1\n[[AND business_id = {{business}}]]")
        if spec.get("order_by"):
            query_sql += f"\nORDER BY {spec['order_by']}"
        tags = {"business": {"id": "business", "name": "business", "display-name": "Business",
                             "type": "text", "required": False}}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            detail = result.get("error", result.get("status"))
            raise RuntimeError(f"SAMA pilot BI query {filename} failed: {detail}")
        payload = {"name": title, "description": spec["description"],
                   "collection_id": collection_id, "display": spec["display"],
                   "dataset_query": query, "visualization_settings": spec["settings"]}
        recognized_titles = {title, *spec.get("aliases", ())}
        matches = [item for item in items
                   if item.get("model") == "card" and item.get("name") in recognized_titles]
        canonical = [item for item in matches if item.get("name") == title]
        aliases = [item for item in matches if item.get("name") != title]
        if len(canonical) > 1 or (not canonical and len(aliases) > 1):
            raise RuntimeError(f"Multiple managed SAMA pilot cards match {title!r}.")
        existing = canonical[0] if canonical else aliases[0] if aliases else None
        if canonical:
            stale_card_ids.update(int(item["id"]) for item in aliases)
        card = (api("PUT", f"/api/card/{existing['id']}", payload)
                if existing else api("POST", "/api/card", payload))
        managed_cards.append((card, spec))

    title = "Pulse — Real COD Pilot"
    recognized_titles = {title, "Pulse \ufffd Real COD Pilot"}
    matches = [item for item in items
               if item.get("model") == "dashboard" and item.get("name") in recognized_titles]
    if len(matches) > 1:
        raise RuntimeError(f"Multiple managed SAMA pilot dashboards match {title!r}.")
    dashboard = matches[0] if matches else None
    dashboard_description = (
        "Executive view of the validated Lightfunnels + COD Network pilot."
    )
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {
            "name": title, "collection_id": collection_id,
            "description": dashboard_description,
        })
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    dashcards = [item for item in dashboard.get("dashcards", [])
                 if item.get("card_id") not in stale_card_ids]
    for index, (card, spec) in enumerate(managed_cards):
        mappings = [{"parameter_id": "business", "card_id": card["id"],
                     "target": ["variable", ["template-tag", "business"]]}]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        row, col, size_x, size_y = spec["layout"]
        layout = {"row": row, "col": col, "size_x": size_x, "size_y": size_y,
                  "parameter_mappings": mappings, "visualization_settings": {}}
        if existing:
            existing.update(layout)
        else:
            dashcards.append({"id": -(600 + index), "card_id": card["id"], **layout})

    parameter = {"id": "business", "name": "Business", "slug": "business", "type": "string/="}
    others = [item for item in dashboard.get("parameters", []) if item["id"] != "business"]
    api("PUT", f"/api/dashboard/{dashboard['id']}", {
        "name": title, "description": dashboard_description,
        "dashcards": dashcards, "parameters": [parameter, *others],
    })
    print(f"Real COD pilot dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")

def _ensure_sama_tiktok_dashboard(session_id: str, database_id: int) -> None:
    """Create the separate aggregate-only real TikTok performance dashboard."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    def field_format(name: str, *, decimals: int, style: str | None = None,
                     currency: str | None = None) -> dict[str, Any]:
        formatting: dict[str, Any] = {"decimals": decimals}
        if style is not None:
            formatting["number_style"] = style
        if currency is not None:
            formatting.update({"currency": currency, "currency_style": "symbol",
                               "currency_in_header": True})
        return {json.dumps(["name", name], separators=(",", ":")): formatting}

    def scalar_settings(field: str, *, money: bool = False) -> dict[str, Any]:
        return {
            "scalar.field": field,
            "column_settings": field_format(
                field, decimals=2 if money else 0,
                style="currency" if money else None,
                currency="USD" if money else None,
            ),
        }

    available = api("POST", "/api/dataset", {
        "database": database_id, "type": "native", "parameters": [],
        "native": {
            "query": "SELECT to_regclass('marts.sama_pilot_tiktok_campaign_outcomes') IS NOT NULL",
            "template-tags": {},
        },
    })
    rows = available.get("data", {}).get("rows", []) if available.get("status") == "completed" else []
    if not rows or not rows[0][0]:
        print("SAMA TikTok marts are not loaded; skipping the optional TikTok dashboard.")
        return

    collection = _unique(api("GET", "/api/collection"), "Pulse Real TikTok Performance")
    if collection is None:
        collection = api("POST", "/api/collection", {"name": "Pulse Real TikTok Performance"})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])
    count_format = field_format("stage_count", decimals=0)
    money_format = {
        **field_format("spend_usd", decimals=2, style="currency", currency="USD"),
        **field_format("cost_per_order_usd", decimals=2, style="currency", currency="USD"),
        **field_format("cost_per_confirmed_usd", decimals=2, style="currency", currency="USD"),
        **field_format("cost_per_delivered_usd", decimals=2, style="currency", currency="USD"),
        **field_format("platform_cpa_usd", decimals=2, style="currency", currency="USD"),
    }
    rate_format = {
        **field_format("delivery_rate", decimals=2, style="percent"),
        **field_format("return_rate", decimals=2, style="percent"),
        **field_format("destination_ctr", decimals=2, style="percent"),
        **field_format("hook_rate", decimals=2, style="percent"),
        **field_format("hold_rate", decimals=2, style="percent"),
    }
    specs = (
        {"filename": "target_spend_kpi", "title": "Target Campaign Spend", "display": "scalar",
         "columns": "target_campaign_spend_display",
         "settings": {"scalar.field": "target_campaign_spend_display"},
         "description": "TikTok spend for the 12 campaigns observed in the Lightfunnels funnel.",
         "layout": (0, 0, 4, 4)},
        {"filename": "platform_conversions_kpi", "title": "Platform Conversions",
         "display": "scalar", "columns": "platform_conversions",
         "settings": scalar_settings("platform_conversions"),
         "description": "TikTok-reported conversions; not labeled as observed orders.",
         "layout": (0, 4, 4, 4)},
        {"filename": "lightfunnels_orders_kpi", "title": "Lightfunnels Orders",
         "display": "scalar", "columns": "lightfunnels_orders",
         "settings": scalar_settings("lightfunnels_orders"),
         "description": "Observed initial orders attributed only at campaign level.",
         "layout": (0, 8, 4, 4)},
        {"filename": "confirmed_orders_kpi", "title": "Confirmed Orders", "display": "scalar",
         "columns": "confirmed_orders", "settings": scalar_settings("confirmed_orders"),
         "description": "Observed high-confidence campaign-level COD confirmations.",
         "layout": (0, 12, 4, 4)},
        {"filename": "delivered_orders_kpi", "title": "Delivered Orders", "display": "scalar",
         "columns": "delivered_orders", "settings": scalar_settings("delivered_orders"),
         "description": "Observed high-confidence campaign-level COD deliveries.",
         "layout": (0, 16, 4, 4)},
        {"filename": "cost_per_delivered_kpi", "title": "Cost / Delivered Order",
         "display": "scalar", "columns": "cost_per_delivered_order_usd",
         "settings": scalar_settings("cost_per_delivered_order_usd", money=True),
         "description": "Target TikTok USD spend divided by observed delivered-order count.",
         "layout": (0, 20, 4, 4)},
        {"filename": "funnel_comparison", "title": "Platform vs Observed Business Outcomes",
         "previous_title": "Platform vs Observed Business Funnel",
         "display": "bar", "columns": "stage_name, stage_count", "order_by": "stage_order",
         "settings": {"graph.dimensions": ["stage_name"], "graph.metrics": ["stage_count"],
                      "graph.show_values": True, "graph.legend_type": "none",
                      "column_settings": count_format},
         "description": "TikTok-reported conversions compared with observed downstream counts. "
                        "Delivered and Returned are sibling terminal outcomes of Shipped Orders; "
                        "neither occurs after the other.",
         "layout": (4, 0, 24, 8)},
        {"filename": "campaign_business_performance",
         "title": "Campaign-Level Observed Business Performance", "display": "table",
         "columns": "campaign_name, spend_usd, platform_conversions, initial_orders, "
                    "confirmed_orders, delivered_orders, returned_orders, cost_per_order_usd, "
                    "cost_per_confirmed_usd, cost_per_delivered_usd, delivery_rate, return_rate",
         "settings": {"column_settings": {**money_format, **rate_format}},
         "description": "Observed outcomes stop at campaign grain; no ad-group or ad allocation.",
         "layout": (12, 0, 24, 10)},
        {"filename": "native_performance", "title": "TikTok Native Ad Performance",
         "display": "table",
         "columns": "campaign_name, ad_group_name, ad_name, spend_usd, impressions, "
                    "destination_clicks, platform_conversions, checkouts, destination_ctr, "
                    "platform_cpa_usd, hook_rate, hold_rate",
         "settings": {"column_settings": {**money_format, **rate_format}},
         "description": "Native hierarchy metrics only; ratios are recalculated from additive components.",
         "layout": (22, 0, 24, 10)},
        {"filename": "attribution_data_quality", "title": "Attribution & Data Quality",
         "display": "table",
         "columns": "check_name, category, issue_count, observed_value, expected_value, status",
         "settings": {"column_settings": field_format("issue_count", decimals=0)},
         "description": "Source defects and observations, including outside-cohort and boundary cases.",
         "layout": (32, 0, 24, 8)},
        {"filename": "economic_limitation", "title": "FX REQUIRED — Economic Limitation",
         "display": "table",
         "columns": "marketing_spend_currency, cod_revenue_currency, economic_status, economic_limitation",
         "settings": {},
         "description": "No cross-currency business ROAS, contribution, or profit is calculated.",
         "layout": (40, 0, 24, 5)},
    )

    managed_cards = []
    for spec in specs:
        base = (Path(__file__).parent / "sama_tiktok_queries" / f"{spec['filename']}.sql").read_text(
            encoding="utf-8"
        ).rstrip(";\n")
        query_sql = (f"SELECT {spec['columns']} FROM (\n" + base
                     + "\n) AS tiktok_card\nWHERE 1=1\n[[AND business_id = {{business}}]]")
        if spec.get("order_by"):
            query_sql += f"\nORDER BY {spec['order_by']}"
        tags = {"business": {"id": "business", "name": "business", "display-name": "Business",
                             "type": "text", "required": False}}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            detail = result.get("error", result.get("status"))
            raise RuntimeError(f"SAMA TikTok BI query {spec['filename']} failed: {detail}")
        payload = {"name": spec["title"], "description": spec["description"],
                   "collection_id": collection_id, "display": spec["display"],
                   "dataset_query": query, "visualization_settings": spec["settings"]}
        collection_cards = [item for item in items if item.get("model") == "card"]
        existing = _unique(collection_cards, spec["title"])
        if existing is None and spec.get("previous_title"):
            existing = _unique(collection_cards, spec["previous_title"])
        card = (api("PUT", f"/api/card/{existing['id']}", payload)
                if existing else api("POST", "/api/card", payload))
        managed_cards.append((card, spec))

    title = "Pulse — Real TikTok Performance"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    description = "TikTok-reported performance and campaign-only observed COD outcomes."
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {
            "name": title, "collection_id": collection_id, "description": description,
        })
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    dashcards = dashboard.get("dashcards", [])
    for index, (card, spec) in enumerate(managed_cards):
        mappings = [{"parameter_id": "business", "card_id": card["id"],
                     "target": ["variable", ["template-tag", "business"]]}]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        row, col, size_x, size_y = spec["layout"]
        layout = {"row": row, "col": col, "size_x": size_x, "size_y": size_y,
                  "parameter_mappings": mappings, "visualization_settings": {}}
        if existing:
            existing.update(layout)
        else:
            dashcards.append({"id": -(700 + index), "card_id": card["id"], **layout})
    parameter = {"id": "business", "name": "Business", "slug": "business", "type": "string/="}
    others = [item for item in dashboard.get("parameters", []) if item["id"] != "business"]
    api("PUT", f"/api/dashboard/{dashboard['id']}", {
        "name": title, "description": description,
        "dashcards": dashcards, "parameters": [parameter, *others],
    })
    print(f"Real TikTok performance dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def _ensure_sama_unified_dashboard(session_id: str, database_id: int) -> None:
    """Create the aggregate-only Phase 6.5C executive operating view."""
    def api(method: str, path: str, payload=None):
        return _request(method, path, payload, session_id)

    def field_format(name: str, *, decimals: int, style: str | None = None,
                     currency: str | None = None) -> dict[str, Any]:
        formatting: dict[str, Any] = {"decimals": decimals}
        if style is not None:
            formatting["number_style"] = style
        if currency is not None:
            formatting.update({"currency": currency, "currency_style": "symbol",
                               "currency_in_header": True})
        return {json.dumps(["name", name], separators=(",", ":")): formatting}

    def scalar_settings(field: str, *, money: bool = False,
                        percent: bool = False, exact: bool = False) -> dict[str, Any]:
        settings = {
            "scalar.field": field,
            "column_settings": field_format(
                field,
                decimals=2 if money or percent else 0,
                style="currency" if money else "percent" if percent else None,
                currency="USD" if money else None,
            ),
        }
        if exact:
            settings["scalar.compact_primary_number"] = False
        return settings

    available = api("POST", "/api/dataset", {
        "database": database_id, "type": "native", "parameters": [],
        "native": {
            "query": "SELECT to_regclass('marts.sama_pilot_unified_overview') IS NOT NULL",
            "template-tags": {},
        },
    })
    rows = available.get("data", {}).get("rows", []) if available.get("status") == "completed" else []
    if not rows or not rows[0][0]:
        print("SAMA unified marts are not loaded; skipping the optional unified dashboard.")
        return

    collection_name = "Pulse Unified Business Overview"
    collection = _unique(api("GET", "/api/collection"), collection_name)
    if collection is None:
        collection = api("POST", "/api/collection", {"name": collection_name})
    collection_id = collection["id"]
    items = api("GET", f"/api/collection/{collection_id}/items").get("data", [])

    money_fields = (
        "target_spend_usd", "marketing_cost_per_delivered_order_usd",
        "known_usd_cost_per_delivered_order", "cost_per_lightfunnels_order_usd",
        "cost_per_confirmed_order_usd", "amount_usd", "total_known_usd_cost",
        "spend_usd", "cost_per_order_usd", "cost_per_confirmed_usd",
        "cost_per_delivered_usd",
    )
    money_format = {}
    for field in money_fields:
        money_format.update(field_format(field, decimals=2, style="currency", currency="USD"))
    rate_format = {}
    for field in ("confirmation_rate", "delivery_rate", "return_rate"):
        rate_format.update(field_format(field, decimals=2, style="percent"))
    count_format = {}
    for field in (
        "stage_count", "lightfunnels_orders", "confirmed_orders", "delivered_orders",
        "returned_orders", "platform_conversions", "issue_count",
    ):
        count_format.update(field_format(field, decimals=0))
    native_format = {
        **field_format("delivered_orders", decimals=0),
        **field_format("collected_native_currency", decimals=2),
        **field_format("cod_fee_native_currency", decimals=2),
        **field_format("cash_after_cod_fee_native_currency", decimals=2),
    }

    specs = (
        {"filename": "target_spend_kpi", "title": "Target Spend", "display": "scalar",
         "columns": "target_spend_usd",
         "settings": scalar_settings("target_spend_usd", money=True, exact=True),
         "description": "TikTok Marketing Spend in USD for the 12 linked target campaigns.",
         "layout": (0, 0, 3, 4)},
        {"filename": "lightfunnels_orders_kpi", "title": "Lightfunnels Orders", "display": "scalar",
         "columns": "lightfunnels_orders", "settings": scalar_settings("lightfunnels_orders"),
         "description": "Observed acquisition intents attributed to the target TikTok cohort.",
         "layout": (0, 3, 3, 4)},
        {"filename": "confirmed_orders_kpi", "title": "Confirmed Orders", "display": "scalar",
         "columns": "confirmed_orders", "settings": scalar_settings("confirmed_orders"),
         "description": "High-confidence matched orders confirmed for COD fulfillment.",
         "layout": (0, 6, 3, 4)},
        {"filename": "delivered_orders_kpi", "title": "Delivered Orders", "display": "scalar",
         "columns": "delivered_orders", "settings": scalar_settings("delivered_orders"),
         "description": "Delivered terminal outcomes for the acquisition cohort.",
         "layout": (0, 9, 3, 4)},
        {"filename": "return_rate_kpi", "title": "Return Rate", "display": "scalar",
         "columns": "return_rate", "settings": scalar_settings("return_rate", percent=True),
         "description": "Returned Orders divided by Shipped Orders.",
         "layout": (0, 12, 4, 4)},
        {"filename": "marketing_cost_delivered_kpi", "title": "Marketing Cost / Delivered",
         "display": "scalar", "columns": "marketing_cost_per_delivered_order_usd",
         "settings": scalar_settings("marketing_cost_per_delivered_order_usd", money=True),
         "description": "TikTok Marketing Spend divided by Delivered Orders.",
         "layout": (0, 16, 4, 4)},
        {"filename": "known_cost_delivered_kpi", "title": "Known USD Cost / Delivered",
         "display": "scalar", "columns": "known_usd_cost_per_delivered_order",
         "settings": scalar_settings("known_usd_cost_per_delivered_order", money=True),
         "description": "Known USD Costs divided by Delivered Orders; not total business cost.",
         "layout": (0, 20, 4, 4)},
        {"filename": "acquisition_outcomes", "title": "Acquisition to Observed Business Outcomes",
         "display": "bar", "columns": "stage_name, stage_count", "order_by": "stage_order",
         "settings": {"graph.dimensions": ["stage_name"], "graph.metrics": ["stage_count"],
                      "graph.show_values": True, "graph.legend_type": "none",
                      "column_settings": count_format},
         "description": "Platform Conversions are distinct from observed orders. Delivered and Returned "
                        "are sibling terminal outcomes of Shipped Orders, not sequential stages.",
         "layout": (4, 0, 24, 8)},
        {"filename": "efficiency_conversion", "title": "Confirmation Rate",
         "previous_title": "Efficiency & Conversion", "display": "scalar",
         "columns": "confirmation_rate",
         "settings": scalar_settings("confirmation_rate", percent=True),
         "description": "Confirmed Orders divided by high-confidence Matched Orders.",
         "layout": (12, 0, 4, 4)},
        {"filename": "efficiency_conversion", "title": "Delivery Rate", "display": "scalar",
         "columns": "delivery_rate", "settings": scalar_settings("delivery_rate", percent=True),
         "description": "Delivered Orders divided by Shipped Orders.",
         "layout": (12, 4, 4, 4)},
        {"filename": "efficiency_conversion", "title": "Efficiency Return Rate", "display": "scalar",
         "columns": "return_rate",
         "settings": {**scalar_settings("return_rate", percent=True), "card.title": "Return Rate"},
         "description": "Returned Orders divided by Shipped Orders.",
         "layout": (12, 8, 4, 4)},
        {"filename": "efficiency_conversion", "title": "Cost / Lightfunnels Order",
         "display": "scalar", "columns": "cost_per_lightfunnels_order_usd",
         "settings": scalar_settings("cost_per_lightfunnels_order_usd", money=True),
         "description": "Target TikTok Marketing Spend divided by Lightfunnels Orders.",
         "layout": (12, 12, 4, 4)},
        {"filename": "efficiency_conversion", "title": "Cost / Confirmed Order",
         "display": "scalar", "columns": "cost_per_confirmed_order_usd",
         "settings": scalar_settings("cost_per_confirmed_order_usd", money=True),
         "description": "Target TikTok Marketing Spend divided by Confirmed Orders.",
         "layout": (12, 16, 4, 4)},
        {"filename": "efficiency_conversion", "title": "Efficiency Marketing Cost / Delivered",
         "display": "scalar", "columns": "marketing_cost_per_delivered_order_usd",
         "settings": {**scalar_settings("marketing_cost_per_delivered_order_usd", money=True),
                      "card.title": "Marketing Cost / Delivered"},
         "description": "Target TikTok Marketing Spend divided by Delivered Orders.",
         "layout": (12, 20, 4, 4)},
        {"filename": "usd_cost_stack", "title": "Known USD Cost Stack", "display": "bar",
         "columns": "cost_category, amount_usd", "order_by": "cost_order",
         "settings": {"graph.dimensions": ["cost_category"], "graph.metrics": ["amount_usd"],
                      "graph.show_values": True, "graph.legend_type": "none",
                      "column_settings": money_format},
         "description": "Additive USD categories only: target marketing, Product COGS, Call Center, and Logistics.",
         "layout": (18, 0, 16, 8)},
        {"filename": "known_usd_cost_total", "title": "Known USD Cost Total", "display": "scalar",
         "columns": "total_known_usd_cost", "settings": scalar_settings("total_known_usd_cost", money=True),
         "description": "Target marketing plus known operational USD costs, shown separately from the stack.",
         "layout": (18, 16, 8, 8)},
        {"filename": "native_cash_collection", "title": "Native Currency — not converted",
         "display": "table",
         "columns": "currency, economic_status, delivered_orders, collected_native_currency, "
                    "cod_fee_native_currency, cash_after_cod_fee_native_currency",
         "settings": {"column_settings": native_format},
         "description": "Cash Collected and COD Fee remain separated by native currency; cash after COD fee is not profit.",
         "layout": (26, 0, 24, 8)},
        {"filename": "daily_target_spend", "title": "Daily Target TikTok Spend", "display": "line",
         "columns": "report_date, spend_usd",
         "settings": {"graph.dimensions": ["report_date"], "graph.metrics": ["spend_usd"],
                      "column_settings": money_format},
         "description": "Daily USD spend for linked target campaigns in UTC+1.",
         "layout": (34, 0, 12, 8)},
        {"filename": "daily_cohort_outcomes", "title": "Daily Acquisition-Cohort Outcomes",
         "display": "line",
         "columns": "report_date, lightfunnels_orders, confirmed_orders, delivered_orders, returned_orders",
         "settings": {"graph.dimensions": ["report_date"],
                      "graph.metrics": ["lightfunnels_orders", "confirmed_orders", "delivered_orders", "returned_orders"],
                      "column_settings": count_format},
         "description": "Final outcomes grouped by acquisition/Lightfunnels intent date, not outcome event date.",
         "layout": (34, 12, 12, 8)},
        {"filename": "campaign_decisions", "title": "Campaign Decision Table", "display": "table",
         "columns": "campaign_name, spend_usd, platform_conversions, lightfunnels_orders, "
                    "confirmed_orders, delivered_orders, returned_orders, cost_per_order_usd, "
                    "cost_per_confirmed_usd, cost_per_delivered_usd, confirmation_rate, delivery_rate, return_rate",
         "settings": {"column_settings": {**money_format, **rate_format, **count_format}},
         "description": "Target campaigns only. Observed business outcomes remain at Campaign level.",
         "layout": (42, 0, 24, 10)},
        {"filename": "measurement_limitations", "title": "Measurement & Data Limitations",
         "display": "table", "columns": "measurement_limitation, issue_count",
         "settings": {"column_settings": count_format},
         "description": "Decision-relevant observations only; inactive Ad-Day detail remains outside this executive view.",
         "layout": (52, 0, 24, 7)},
    )

    managed_cards = []
    collection_cards = [item for item in items if item.get("model") == "card"]
    for spec in specs:
        base = (Path(__file__).parent / "sama_unified_queries" / f"{spec['filename']}.sql").read_text(
            encoding="utf-8"
        ).rstrip(";\n")
        query_sql = (f"SELECT {spec['columns']} FROM (\n" + base
                     + "\n) AS unified_card\nWHERE 1=1\n[[AND business_id = {{business}}]]")
        if spec.get("order_by"):
            query_sql += f"\nORDER BY {spec['order_by']}"
        tags = {"business": {"id": "business", "name": "business", "display-name": "Business",
                             "type": "text", "required": False}}
        query = {"database": database_id, "type": "native",
                 "native": {"query": query_sql, "template-tags": tags}}
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            detail = result.get("error", result.get("status"))
            raise RuntimeError(f"SAMA unified BI query {spec['filename']} failed: {detail}")
        payload = {"name": spec["title"], "description": spec["description"],
                   "collection_id": collection_id, "display": spec["display"],
                   "dataset_query": query, "visualization_settings": spec["settings"]}
        existing = _unique(collection_cards, spec["title"])
        if existing is None and spec.get("previous_title"):
            existing = _unique(collection_cards, spec["previous_title"])
        card = (api("PUT", f"/api/card/{existing['id']}", payload)
                if existing else api("POST", "/api/card", payload))
        managed_cards.append((card, spec))

    title = "Pulse — Unified Business Overview"
    dashboard = _unique([item for item in items if item.get("model") == "dashboard"], title)
    description = (
        "Primary executive operating view for the target TikTok-to-Lightfunnels acquisition cohort."
    )
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {
            "name": title, "collection_id": collection_id, "description": description,
        })
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    economic_card = _unique(collection_cards, "FX_REQUIRED — Economic Completeness")
    dashcards = [
        item for item in dashboard.get("dashcards", [])
        if economic_card is None or item.get("card_id") != economic_card["id"]
    ]
    for index, (card, spec) in enumerate(managed_cards):
        mappings = [{"parameter_id": "business", "card_id": card["id"],
                     "target": ["variable", ["template-tag", "business"]]}]
        existing = next((item for item in dashcards if item.get("card_id") == card["id"]), None)
        row, col, size_x, size_y = spec["layout"]
        layout = {"row": row, "col": col, "size_x": size_x, "size_y": size_y,
                  "parameter_mappings": mappings, "visualization_settings": {}}
        if existing:
            existing.update(layout)
        else:
            dashcards.append({"id": -(800 + index), "card_id": card["id"], **layout})

    economic_text = (
        "## FX_REQUIRED\n\n"
        "Marketing and known operating costs are USD.  \n"
        "COD collections are retained in native currencies.  \n"
        "Cross-currency profit, contribution, margin and business ROAS are unavailable "
        "until a trusted FX source is supplied."
    )
    text_settings = {
        "virtual_card": {
            "name": None,
            "display": "text",
            "visualization_settings": {},
            "dataset_query": {},
            "archived": False,
        },
        "text": economic_text,
    }
    existing_text = next((
        item for item in dashcards
        if item.get("card_id") is None
        and str(item.get("visualization_settings", {}).get("text", "")).startswith(
            "## FX_REQUIRED"
        )
    ), None)
    text_layout = {
        "row": 59, "col": 0, "size_x": 24, "size_y": 5,
        "parameter_mappings": [], "visualization_settings": text_settings,
    }
    if existing_text:
        existing_text.update(text_layout)
    else:
        dashcards.append({"id": -900, "card_id": None, **text_layout})
    parameter = {"id": "business", "name": "Business", "slug": "business", "type": "string/="}
    others = [item for item in dashboard.get("parameters", []) if item["id"] != "business"]
    api("PUT", f"/api/dashboard/{dashboard['id']}", {
        "name": title, "description": description,
        "dashcards": dashcards, "parameters": [parameter, *others],
    })
    print(f"Unified business dashboard ready: {METABASE_URL}/dashboard/{dashboard['id']}")


def main() -> int:
    properties = _request("GET", "/api/session/properties")
    setup_token = properties.get("setup-token")
    if setup_token:
        _complete_initial_setup(str(setup_token))
    session_id = _login()
    database_id = _ensure_warehouse(session_id)
    _verify_marts(session_id, database_id)
    _ensure_dashboard(session_id, database_id)
    _ensure_marketing_dashboard(session_id, database_id)
    _ensure_operations_dashboard(session_id, database_id)
    _ensure_economics_dashboard(session_id, database_id)
    _ensure_olist_dashboard(session_id, database_id)
    _ensure_uci_dashboard(session_id, database_id)
    _ensure_sama_pilot_dashboard(session_id, database_id)
    _ensure_sama_tiktok_dashboard(session_id, database_id)
    _ensure_sama_unified_dashboard(session_id, database_id)
    if __package__:
        from .monitoring_dashboard import ensure_dashboard
    else:
        from monitoring_dashboard import ensure_dashboard
    dashboard_id = ensure_dashboard(
        lambda method, path, payload=None: _request(method, path, payload, session_id),
        _unique, database_id)
    print(f"Platform health dashboard ready: {METABASE_URL}/dashboard/{dashboard_id}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Metabase setup failed: {error}", file=sys.stderr)
        raise SystemExit(1)
