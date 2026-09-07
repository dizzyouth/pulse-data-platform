"""Provision operational questions over read-only warehouse views.

Native queries never mutate history. Initialization belongs to src.warehouse.monitoring.
"""

from pathlib import Path
import re


TITLE = "Pulse Platform Health"
COLLECTION = "Pulse Monitoring"
SPECS = (
    ("current_health", "Current quality health by layer", "table", {}),
    ("latest_quality_status", "Latest quality by dataset", "table", {}),
    ("quality_trend", "Quality run trend (UTC)", "line",
     {"graph.dimensions": ["completed_date_utc", "overall_status"], "graph.metrics": ["quality_runs"]}),
    ("status_distribution", "Quality run status distribution", "bar",
     {"graph.dimensions": ["overall_status"], "graph.metrics": ["quality_runs"]}),
    ("recent_warnings", "Recent warning checks (latest 100)", "table", {}),
    ("recent_critical_failures", "Recent failed CRITICAL checks (latest 100)", "table", {}),
    ("failing_checks", "Most frequent warning / failing checks (top 20)", "table", {}),
    ("runs_by_layer", "Quality runs and incident counts by layer", "table", {}),
)
FILTERS = {"layer": "Layer", "dataset": "Dataset", "status": "Run status",
           "start_date": "Start date (UTC)", "end_date": "End date (UTC)"}


def question_query(filename, database_id):
    query = (Path(__file__).parent / "monitoring_queries" / f"{filename}.sql").read_text(encoding="utf-8")
    names = list(dict.fromkeys(re.findall(r"\{\{(\w+)\}\}", query)))
    tags = {name: {"id": name, "name": name, "display-name": FILTERS[name],
                   "type": "date" if name.endswith("date") else "text", "required": False}
            for name in names}
    return {"database": database_id, "type": "native", "native": {"query": query, "template-tags": tags}}


def ensure_dashboard(api, unique, database_id):
    """Reconcile only managed objects; retain unrelated cards and parameters."""
    # Verify the views before creating any monitoring metadata. Empty is valid.
    queries = []
    for filename, title, display, settings in SPECS:
        query = question_query(filename, database_id)
        result = api("POST", "/api/dataset", {**query, "parameters": []})
        if result.get("status") != "completed":
            raise RuntimeError(f"Monitoring question {filename} failed; run "
                               "python -m src.warehouse.monitoring and check warehouse access")
        print(f"Verified monitoring {filename}: {len(result['data']['rows'])} rows")
        queries.append((title, display, settings, query))
    collection = unique(api("GET", "/api/collection"), COLLECTION)
    if collection is None:
        collection = api("POST", "/api/collection", {"name": COLLECTION})
    items = api("GET", f"/api/collection/{collection['id']}/items").get("data", [])
    cards = []
    for title, display, settings, query in queries:
        payload = {"name": title, "collection_id": collection["id"], "display": display,
                   "dataset_query": query, "visualization_settings": settings}
        existing = unique([i for i in items if i.get("model") == "card"], title)
        card = api("PUT", f"/api/card/{existing['id']}", payload) if existing else api("POST", "/api/card", payload)
        cards.append((card, query["native"]["template-tags"]))
    dashboard = unique([i for i in items if i.get("model") == "dashboard"], TITLE)
    if dashboard is None:
        dashboard = api("POST", "/api/dashboard", {"name": TITLE, "collection_id": collection["id"],
                        "description": "Persisted quality history, all execution sources and attempts. "
                        "UNKNOWN means missing dataset history. Dates and Run status filter historical cards only. "
                        "No date selection means all history; incident tables show the latest 100 matches."})
    dashboard = api("GET", f"/api/dashboard/{dashboard['id']}")
    parameters = [{"id": name, "name": label, "slug": name,
                   "type": "date/single" if name.endswith("date") else "string/="}
                  for name, label in FILTERS.items()]
    dashcards = dashboard.get("dashcards", [])
    for index, (card, tags) in enumerate(cards):
        mappings = [{"parameter_id": name, "card_id": card["id"],
                     "target": ["variable", ["template-tag", name]]} for name in tags]
        existing = next((d for d in dashcards if d.get("card_id") == card["id"]), None)
        if existing:
            existing["parameter_mappings"] = mappings
        else:
            dashcards.append({"id": -(index + 1), "card_id": card["id"], "row": (index // 2) * 9,
                              "col": (index % 2) * 12, "size_x": 12, "size_y": 9,
                              "parameter_mappings": mappings, "visualization_settings": {}})
    parameters.extend(p for p in dashboard.get("parameters", []) if p["id"] not in FILTERS)
    api("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": dashcards, "parameters": parameters})
    return dashboard["id"]
