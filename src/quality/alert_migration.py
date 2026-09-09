"""Backfill Phase 5.5 events without deleting their IDs or source evidence."""

from psycopg.types.json import Jsonb

from src.quality.alert_service import _audit, incident_key


def migrate_alerts(cursor):
    cursor.execute("""SELECT a.alert_event_id,a.source_type,a.dataset_name,a.layer,a.details,
        a.created_at_utc,a.source_id,r.metric_name,r.dimensions
        FROM monitoring.alert_events a LEFT JOIN monitoring.anomaly_results r
        ON a.source_type='ANOMALY' AND a.source_id=r.anomaly_id
        WHERE a.incident_key IS NULL ORDER BY a.created_at_utc,a.alert_event_id""")
    for identity, source, dataset, layer, details, created, source_id, metric, dimensions in cursor.fetchall():
        # Orphaned legacy anomaly sources remain isolated rather than merging
        # unrelated unknown metrics. Their full evidence is retained.
        metric = details.get("metric_name") or metric or f"legacy:{source_id}"
        check = details.get("check_name") or f"legacy:{source_id}"
        key = incident_key(source, dataset, layer, metric, check_name=check,
                           dimensions=dimensions or details.get("dimensions", {}))
        cursor.execute("SELECT alert_event_id FROM monitoring.alert_events WHERE incident_key=%s "
                       "AND status IN ('OPEN','ACKNOWLEDGED')", (key,))
        active = cursor.fetchone()
        cursor.execute("UPDATE monitoring.alert_events SET incident_key=%s,first_seen_at_utc=%s,"
                       "last_seen_at_utc=%s WHERE alert_event_id=%s", (key, created, created, identity))
        _audit(cursor, identity, None, "OPEN", "system:phase56-migration", "Imported Phase 5.5 alert")
        if active:
            # Preserve every old row. The oldest becomes the active representative.
            note = f"Consolidated legacy duplicate into active alert {active[0]}"
            cursor.execute("UPDATE monitoring.alert_events SET status='RESOLVED',"
                           "resolved_at_utc=clock_timestamp(),resolved_by='system:phase56-migration',"
                           "resolution_note=%s WHERE alert_event_id=%s", (note, identity))
            _audit(cursor, identity, "OPEN", "RESOLVED", "system:phase56-migration", note)
            cursor.execute("UPDATE monitoring.alert_events SET occurrence_count=occurrence_count+1,"
                           "last_seen_at_utc=GREATEST(last_seen_at_utc,%s),"
                           "severity=CASE WHEN severity='CRITICAL' OR "
                           "(SELECT severity FROM monitoring.alert_events WHERE alert_event_id=%s)='CRITICAL' "
                           "THEN 'CRITICAL' ELSE 'WARNING' END WHERE alert_event_id=%s",
                           (created, identity, active[0]))
        cursor.execute("""INSERT INTO monitoring.alert_occurrences
            (occurrence_id,alert_event_id,incident_key,observed_at_utc,source_id,snapshot)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (identity, active[0] if active else identity, key, created, source_id, Jsonb(details)))
    cursor.execute("""ALTER TABLE monitoring.alert_events
        ALTER COLUMN incident_key SET NOT NULL,
        ALTER COLUMN first_seen_at_utc SET NOT NULL,
        ALTER COLUMN last_seen_at_utc SET NOT NULL""")
    cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS alert_events_active_incident_idx
        ON monitoring.alert_events(incident_key) WHERE status IN ('OPEN','ACKNOWLEDGED')""")
