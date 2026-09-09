"""Airflow-independent contract for the alert delivery sweep DAG."""

ALERT_OPERATIONS_DAG_ID = "pulse_alert_operations"
ALERT_OPERATIONS_SCHEDULE = "*/5 * * * *"
ALERT_OPERATIONS_TASK_ID = "alert_delivery_sweep"
