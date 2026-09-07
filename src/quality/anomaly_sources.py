"""Read candidate metric series from monitoring history and analytics tables."""

from collections import defaultdict
from datetime import datetime, time, timezone

import psycopg

from src.quality.anomaly import MetricSeries
from src.warehouse.load_gold import connection_kwargs


def _series(rows):
    grouped = defaultdict(list)
    for dataset, layer, metric, dimensions, observed_at, value, tie_breaker in rows:
        grouped[(dataset, layer, metric, tuple(sorted(dimensions.items())))].append(
            (observed_at, str(tie_breaker), float(value)))
    output = []
    for (dataset, layer, metric, dimensions), points in grouped.items():
        points.sort(key=lambda point: (point[0], point[1]))
        observed_at, _, current = points[-1]
        output.append(MetricSeries(metric_name=metric, dataset_name=dataset, layer=layer,
                                   current_value=current, history=tuple(value for _, _, value in points[:-1]),
                                   observed_at_utc=observed_at, dimensions=dict(dimensions)))
    return output


def load_metric_series():
    """Return latest value plus prior logical observations for every supported series."""
    rows = []
    with psycopg.connect(**connection_kwargs()) as connection:
        connection.read_only = True
        logical = """WITH logical_runs AS (
          SELECT DISTINCT ON (execution_source,execution_id,coalesce(dag_id,''),coalesce(airflow_run_id,''),
            coalesce(task_id,''),map_index,dataset_name,layer) *
          FROM monitoring.quality_runs
          ORDER BY execution_source,execution_id,coalesce(dag_id,''),coalesce(airflow_run_id,''),
            coalesce(task_id,''),map_index,dataset_name,layer,attempt_number DESC,completed_at_utc DESC
        ) """
        quality = connection.execute(logical + """SELECT r.dataset_name,r.layer,'row_count','{}'::jsonb,
          q.checked_at_utc,(q.observed_value #>> '{}')::double precision,r.quality_run_id::text
          FROM logical_runs r JOIN monitoring.quality_results q USING(quality_run_id)
          WHERE q.metric_name='row_count' AND jsonb_typeof(q.observed_value)='number'
          UNION ALL SELECT dataset_name,layer,'warning_check_count','{}'::jsonb,completed_at_utc,warning_checks,
          quality_run_id::text
          FROM logical_runs UNION ALL SELECT dataset_name,layer,'failed_check_count','{}'::jsonb,
          completed_at_utc,failed_checks,quality_run_id::text FROM logical_runs""").fetchall()
        rows.extend(quality)
        sales = connection.execute("""SELECT event_date,sum(completed_orders)::double precision
          FROM analytics.daily_sales GROUP BY event_date ORDER BY event_date""").fetchall()
        rows.extend(("daily_sales", "analytics", "completed_order_volume", {},
                     datetime.combine(day, time.min, timezone.utc), value, day) for day, value in sales)
        revenue = connection.execute("""SELECT event_date,currency,sum(gross_revenue)::double precision
          FROM analytics.daily_sales GROUP BY event_date,currency ORDER BY event_date,currency""").fetchall()
        rows.extend(("daily_sales", "analytics", "gross_revenue", {"currency": currency},
                     datetime.combine(day, time.min, timezone.utc), value, day) for day, currency, value in revenue)
        rates = ("view_to_cart_rate", "cart_to_checkout_rate", "checkout_to_order_rate", "order_to_payment_rate")
        funnel = connection.execute("""SELECT event_date,country,view_to_cart_rate,cart_to_checkout_rate,
          checkout_to_order_rate,order_to_payment_rate FROM analytics.funnel_metrics ORDER BY event_date,country""").fetchall()
        for day, country, *values in funnel:
            for metric, value in zip(rates, values):
                if value is not None:
                    rows.append(("funnel_metrics", "analytics", metric, {"country": country},
                                 datetime.combine(day, time.min, timezone.utc), value, day))
    return _series(rows)
