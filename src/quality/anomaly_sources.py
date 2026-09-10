"""Read candidate metric series from monitoring history and analytics tables."""

from collections import defaultdict
from datetime import datetime, time, timezone

import psycopg

from src.quality.anomaly import MetricSeries
from src.warehouse.load_gold import connection_kwargs


def _series(rows):
    grouped = defaultdict(list)
    for dataset, layer, metric, dimensions, observed_at, value, tie_breaker, sample_size in rows:
        grouped[(dataset, layer, metric, tuple(sorted(dimensions.items())))].append(
            (observed_at, str(tie_breaker), float(value), sample_size))
    output = []
    for (dataset, layer, metric, dimensions), points in grouped.items():
        points.sort(key=lambda point: (point[0], point[1]))
        observed_at, _, current, current_sample_size = points[-1]
        output.append(MetricSeries(metric_name=metric, dataset_name=dataset, layer=layer,
                                   current_value=current,
                                   history=tuple(value for _, _, value, _ in points[:-1]),
                                   history_observed_at_utc=tuple(stamp for stamp, _, _, _ in points[:-1]),
                                   observed_at_utc=observed_at, dimensions=dict(dimensions),
                                   current_sample_size=current_sample_size,
                                   history_sample_sizes=tuple(size for _, _, _, size in points[:-1])))
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
        rows.extend((*row, None) for row in quality)
        sales = connection.execute("""SELECT business_id,event_date,sum(completed_orders)::double precision
          FROM analytics.daily_sales GROUP BY business_id,event_date ORDER BY business_id,event_date""").fetchall()
        rows.extend(("daily_sales", "analytics", "completed_order_volume", {"business_id": business_id},
                     datetime.combine(day, time.min, timezone.utc), value, day, None) for business_id, day, value in sales)
        revenue = connection.execute("""SELECT business_id,event_date,currency,sum(gross_revenue)::double precision
          FROM analytics.daily_sales GROUP BY business_id,event_date,currency ORDER BY business_id,event_date,currency""").fetchall()
        rows.extend(("daily_sales", "analytics", "gross_revenue", {"business_id": business_id, "currency": currency},
                     datetime.combine(day, time.min, timezone.utc), value, day, None)
                    for business_id, day, currency, value in revenue)
        rates = (("view_to_cart_rate", 0), ("cart_to_checkout_rate", 1),
                 ("checkout_to_order_rate", 2), ("order_to_payment_rate", 3))
        funnel = connection.execute("""SELECT business_id,event_date,country,view_to_cart_rate,cart_to_checkout_rate,
          checkout_to_order_rate,order_to_payment_rate,product_views,cart_adds,
          checkouts_started,orders_created FROM analytics.funnel_metrics ORDER BY business_id,event_date,country""").fetchall()
        for business_id, day, country, *values in funnel:
            rate_values, denominators = values[:4], values[4:]
            for metric, index in rates:
                value, sample_size = rate_values[index], denominators[index]
                if value is not None:
                    rows.append(("funnel_metrics", "analytics", metric,
                                 {"business_id": business_id, "country": country},
                                 datetime.combine(day, time.min, timezone.utc), value, day, sample_size))
        marketing_exists = connection.execute(
            "SELECT to_regclass('analytics.marketing_daily') IS NOT NULL"
        ).fetchone()[0]
        if marketing_exists:
            marketing = connection.execute("""SELECT business_id,source_type,source_id,platform,account_id,
              report_date,reporting_timezone,currency,spend,impressions,clicks,platform_conversions,ctr,cpa
              FROM analytics.marketing_daily
              ORDER BY business_id,source_type,source_id,platform,account_id,report_date,currency""").fetchall()
            for (business_id, source_type, source_id, platform, account_id, day,
                 reporting_timezone, currency, spend, impressions, clicks,
                 conversions, ctr, cpa) in marketing:
                dimensions = {"business_id": business_id, "source_type": source_type,
                              "source_id": source_id, "platform": platform,
                              "account_id": account_id, "currency": currency,
                              "reporting_timezone": reporting_timezone}
                stamp = datetime.combine(day, time.min, timezone.utc)
                for metric, value, sample_size in (
                    ("daily_spend", spend, None), ("impressions", impressions, None),
                    ("clicks", clicks, None), ("platform_conversions", conversions, None),
                    ("ctr", ctr, impressions), ("cpa", cpa, None),
                ):
                    if value is not None:
                        rows.append(("marketing_daily", "analytics", metric, dimensions,
                                     stamp, value, day, sample_size))
        operations_exists = connection.execute(
            "SELECT to_regclass('analytics.order_operations_daily') IS NOT NULL"
        ).fetchone()[0]
        if operations_exists:
            daily = connection.execute("""SELECT business_id,event_date,reporting_timezone,currency,payment_type,
              shipped_orders,delivered_orders FROM analytics.order_operations_daily
              ORDER BY business_id,event_date,currency,payment_type""").fetchall()
            for business_id, day, reporting_timezone, currency, payment_type, shipped, delivered in daily:
                dimensions = {"business_id": business_id, "currency": currency,
                              "payment_type": payment_type, "reporting_timezone": reporting_timezone}
                stamp = datetime.combine(day, time.min, timezone.utc)
                rows.extend(("order_operations_daily", "analytics", metric, dimensions, stamp, value, day, None)
                            for metric, value in (("daily_shipped_volume", shipped),
                                                  ("daily_delivered_volume", delivered)))
            confirmation = connection.execute("""SELECT business_id,cohort_date,provider,currency,payment_type,
              confirmation_rate,eligible_orders FROM analytics.confirmation_performance
              ORDER BY business_id,cohort_date,provider,currency,payment_type""").fetchall()
            for business_id, day, provider, currency, payment_type, value, sample_size in confirmation:
                if value is not None:
                    rows.append(("confirmation_performance", "analytics", "confirmation_rate",
                        {"business_id": business_id, "provider": provider, "currency": currency,
                         "payment_type": payment_type}, datetime.combine(day, time.min, timezone.utc),
                        value, day, sample_size))
            delivery = connection.execute("""SELECT business_id,cohort_date,courier,currency,payment_type,
              delivery_rate,refusal_rate,return_rate,shipped_orders,delivered_orders
              FROM analytics.delivery_performance
              ORDER BY business_id,cohort_date,courier,currency,payment_type""").fetchall()
            for business_id, day, courier, currency, payment_type, delivery_rate, refusal_rate, return_rate, shipped, delivered in delivery:
                dimensions = {"business_id": business_id, "courier": courier,
                              "currency": currency, "payment_type": payment_type}
                stamp = datetime.combine(day, time.min, timezone.utc)
                for metric, value, sample_size in (("delivery_rate", delivery_rate, shipped),
                                                    ("refusal_rate", refusal_rate, shipped),
                                                    ("return_rate", return_rate, delivered)):
                    if value is not None:
                        rows.append(("delivery_performance", "analytics", metric, dimensions,
                                     stamp, value, day, sample_size))
            pending = connection.execute("""SELECT business_id,period_end,provider,currency,
              sum(remittance_pending_amount)::double precision
              FROM analytics.remittance_performance
              GROUP BY business_id,period_end,provider,currency
              ORDER BY business_id,period_end,provider,currency""").fetchall()
            rows.extend(("remittance_performance", "analytics", "pending_remittance_amount",
                         {"business_id": business_id, "provider": provider, "currency": currency},
                         datetime.combine(day, time.min, timezone.utc), value, day, None)
                        for business_id, day, provider, currency, value in pending)
    return _series(rows)
