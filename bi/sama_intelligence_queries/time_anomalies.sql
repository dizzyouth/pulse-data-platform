with businesses as (
    select business_id from marts.sama_pilot_unified_overview
), latest as (
    select distinct on (business_id, metric_name)
        business_id,
        metric_name,
        status,
        severity,
        observed_value,
        expected_value,
        confidence,
        explanation,
        evaluated_at_utc
    from monitoring_views.anomaly_baseline_history
    where dataset_name = 'sama_pilot_unified_daily'
    order by business_id, metric_name, evaluated_at_utc desc
), anomalies as (
    select * from latest where status = 'ANOMALY'
)
select
    businesses.business_id,
    coalesce(anomalies.metric_name, 'No current anomaly detected') as metric,
    anomalies.severity,
    anomalies.observed_value,
    anomalies.expected_value as baseline_value,
    anomalies.confidence,
    anomalies.explanation
from businesses
left join anomalies using (business_id)
