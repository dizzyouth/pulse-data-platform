select
    business_id,
    platform,
    report_date,
    reporting_timezone,
    currency,
    sum(spend) as spend,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(platform_conversions) as platform_conversions,
    sum(platform_conversion_value) as platform_conversion_value,
    sum(clicks)::double precision / nullif(sum(impressions), 0) as ctr,
    sum(spend) / nullif(sum(clicks), 0) as cpc,
    sum(spend) * 1000.0 / nullif(sum(impressions), 0) as cpm,
    sum(spend) / nullif(sum(platform_conversions), 0) as cpa,
    sum(platform_conversion_value) / nullif(sum(spend), 0) as platform_roas
from {{ source('analytics', 'marketing_daily') }}
group by business_id, platform, report_date, reporting_timezone, currency
