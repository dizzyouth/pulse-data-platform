SELECT
    business_id,
    campaign_name,
    ad_group_name,
    ad_name,
    ROUND(SUM(spend_usd)::numeric, 2) AS spend_usd,
    SUM(impressions)::bigint AS impressions,
    SUM(destination_clicks)::bigint AS destination_clicks,
    SUM(platform_conversions)::bigint AS platform_conversions,
    SUM(checkouts)::bigint AS checkouts,
    SUM(destination_clicks)::double precision / NULLIF(SUM(impressions), 0) AS destination_ctr,
    SUM(spend_usd)::double precision / NULLIF(SUM(platform_conversions), 0) AS platform_cpa_usd,
    SUM(video_views)::double precision / NULLIF(SUM(impressions), 0) AS hook_rate,
    SUM(video_views_6s)::double precision / NULLIF(SUM(video_views), 0) AS hold_rate
FROM marts.sama_pilot_tiktok_native_performance
GROUP BY business_id, campaign_name, ad_group_name, ad_name
ORDER BY spend_usd DESC, campaign_name, ad_group_name, ad_name
