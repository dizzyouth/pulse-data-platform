select
    business_id,
    check_name,
    category,
    issue_count,
    observed_value,
    expected_value,
    status
from {{ source('analytics', 'sama_pilot_tiktok_data_quality') }}
