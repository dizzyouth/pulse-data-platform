select business_id, check_name, category, issue_count
from {{ source('analytics', 'sama_pilot_data_quality') }}
