select business_id, code, severity, classification, issue_count, sample_ids_json
from {{ source('analytics', 'olist_data_quality') }}
