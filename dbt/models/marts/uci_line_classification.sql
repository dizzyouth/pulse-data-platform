select
    business_id,
    line_classification,
    currency,
    line_count,
    signed_line_value
from {{ source('analytics', 'uci_line_classification') }}
