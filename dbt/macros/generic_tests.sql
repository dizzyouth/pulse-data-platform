{% test nonnegative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}

{% test contribution_formula(model) %}
select *
from {{ model }}
where cogs_complete
  and (
    contribution_before_marketing is null
    or abs(contribution_before_marketing
           - (recognized_economic_value - variable_operational_cost)) > 0.00000001
  )
{% endtest %}

{% test unique_combination(model, column_names) %}
select {{ column_names | join(', ') }}
from {{ model }}
group by {{ column_names | join(', ') }}
having count(*) > 1
{% endtest %}

{% test between_zero_and_one(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 1)
{% endtest %}

{% test uci_daily_signed_reconciliation(model) %}
select *
from {{ model }}
where abs(net_ledger_value - (
    positive_merchandise_value + cancellation_value + adjustment_value + non_merchandise_value
)) > 0.000001
{% endtest %}

{% test uci_invoice_signed_reconciliation(model) %}
select *
from {{ model }}
where abs(net_ledger_value - (
    positive_merchandise_value + cancellation_value + adjustment_value + non_merchandise_value
)) > 0.000001
{% endtest %}
