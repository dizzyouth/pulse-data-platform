{% test nonnegative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
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
