{% test not_null_all_columns(model) %}
-- {% set relation = ref(model) %}
{% set cols = adapter.get_columns_in_relation(relation) %}

    select *
    from {{ model }}
    where
        {% for col in adapter.get_columns_in_relation(model) %}
            {{ col.name }} is null
            {% if not loop.last %} or {% endif %}
        {% endfor %}

{% endtest %}