-- Can be used to set up automated tests, encapsulated logic which is reusable
-- Better to know basic JINJA for better use of control structures, loops, etc.

{%  macro no_nulls_in_cols(model) %}
    select * from {{ model }} where
    {% for col in adapter.get_columns_in_relation(model) %}
        {{ col.column }} is null or
    {% endfor %}
    false
{% endmacro %}


-- Generates the following:

-- select * from customers where
--     id is null or
--     name is null or
--     created_at is null or
--     false

-- to check whether each row's correponding value is null or not