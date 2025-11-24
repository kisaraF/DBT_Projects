{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

with stg_reviews as (
    select *
    from {{ ref('stg_reviews') }}
)

select *
from stg_reviews
where review_text is not null
{% if is_incremental() %}
    and review_date > (select max(review_date) from {{ this }})
{% endif %}
