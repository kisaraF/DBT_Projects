{{
    config(
        materialized = 'table'
    )
}}

with fct_reviews_l as (
    select * from {{ ref('fct_reviews') }}
)
,

full_moon_dates as (
    select * from airbnb.dev_2.SEED_FULL_MOON_DATES_ho
)

select
    r.*,
    case 
        when fm.full_moon_date is null then 'not full moon'
        else 'full_moon' 
    end as is_full_moon
from fct_reviews r
left join full_moon_dates fm on to_date(r.review_date) = dateadd(day, 1, fm.full_moon_date)