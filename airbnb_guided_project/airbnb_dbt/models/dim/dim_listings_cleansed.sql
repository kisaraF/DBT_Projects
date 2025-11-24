{{
    config(
        materialized = 'ephemeral'
    )
}}

with stg_listings as (
    select *
    from {{ ref('stg_listings') }}
)

select 
    listing_id::int as listing_id,
    listing_name,
    listing_url,
    room_type,
    case
        when try_cast(minimum_nights as int) = 0 then 1
        else try_cast(minimum_nights as int)
    end as minimum_nights,
    host_id,
    replace(price, '$', '')::double as price,
    try_cast(created_at as timestamp) as created_at,
    try_cast(updated_at as timestamp) as updated_at
from stg_listings