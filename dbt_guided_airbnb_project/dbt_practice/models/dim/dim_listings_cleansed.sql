{{
    config(
        materialized = 'view'
    )
}}

with src_listings_l as (
    select * from {{ ref('src_listings') }}
)

select  
    LISTING_ID,
    LISTING_NAME,
    LISTING_URL,
    ROOM_TYPE,
    iff(MINIMUM_NIGHTS=0, 1, MINIMUM_NIGHTS) as MINIMUM_NIGHTS,
    HOST_ID,
    replace(PRICE_STR,'$')::number(10,2) as price,
    CREATED_AT,
    UPDATED_AT
from src_listings_l