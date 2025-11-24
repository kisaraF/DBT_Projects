with src_listings as (
    select *
    from {{ source('airbnb_source', 'listings') }}
)

select
    id::string as listing_id,
    listing_url::string,
    name::string as listing_name,
    room_type::string,
    minimum_nights::string,
    host_id::string,
    price::string,
    created_at::string,
    updated_at::string
from src_listings
