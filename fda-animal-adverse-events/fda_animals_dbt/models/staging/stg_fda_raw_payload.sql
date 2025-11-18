with src_data as (
    select
        _created_at,
        _hash_id,
        try_parse_json(raw_payload) as raw_payload
    from {{ source('fda_animals_raw', 'raw_payload') }}
)

select *
from src_data
