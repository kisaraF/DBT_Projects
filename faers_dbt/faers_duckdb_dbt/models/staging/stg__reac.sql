with src_reac as (
    select *
    from {{ source('faers_source', 'REAC') }}
)

select
    cast(primaryid as integer) as primaryid,
    cast(caseid as integer) as caseid,
    pt
from src_reac
