with src_reac as (
    select *
    from {{ source('faers_source', 'REAC') }}
)

select
    cast(primaryid as bigint) as primaryid,
    cast(caseid as bigint) as caseid,
    pt
from src_reac
