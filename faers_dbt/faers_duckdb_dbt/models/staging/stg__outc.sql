with src_outc as (
    select *
    from {{ source('faers_source', 'OUTC') }}
)

select
    cast(primaryid as bigint) as primaryid,
    cast(caseid as bigint) as caseid,
    outc_cod
from src_outc
