with src_outc as (
    select *
    from {{ source('faers_source', 'OUTC') }}
)

select
    cast(primaryid as integer) as primaryid,
    cast(caseid as integer) as caseid,
    outc_cod
from src_outc
