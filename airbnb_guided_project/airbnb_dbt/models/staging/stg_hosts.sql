with src_hosts as (
    select *
    from {{ source('airbnb_source', 'hosts') }}
)

select
    id as host_id,
    name as host_name,
    created_at,
    updated_at,
    case
        when is_superhost = 'f' then False
        when is_superhost = 't' then True
    end as is_superhost
from src_hosts
