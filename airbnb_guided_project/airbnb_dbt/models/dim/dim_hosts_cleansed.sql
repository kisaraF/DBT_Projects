{{
    config(
        materialized = 'ephemeral'
    )
}}

with stg_hosts as (
    select *
    from {{ ref('stg_hosts') }}
),

final as (
    select
        host_id,
        created_at,
        updated_at,
        coalesce(host_name, 'Anonymous') as host_name,
        case
            when is_superhost = 'f' then False
            when is_superhost = 't' then True
        end as is_superhost
    from stg_hosts
)

select
    host_id,
    host_name,
    is_superhost,
    created_at,
    updated_at
from final
