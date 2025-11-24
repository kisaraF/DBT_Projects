with base as (
    select
        reaction_id,
        veddra_term_code,
        veddra_term_name,
        veddra_version
    from {{ ref('int_reactions') }}
    qualify
        row_number() over (partition by reaction_id order by reaction_id) = 1
)

select *
from base
