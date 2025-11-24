with base as (
    select
        unique_aer_id_number,
        reaction_id,
        accuracy,
        number_of_animals_affected
    from {{ ref('int_reactions') }}
)

select *
from base
