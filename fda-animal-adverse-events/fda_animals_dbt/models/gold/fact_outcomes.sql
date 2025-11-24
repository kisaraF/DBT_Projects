with base as (
    select
        unique_aer_id_number,
        outcome_id,
        number_of_animals_affected
    from {{ ref('int_outcomes') }}
)

select *
from base
