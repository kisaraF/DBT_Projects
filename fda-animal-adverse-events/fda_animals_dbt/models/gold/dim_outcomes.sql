with base as (
    select
        outcome_id,
        medical_status
    from {{ ref('int_outcomes') }}
    qualify row_number() over (partition by outcome_id order by outcome_id) = 1

)

select *
from base
