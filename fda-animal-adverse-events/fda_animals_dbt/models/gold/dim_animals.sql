with base as (
    select
        animal_id,
        age,
        age_unit,
        breed,
        is_crossbred,
        female_animal_physiological_status,
        gender,
        reproductive_status,
        species,
        weight,
        weight_unit
    from {{ ref('int_adverse_event') }}
    qualify row_number() over (partition by animal_id order by animal_id) = 1
)

select *
from base
