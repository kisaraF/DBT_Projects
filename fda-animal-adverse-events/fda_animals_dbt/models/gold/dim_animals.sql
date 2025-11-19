with base as (
    select
        _hash_id,
        raw_payload:report_id::string as report_id,
        raw_payload:unique_aer_id_number::string as unique_aer_id_number,
        raw_payload:animal.age.min::double as age,
        raw_payload:animal.age.unit::string as age_unit,
        raw_payload:animal.species::string as species,
        raw_payload:animal.breed.breed_component::string as breed,
        raw_payload:animal.breed.is_crossbred::string as is_crossbred,
        raw_payload:animal.female_animal_physiological_status::string
            as female_animal_physiological_status,
        raw_payload:animal.gender::string as gender,
        raw_payload:animal.reproductive_status::string as reproductive_status,
        raw_payload:animal.weight.unit::string as weight_unit,
        _created_at,
        round(raw_payload:animal.weight.min::double, 2) as weight
    from {{ ref('stg_fda_raw_payload') }}
)

select
    _hash_id,
    report_id,
    unique_aer_id_number,
    coalesce(age, -1) as age,
    coalesce(age_unit, 'n/a') as age_unit,
    coalesce(species, 'n/a') as species,
    coalesce(breed, 'n/a') as breed,
    coalesce(is_crossbred, 'n/a') as is_crossbred,
    coalesce(female_animal_physiological_status, 'n/a')
        as female_animal_physiological_status,
    coalesce(gender, 'n/a') as gender,
    coalesce(reproductive_status, 'n/a') as reproductive_status,
    coalesce(weight, -1) as weight,
    coalesce(weight_unit, 'n/a') as weight_unit
from base
qualify row_number() over (partition by _hash_id order by _created_at desc) = 1
