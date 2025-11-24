with base as (
    select
        _hash_id,
        raw_payload:unique_aer_id_number::string as unique_aer_id_number,
        raw_payload:report_id::string as report_id,
        raw_payload:animal.age.min::double as age,
        raw_payload:animal.age.unit::string as age_unit,
        raw_payload:animal.species::string as species,
        raw_payload:animal.breed.is_crossbred::string as is_crossbred,
        raw_payload:animal.female_animal_physiological_status::string
            as female_animal_physiological_status,
        raw_payload:animal.gender::string as gender,
        raw_payload:animal.reproductive_status::string as reproductive_status,
        raw_payload:animal.weight.unit::string as weight_unit,
        raw_payload:receiver.city::string as reciever_city,
        raw_payload:receiver.country::string as reciever_country,
        raw_payload:receiver.organization::string as reciever_organization,
        raw_payload:receiver.postal_code::string as reciever_postal_code,
        raw_payload:receiver.state::string as reciever_state,
        raw_payload:receiver.street_address::string as reciever_street_address,
        raw_payload:onset_date::string as onset_date,
        raw_payload:original_receive_date::string as original_receive_date,
        raw_payload:number_of_animals_affected::integer
            as number_of_animals_affected,
        raw_payload:number_of_animals_treated::integer
            as number_of_animals_treated,
        raw_payload:primary_reporter::string as primary_reporter,
        raw_payload:secondary_reporter::string as secondary_reporter,
        raw_payload:serious_ae::string as serious_ae,
        raw_payload:treated_for_ae::string as treated_for_ae,
        raw_payload:type_of_information::string as type_of_information,
        raw_payload:duration.value::double as duration,
        raw_payload:duration.unit::string as duration_unit,
        raw_payload:health_assessment_prior_to_exposure.assessed_by::string
            as assessed_by,
        raw_payload:animal.breed.breed_component as breed,
        round(raw_payload:animal.weight.min::double, 2) as weight
    from {{ ref('stg_fda_raw_payload') }}
),

parsed_events as (
    select
        _hash_id,
        unique_aer_id_number,
        report_id,
        coalesce(age, -1) as age,
        coalesce(age_unit, 'n/a') as age_unit,
        coalesce(species, 'n/a') as species,
        coalesce(breed, 'n/a'::variant) as breed,
        coalesce(is_crossbred, 'n/a') as is_crossbred,
        coalesce(female_animal_physiological_status, 'Unknown')
            as female_animal_physiological_status,
        coalesce(gender, 'n/a') as gender,
        coalesce(reproductive_status, 'Unknown') as reproductive_status,
        coalesce(weight, -1) as weight,
        coalesce(weight_unit, 'n/a') as weight_unit,
        coalesce(reciever_city, 'n/a') as reciever_city,
        coalesce(reciever_country, 'n/a') as reciever_country,
        coalesce(reciever_organization, 'n/a') as reciever_organization,
        coalesce(reciever_postal_code, 'n/a') as reciever_postal_code,
        coalesce(reciever_state, 'n/a') as reciever_state,
        coalesce(reciever_street_address, 'n/a') as reciever_street_address,
        coalesce(try_to_date(onset_date, 'yyyyMMdd'), to_date('9999-12-31'))
            as onset_date,
        coalesce(
            try_to_date(original_receive_date, 'yyyyMMdd'),
            to_date('9999-12-31')
        ) as original_receive_date,
        coalesce(number_of_animals_affected, -1) as number_of_animals_affected,
        coalesce(number_of_animals_treated, -1) as number_of_animals_treated,
        coalesce(primary_reporter, 'n/a') as primary_reporter,
        coalesce(secondary_reporter, 'n/a') as secondary_reporter,
        coalesce(serious_ae, 'n/a') as serious_ae,
        coalesce(treated_for_ae, 'n/a') as treated_for_ae,
        coalesce(type_of_information, 'n/a') as type_of_information,
        coalesce(duration, -1) as duration,
        coalesce(duration_unit, 'n/a') as duration_unit,
        coalesce(assessed_by, 'n/a') as assessed_by
    from base
)
,
final as (
    select
        _hash_id,
        unique_aer_id_number,
        report_id,
        age,
        age_unit,
        species,
        breed,
        is_crossbred,
        female_animal_physiological_status,
        gender,
        reproductive_status,
        weight,
        weight_unit,
        reciever_city,
        reciever_country,
        reciever_organization,
        reciever_postal_code,
        reciever_state,
        reciever_street_address,
        onset_date,
        original_receive_date,
        number_of_animals_affected,
        number_of_animals_treated,
        primary_reporter,
        secondary_reporter,
        serious_ae,
        treated_for_ae,
        type_of_information,
        duration,
        duration_unit,
        assessed_by,
        sha2(
            age
            || age_unit
            || species
            || breed
            || is_crossbred
            || female_animal_physiological_status
            || gender
            || reproductive_status
            || weight
            || weight_unit,
            256
        ) as animal_id,
        sha2(
            reciever_city
            || reciever_country
            || reciever_organization
            || reciever_postal_code
            || reciever_state
            || reciever_street_address,
            256
        ) as reciever_id
    from parsed_events
)

select
    unique_aer_id_number,
    report_id,
    animal_id,
    reciever_id,
    age,
    age_unit,
    species,
    breed,
    is_crossbred,
    female_animal_physiological_status,
    gender,
    reproductive_status,
    weight,
    weight_unit,
    reciever_city,
    reciever_country,
    reciever_organization,
    reciever_postal_code,
    reciever_state,
    reciever_street_address,
    onset_date,
    original_receive_date,
    number_of_animals_affected,
    number_of_animals_treated,
    primary_reporter,
    secondary_reporter,
    serious_ae,
    treated_for_ae,
    type_of_information,
    duration,
    duration_unit,
    assessed_by
from final
