with base as (
    select
        unique_aer_id_number,
        report_id,
        animal_id,
        reciever_id,
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
    from {{ ref('int_adverse_event') }}
)

select *
from base
