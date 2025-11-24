with base as (
    select
        unique_aer_id_number,
        drug_id,
        route,
        administered_by,
        first_exposure_date,
        last_exposure_date,
        dosage_form,
        dose,
        dose_unit,
        used_according_to_label,
        off_label_use
    from {{ ref('int_drugs') }}
)

select *
from base
