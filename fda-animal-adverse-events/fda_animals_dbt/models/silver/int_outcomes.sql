with base as (
    select
        raw_payload:unique_aer_id_number::string as unique_aer_id_number,
        get_json_object(raw_payload::string, '$.outcome') as outcome
    from {{ ref('stg_fda_raw_payload') }}
)
,
base_processed as (
    select
        unique_aer_id_number,
        from_json(
            outcome,
            'array<struct<
    medical_status:string,
    number_of_animals_affected:integer
    >>'
        ) as outcome_array
    from base
)
,
processed_final as (
    select
        base_processed.unique_aer_id_number,
        coalesce(oa.col.medical_status, 'n/a') as medical_status,
        coalesce(oa.col.number_of_animals_affected, -1)
            as number_of_animals_affected
    from base_processed
        lateral view explode(outcome_array) oa
)
,
final as (
    select
        unique_aer_id_number,
        medical_status,
        number_of_animals_affected,
        sha2(medical_status, 256) as outcome_id
    from processed_final
)

select *
from final
limit 20;
