with base as (
    select
        raw_payload:unique_aer_id_number::string as unique_aer_id_number,
        get_json_object(raw_payload::string, '$.reaction') as reaction_json
    from {{ ref('stg_fda_raw_payload') }}
)
,
parsed_base as (
    select
        unique_aer_id_number,
        from_json(
            reaction_json,
            'array<struct<
    accuracy:string,
    number_of_animals_affected:int,
    veddra_term_code:string,
    veddra_term_name:string,
    veddra_version:string
    >>'
        ) as reaction_array
    from base
)
,
process_json as (
    select
        parsed_base.unique_aer_id_number,
        coalesce(ra.col.accuracy, 'n/a') as accuracy,
        coalesce(ra.col.number_of_animals_affected, -1)
            as number_of_animals_affected,
        coalesce(ra.col.veddra_term_code, 'n/a') as veddra_term_code,
        coalesce(ra.col.veddra_term_name, 'n/a') as veddra_term_name,
        coalesce(ra.col.veddra_version, 'n/a') as veddra_version
    from parsed_base
        lateral view explode(reaction_array) ra
)
,
final as (
    select
        unique_aer_id_number,
        accuracy,
        number_of_animals_affected,
        veddra_term_code,
        veddra_term_name,
        veddra_version,
        sha2(veddra_term_code || veddra_term_name || veddra_version, 256)
            as reaction_id
    from process_json
)

select *
from final
