with base as (
    select
        _hash_id,
        get_json_object(raw_payload::string, '$.drug') as drug_json
    from {{ ref('stg_fda_raw_payload') }}
)
,
unpack_json as (
    select
        _hash_id,
        from_json(
            drug_json,
            'array<struct<
         active_ingredients:array<struct<
            name:string,
            dose:struct<
                numerator:string,
                numerator_unit:string,
                denominator:string,
                denominator_unit:string
            >
         >>
      >>'
        ) as drug_dose_array
    from base
)
,
json_process as (
    select
        unpack_json._hash_id,
        coalesce(da.col.name::string, 'n/a') as drug_name
    from unpack_json
        lateral view explode(drug_dose_array) dda
        lateral view explode(dda.col.active_ingredients) da
)
,
final as (
    select *
    from json_process
    qualify
        row_number() over (partition by _hash_id, drug_name order by _hash_id)
        = 1
)

select *
from final
