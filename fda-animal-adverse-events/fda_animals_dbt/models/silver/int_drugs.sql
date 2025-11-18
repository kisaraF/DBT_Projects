WITH base AS (
    SELECT
        _hash_id,
        get_json_object(raw_payload::string, '$.drug') AS drug_json
    FROM {{ ref('stg_fda_raw_payload') }}
)
,
parsed AS (
    SELECT
        _hash_id,
        from_json(
            drug_json,
            'array<struct<
         brand_name:string,
         route:string,
         administered_by:string,
         off_label_use:string,
         used_according_to_label:string,
         first_exposure_date:string,
         last_exposure_date:string,
         dosage_form:string,
         manufacturer:struct<
            name:string,
            registration_number:string
         >,
         dose:struct<
              numerator:double,
              numerator_unit:string,
              denominator:double,
              denominator_unit:string
         >,
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
        ) AS drug_arr
    FROM base
)
,
final AS (
    SELECT
        parsed._hash_id,
        d.col.brand_name,
        d.col.route,   ---- Add Coalesce too
        d.col.administered_by,
        d.col.off_label_use,
        d.col.used_according_to_label,
        d.col.first_exposure_date,
        d.col.last_exposure_date,
        d.col.dosage_form,
        d.col.manufacturer.name AS manufacturer_name,
        d.col.manufacturer.registration_number AS manufacturer_registration,
        round(d.col.dose.numerator / d.col.dose.denominator, 2) AS dose,
        d.col.dose.numerator_unit
        || '/'
        || d.col.dose.denominator_unit AS dose_unit
    -- d.col.active_ingredients.name as drug_names  ---- Need to create a separate dim for drug names. because they comes as a list
    FROM parsed
        LATERAL VIEW explode(drug_arr) d
)

SELECT *
FROM final
