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
         >
      >>'
        ) AS drug_arr
    FROM base
)
,
final AS (
    SELECT
        parsed._hash_id,
        coalesce(d.col.brand_name, 'n/a') AS brand_name,
        coalesce(d.col.route, 'n/a') AS route,
        coalesce(d.col.administered_by, 'n/a') AS administered_by,
        coalesce(d.col.off_label_use, 'n/a') AS off_label_use,
        coalesce(d.col.used_according_to_label, 'n/a')
            AS used_according_to_label,
        coalesce(
            try_to_date(d.col.first_exposure_date, 'yyyyMMdd'),
            to_date('9999-12-31')
        ) AS first_exposure_date,
        coalesce(
            try_to_date(d.col.last_exposure_date, 'yyyyMMdd'),
            to_date('9999-12-31')
        ) AS last_exposure_date,
        coalesce(d.col.dosage_form, 'n/a') AS dosage_form,
        coalesce(d.col.manufacturer.name, 'n/a') AS manufacturer_name,
        coalesce(d.col.manufacturer.registration_number, 'n/a')
            AS manufacturer_registration,
        coalesce(round(d.col.dose.numerator / d.col.dose.denominator, 2), -1)
            AS dose,
        coalesce(
            d.col.dose.numerator_unit
            || '/'
            || d.col.dose.denominator_unit, 'n/a'
        ) AS dose_unit
    FROM parsed
        LATERAL VIEW explode(drug_arr) d
)

SELECT *
FROM final
