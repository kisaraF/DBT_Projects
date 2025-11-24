WITH base AS (
    SELECT
        _hash_id,
        raw_payload:unique_aer_id_number::string AS unique_aer_id_number,
        get_json_object(raw_payload::string, '$.drug') AS drug_json
    FROM {{ ref('stg_fda_raw_payload') }}
)
,
parsed AS (
    SELECT
        _hash_id,
        unique_aer_id_number,
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
processed_parse AS (
    SELECT
        parsed._hash_id,
        parsed.unique_aer_id_number,
        d.col.brand_name,
        da.col.name::string AS drug_name,
        d.col.route,
        d.col.administered_by,
        d.col.first_exposure_date,
        d.col.last_exposure_date,
        d.col.dosage_form,
        d.col.used_according_to_label,
        d.col.off_label_use,
        d.col.manufacturer.name AS manufacturer_name,
        d.col.manufacturer.registration_number AS manufacturer_registration,
        d.col.dose.numerator / d.col.dose.denominator AS dose,
        d.col.dose.numerator_unit
        || '/'
        || d.col.dose.denominator_unit AS dose_unit
    FROM parsed
        LATERAL VIEW explode(drug_arr) d
        LATERAL VIEW explode(d.col.active_ingredients) da
)
,
processed_parse_final AS (
    SELECT
        _hash_id,
        unique_aer_id_number,
        coalesce(brand_name, 'n/a') AS brand_name,
        coalesce(drug_name, 'n/a') AS drug_name,
        coalesce(route, 'n/a') AS route,
        coalesce(administered_by, 'n/a') AS administered_by,
        coalesce(off_label_use, 'n/a') AS off_label_use,
        coalesce(used_according_to_label, 'n/a') AS used_according_to_label,
        coalesce(
            try_to_date(first_exposure_date, 'yyyyMMdd'),
            to_date('9999-12-31')
        ) AS first_exposure_date,
        coalesce(
            try_to_date(last_exposure_date, 'yyyyMMdd'),
            to_date('9999-12-31')
        ) AS last_exposure_date,
        coalesce(dosage_form, 'n/a') AS dosage_form,
        coalesce(manufacturer_name, 'n/a') AS manufacturer_name,
        coalesce(manufacturer_registration, 'n/a') AS manufacturer_registration,
        coalesce(round(dose, 2), -1) AS dose,
        coalesce(dose_unit, 'n/a') AS dose_unit
    FROM processed_parse
    QUALIFY row_number() OVER (
        PARTITION BY
            _hash_id, unique_aer_id_number, drug_name ORDER BY
            brand_name NULLS LAST,
            drug_name NULLS LAST,
            route NULLS LAST,
            off_label_use NULLS LAST,
            used_according_to_label NULLS LAST,
            first_exposure_date NULLS LAST,
            last_exposure_date NULLS LAST,
            dosage_form NULLS LAST,
            manufacturer_name NULLS LAST,
            manufacturer_registration NULLS LAST,
            dose NULLS LAST,
            dose_unit NULLS LAST
    ) = 1
),

final AS (
    SELECT
        unique_aer_id_number,
        brand_name,
        drug_name,
        route,
        administered_by,
        off_label_use,
        used_according_to_label,
        first_exposure_date,
        last_exposure_date,
        dosage_form,
        manufacturer_name,
        manufacturer_registration,
        dose,
        dose_unit,
        sha2(
            brand_name
            || drug_name
            || manufacturer_name
            || manufacturer_registration,
            256
        ) AS drug_id
    FROM processed_parse_final
)

SELECT
    unique_aer_id_number,
    drug_id,
    drug_name,
    brand_name,
    route,
    administered_by,
    off_label_use,
    used_according_to_label,
    first_exposure_date,
    last_exposure_date,
    dosage_form,
    manufacturer_name,
    manufacturer_registration,
    dose,
    dose_unit
FROM final
