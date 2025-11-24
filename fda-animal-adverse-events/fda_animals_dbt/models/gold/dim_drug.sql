with base as (
    select
        drug_id,
        drug_name,
        brand_name,
        manufacturer_name,
        manufacturer_registration
    from {{ ref('int_drugs') }}
    qualify row_number() over (partition by drug_id order by drug_id) = 1
)

select *
from base
