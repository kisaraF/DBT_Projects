with base as (
    select
        reciever_id,
        reciever_city,
        reciever_country,
        reciever_organization,
        reciever_postal_code,
        reciever_state,
        reciever_street_address
    from fda_animals_prj.dbt_dev_intermediate.int_adverse_event
    qualify
        row_number() over (partition by reciever_id order by reciever_id) = 1
)

select *
from base
