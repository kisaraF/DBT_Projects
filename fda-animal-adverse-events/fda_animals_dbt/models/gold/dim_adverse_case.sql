/*
Can be used as a bridging table for fact_drug_events and fact_adverse_events
*/

with base as (
    select
        report_id,
        unique_aer_id_number
    from fda_animals_prj.dbt_dev_intermediate.int_adverse_event
)

select *
from base
