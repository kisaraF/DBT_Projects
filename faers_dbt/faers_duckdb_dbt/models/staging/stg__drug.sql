with src_drug as (
    select *
    from {{ source('faers_source', 'DRUG') }}
)

select
    cast(primaryid as integer) as primaryid,
    cast(caseid as integer) as caseid,
    cast(drug_seq as integer) as drug_seq,
    drugname,
    prod_ai,
    val_vbm,
    route,
    dose_vbm,
    cum_dose_unit,
    dechal,
    rechal,
    lot_num,
    exp_dt,
    cast(nda_num as integer) as nda_num,
    cast(dose_amt as integer) as dose_amt,
    dose_unit,
    dose_form,
    dose_freq
from src_drug
