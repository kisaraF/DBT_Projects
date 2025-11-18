with src_drug as (
    select *
    from {{ source('faers_source', 'DRUG') }}
)

select
    cast(primaryid as bigint) as primaryid,
    cast(caseid as bigint) as caseid,
    cast(drug_seq as integer) as drug_seq,
    drugname,
    val_vbm,
    coalesce(prod_ai, 'n/a') as prod_ai,
    coalesce(route, 'Unknown') as route,
    coalesce(dose_vbm, 'UNK') as dose_vbm,
    coalesce(cum_dose_unit, 'n/a') as cum_dose_unit,
    coalesce(dechal, 'n/a') as dechal,
    coalesce(rechal, 'n/a') as rechal,
    coalesce(lot_num, 'n/a') as lot_num,
    coalesce(cast(strptime(exp_dt, '%Y%m%d') as date), date '9999-12-31')
        as exp_dt,
    coalesce(nda_num, '-1') as nda_num,
    coalesce(cast(dose_amt as integer), -1) as dose_amt,
    coalesce(dose_unit, 'n/a') as dose_unit,
    coalesce(dose_form, 'n/a') as dose_form,
    coalesce(dose_freq, 'n/a') as dose_freq
from src_drug
