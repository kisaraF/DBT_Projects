with src_demo as (
    select *
    from {{ source('faers_source','DEMO') }}
)

select 
    cast(primaryid as integer) as primaryid,
    cast (caseid as integer) as caseid,
    cast(caseversion as integer) as caseversion,
    i_f_code,
    strptime(event_dt, '%Y%m%d') as event_dt,
    strptime(mfr_dt, '%Y%m%d') as mfr_dt,
    strptime(init_fda_dt, '%Y%m%d') as init_fda_dt,
    strptime(fda_dt, '%Y%m%d') as fda_dt,
    strptime(rept_dt, '%Y%m%d') as rept_dt,
    rept_cod,
    coalesce(auth_num, 'n/a') as auth_num,
    coalesce(mfr_num, 'n/a') as mfr_num,
    coalesce(mfr_sndr, 'n/a') as mfr_sndr,
    coalesce(lit_ref, 'n/a') as lit_ref,
    coalesce(cast(age as integer),-1) as age,
    coalesce(age_cod, 'n/a') as age_cod,
    coalesce(age_grp, 'n/a') as age_grp,
    coalesce(sex, 'n/a') as gender,
    e_sub,
    coalesce(cast(wt as integer), -1) as wt,
    coalesce(wt_cod, 'n/a') as wt_cod,
    coalesce(to_mfr, 'n/a') as to_mfr,
    coalesce(occp_cod, 'n/a') as occp_cod,
    case
        when occr_country is null and reporter_country is not null then reporter_country
        when reporter_country is null and occr_country is not null then occr_country
        when reporter_country is not null and occr_country is not null then reporter_country
        when occr_country is null and reporter_country is null then 'n/a'
    end as origin_country
from src_demo
