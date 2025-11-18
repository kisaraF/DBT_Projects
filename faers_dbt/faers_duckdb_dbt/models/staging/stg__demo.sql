with src_demo as (
    select *
    from {{ source('faers_source','DEMO') }}
)

select
    cast(primaryid as bigint) as primaryid,
    cast(caseid as bigint) as caseid,
    cast(caseversion as bigint) as caseversion,
    i_f_code,
    rept_cod,
    e_sub,
    coalesce(strptime(event_dt, '%Y%m%d'), date '9999-12-31') as event_dt,
    coalesce(strptime(mfr_dt, '%Y%m%d'), date '9999-12-31') as mfr_dt,
    coalesce(strptime(init_fda_dt, '%Y%m%d'), date '9999-12-31') as init_fda_dt,
    coalesce(strptime(fda_dt, '%Y%m%d'), date '9999-12-31') as fda_dt,
    coalesce(strptime(rept_dt, '%Y%m%d'), date '9999-12-31') as rept_dt,
    coalesce(auth_num, 'n/a') as auth_num,
    coalesce(mfr_num, 'n/a') as mfr_num,
    coalesce(mfr_sndr, 'n/a') as mfr_sndr,
    coalesce(lit_ref, 'n/a') as lit_ref,
    coalesce(cast(age as integer), -1) as age,
    coalesce(age_cod, 'n/a') as age_cod,
    coalesce(age_grp, 'n/a') as age_grp,
    coalesce(sex, 'n/a') as gender,
    coalesce(cast(wt as integer), -1) as wt,
    coalesce(wt_cod, 'n/a') as wt_cod,
    coalesce(to_mfr, 'n/a') as to_mfr,
    coalesce(occp_cod, 'n/a') as occp_cod,
    case
        when
            occr_country is null and reporter_country is not null
            then reporter_country
        when
            reporter_country is null and occr_country is not null
            then occr_country
        when
            reporter_country is not null and occr_country is not null
            then reporter_country
        when occr_country is null and reporter_country is null then 'n/a'
    end as origin_country
from src_demo
