select *
from {{ ref('dim_listings_cleansed') }}
where minimum_nights < 1 


-- Only passes the test if the condition is set as FALSE
-- 👉 A dbt test fails if the query returns any rows.
-- 👉 A dbt test passes if the query returns zero rows.