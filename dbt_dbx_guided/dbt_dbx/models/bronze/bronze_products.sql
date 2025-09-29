select *
from {{ source('landing_s', 'products') }}