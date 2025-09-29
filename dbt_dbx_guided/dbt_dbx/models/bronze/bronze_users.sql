select *
from {{ source('landing_s', 'users') }}