select
    id,
    created_at,
    title as product_name,
    category,
    ean,
    vendor,
    price
from {{ ref('bronze_products') }}