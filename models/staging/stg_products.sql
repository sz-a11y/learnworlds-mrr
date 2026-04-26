with source as (
    select * from {{ source('learnworlds', 'products') }}
)

select
    product_id,
    product_name,
    billing_frequency
from source
