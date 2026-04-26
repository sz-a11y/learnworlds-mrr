with source as (
    select * from {{ source('learnworlds', 'customers') }}
)

select
    customer_id,
    company_name,
    country,
    default_billing_method
from source
