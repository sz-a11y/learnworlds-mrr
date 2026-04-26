with source as (
    select * from {{ source('learnworlds', 'invoices') }}
)

select
    invoice_id,
    customer_id,
    subscription_id,
    product_id,
    invoice_date::date        as invoice_date,
    billing_start_date::date  as billing_start_date,
    billing_end_date::date    as billing_end_date,
    amount_usd::numeric       as amount_usd
from source
