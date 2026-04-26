with source as (
    select * from {{ source('learnworlds', 'subscriptions') }}
)

select
    subscription_id,
    subscription_type,
    school_id,
    billing_method,
    status,
    start_date::date        as start_date,
    billed_until_date::date as billed_until_date
from source
