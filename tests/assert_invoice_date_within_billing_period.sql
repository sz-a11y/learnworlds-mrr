-- Test: invoice_date must fall within the billing period (billing_start_date to billing_end_date)
-- If any rows are returned, the test fails

select
    invoice_id,
    invoice_date,
    billing_start_date,
    billing_end_date
from {{ ref('stg_invoices') }}
where invoice_date < billing_start_date
   or invoice_date > billing_end_date
