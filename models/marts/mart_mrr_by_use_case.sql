with amortized as (
    select * from {{ ref('int_invoice_amortized') }}
)

select
    month,
    use_case,
    country,
    round(sum(mrr_usd)::numeric, 2) as mrr_usd
from amortized
group by month, use_case, country
order by month, use_case, country
