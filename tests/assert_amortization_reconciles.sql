-- Test: total amortized MRR per use_case/country must equal total raw invoice amount
-- Tolerance of $0.10 allowed for floating point rounding
-- If any rows are returned, the test fails

with raw_totals as (
    select
        sc.use_case,
        c.country,
        sum(i.amount_usd) as raw_amount
    from {{ ref('stg_invoices') }} i
    left join {{ ref('stg_subscriptions') }} s  on s.subscription_id = i.subscription_id
    left join {{ ref('stg_schools') }} sc        on sc.school_id      = s.school_id
    left join {{ ref('stg_customers') }} c       on c.customer_id     = i.customer_id
    group by sc.use_case, c.country
),

amortized_totals as (
    select
        use_case,
        country,
        sum(mrr_usd) as amortized_amount
    from {{ ref('mart_mrr_by_use_case') }}
    group by use_case, country
)

select
    a.use_case,
    a.country,
    a.amortized_amount,
    r.raw_amount,
    abs(a.amortized_amount - r.raw_amount) as diff
from amortized_totals a
left join raw_totals r
    on r.use_case = a.use_case
    and r.country = a.country
where abs(a.amortized_amount - r.raw_amount) >= 0.10
