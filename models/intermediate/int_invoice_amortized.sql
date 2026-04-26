with month_series as (
    select generate_series(
        date_trunc('month', min(billing_start_date)),
        date_trunc('month', max(billing_end_date)),
        interval '1 month'
    )::date as month
    from {{ ref('stg_invoices') }}
),

invoice_enriched as (
    select
        i.invoice_id,
        i.amount_usd,
        i.billing_start_date,
        i.billing_end_date,
        (i.billing_end_date - i.billing_start_date + 1) as billing_days,
        c.country,
        sc.use_case
    from {{ ref('stg_invoices') }} i
    left join {{ ref('stg_subscriptions') }} s  on s.subscription_id = i.subscription_id
    left join {{ ref('stg_schools') }} sc        on sc.school_id      = s.school_id
    left join {{ ref('stg_customers') }} c       on c.customer_id     = i.customer_id
),

amortized as (
    select
        m.month,
        ie.invoice_id,
        ie.country,
        ie.use_case,
        ie.amount_usd,
        ie.billing_days,
        (
            least(ie.billing_end_date, (m.month + interval '1 month - 1 day')::date)
            - greatest(ie.billing_start_date, m.month)
            + 1
        ) as days_in_month,
        ie.amount_usd * (
            (
                least(ie.billing_end_date, (m.month + interval '1 month - 1 day')::date)
                - greatest(ie.billing_start_date, m.month)
                + 1
            )::numeric / ie.billing_days
        ) as mrr_usd
    from month_series m
    inner join invoice_enriched ie
        on ie.billing_start_date <= (m.month + interval '1 month - 1 day')::date
        and ie.billing_end_date  >= m.month
)

select * from amortized
