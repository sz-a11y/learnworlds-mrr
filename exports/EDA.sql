-- Exploratory Data Analysis

-- Data modelling checks 

-- 1. PK constraint on invoices : OK, invoice_id acts as a PK

select invoice_id, count(*)
from invoices 
group by invoice_id
order by count(*) desc 

-- 2. Join integrity check : OK, i.e. no unmatched foreign keys exist in dimension tables in relation to invoices

SELECT
    COUNT(*) as total_invoices,
    COUNT(s.subscription_id) as matched_subscriptions,
    COUNT(p.product_id) as matched_products,
    COUNT(sc.school_id) as matched_schools,
    COUNT(c.customer_id) as matched_customers
FROM invoices i
LEFT JOIN subscriptions s ON s.subscription_id = i.subscription_id
LEFT JOIN schools sc ON sc.school_id = s.school_id
LEFT JOIN customers c ON c.customer_id = i.customer_id
left join products p on p.product_id=i.product_id 
;

-- 3. Subscriptions --> products 
-- A subscription entity is associated with one and only product entity   

select subscription_id
, count(distinct i.product_id) as products
from invoices i 
group by subscription_id 
order by count(distinct i.product_id) desc; 


-- 4. Subscriptions --> customers 
-- A subscription entity is assigned to a single customer  entity each time 


select subscription_id, count(distinct customer_id) custs 
from invoices 
group by subscription_id 
order by count(distinct customer_id)  desc 

-- 5. Customer --> Subscriptions, Products 
-- A customer entity can have multiple subscriptions and products 

select customer_id
, count(distinct subscription_id) subscriptions
, count(distinct product_id) products 
from invoices 
group by customer_id 


-- Basic data integrity checks 

-- 1. Relationship between invoice_date and billing period in invoices table 
-- conclusion : invoice_date is between the billing period in all datapoints 

select count(*)
from invoices i 
where invoice_date < billing_start_date 
or invoice_date > billing_end_date 


--  Understanding the business that the data describes

-- 1. invoices, credit notes, amounts : 170/2421 invoice items are actually credit notes 
SELECT 
    COUNT(*) as total_invoices,
    SUM(CASE WHEN amount_usd < 0 THEN 1 ELSE 0 END) as credit_notes,
    SUM(CASE WHEN amount_usd = 0 THEN 1 ELSE 0 END) as zero_amount,
    MIN(amount_usd) as min_amount,
    MAX(amount_usd) as max_amount
FROM invoices;


-- 2. Invoices' date range : 2023 - 2025 
SELECT
    MIN(billing_start_date) as earliest_start,
    MAX(billing_end_date) as latest_end
FROM invoices;

-- 3. Subscriptions' date range : 2023 - 2025 

SELECT
    MIN(start_date) as earliest_start,
    MAX(billed_until_date) as latest_end
FROM subscriptions;


-- 4. Is the same product billed differently across different customers/invoices ? 
-- Answer: No, the only variation observed across products' pricing is credit notes, so negative amounts 
-- i.e. query below returns same min, max values for ech product of the product portfolio 

select product_id 
, max(amount_usd) max_amount
, min(amount_usd) min_amount
from invoices 
where amount_usd>0 
group by product_id



-- 5. revenue/invoice distribution by use case : no skew observed, amounts and invoices appear even across all raw use cases 
-- some ranges: invoice count (444–528), total revenue ($102K–$117K)
-- SOS: No use case will dominate the MRR output

SELECT
    sc.use_case,
    COUNT(DISTINCT i.invoice_id) as invoice_count,
    ROUND(SUM(i.amount_usd)::numeric, 2) as total_revenue_usd
FROM invoices i
LEFT JOIN subscriptions s ON s.subscription_id = i.subscription_id
LEFT JOIN schools sc ON sc.school_id = s.school_id
GROUP BY sc.use_case
ORDER BY total_revenue_usd DESC;


-- 6. Cancelled subscriptions: most are cancelled in the dataset

SELECT
    s.status,
    COUNT(DISTINCT i.invoice_id) as invoice_count,
    ROUND(SUM(i.amount_usd)::numeric, 2) as total_revenue
FROM invoices i
LEFT JOIN subscriptions s ON s.subscription_id = i.subscription_id
GROUP BY s.status;


-- 7. Invoices' distribution across billing billing period, frequency, status 
-- Conclusion : Most invoices are cancelled before they get re-billed, and the billing period in most cases is less than a full one 
-- i.e. 365 days for annual frequency, 90 days for quarterly, 30 days for monthly 

select (i.billing_end_date - i.billing_start_date) as billing_period_days
, p.billing_frequency
, s.status
, case when p.billing_frequency = 'annual' and (i.billing_end_date - i.billing_start_date) < 365 then 'Billing ended before full billing period'
	   when p.billing_frequency = 'quarterly' and (i.billing_end_date - i.billing_start_date) < 90 then 'Billing ended before full billing period'
	   when p.billing_frequency = 'monthly' and (i.billing_end_date - i.billing_start_date) < 30 then 'Billing ended before full billing period'
  else 	'Full billing period' end as billing_period_classification
, count(*)
from invoices i 
left join products p on p.product_id=i.product_id 
LEFT JOIN subscriptions s ON s.subscription_id = i.subscription_id
group by (billing_end_date - billing_start_date) , billing_frequency, s.status
order by (billing_end_date - billing_start_date) desc;


-- Anomaly checks 

-- 1. Subscription  <--> invoice date range 
-- find anomalies i.e. 
-- subscriptions invoiced outside of their billing period --> 170/666 suscriptions portray this behaviour 
-- all of the 170 anomaly cases have --> invoice ends after subscription

select count(distinct subscription_id) as s
FROM (
SELECT
    s.subscription_id,
    s.start_date                        as sub_start,
    s.billed_until_date                 as sub_end,
    MIN(i.billing_start_date)           as first_invoice_start,
    MAX(i.billing_end_date)             as last_invoice_end,
    -- flag where invoice dates fall outside subscription dates
    CASE 
        WHEN MIN(i.billing_start_date) < s.start_date 
        THEN 'invoice starts before subscription' 
        ELSE 'ok' 
    END as start_check,
    CASE 
        WHEN MAX(i.billing_end_date) > s.billed_until_date 
        THEN 'invoice ends after subscription' 
        ELSE 'ok' 
    END as end_check
FROM subscriptions s
LEFT JOIN invoices i ON i.subscription_id = s.subscription_id
GROUP BY 
    s.subscription_id, 
    s.start_date, 
    s.billed_until_date
HAVING
    MIN(i.billing_start_date) > s.start_date
    OR MAX(i.billing_end_date) > s.billed_until_date
ORDER BY s.subscription_id) gg;



-- Amortization logic : 
-- Since we want Monthly revenue, we can distribute the invoices.amount_usd across 
-- months in the billing period (billing_start_date - billing_end_date) first and then aggregate from there 
-- How will we treat credit notes? --> just like the invoices, since they also contain billing period, 
-- those will simply be subtracted from their assigned billing months automatically when aggregating 


with month_series as (
    -- generate all calendar months in the dataset range
    select generate_series(
        date_trunc('month', min(billing_start_date)),
        date_trunc('month', max(billing_end_date)),
        interval '1 month'
    )::date as month
    from invoices
),

invoice_enriched as (
    -- join all dimensions onto invoices
    select
        i.invoice_id,
        i.amount_usd,
        i.billing_start_date,
        i.billing_end_date,
        -- total days in the billing period
        (i.billing_end_date - i.billing_start_date + 1) as billing_days,
        c.country,
        sc.use_case
    from invoices i
    left join subscriptions s  on s.subscription_id  = i.subscription_id
    left join schools sc       on sc.school_id        = s.school_id
    left join customers c      on c.customer_id       = i.customer_id
),

amortized as (
    -- for each invoice, find which months it overlaps with
    -- and calculate how many days of that invoice fall in each month
    select
        m.month,
        ie.invoice_id,
        ie.country,
        ie.use_case,
        ie.amount_usd,
        ie.billing_days,
        -- days of this invoice that fall within this calendar month
        (
            LEAST(ie.billing_end_date, (m.month + interval '1 month - 1 day')::date)
            - GREATEST(ie.billing_start_date, m.month)
            + 1
        ) as days_in_month,
        -- proportional MRR for this month = invoice_amount * fraction
		-- where fraction = days_in_month/billing_days
        ie.amount_usd * (
            (
                LEAST(ie.billing_end_date, (m.month + interval '1 month - 1 day')::date)
                - GREATEST(ie.billing_start_date, m.month)
                + 1
            )::numeric / ie.billing_days
        ) as mrr_usd
    from month_series m
	-- For each calendar month, find every invoice whose billing period overlaps with that month
	-- overlap: start billing before month ends, end billing on or after month starts
    inner join invoice_enriched ie
        on ie.billing_start_date <= (m.month + interval '1 month - 1 day')::date
        and ie.billing_end_date  >= m.month
)

, mrr as (
-- final aggregation to the required grain
select
    month,
    use_case,
    country,
    round(sum(mrr_usd)::numeric, 2) as mrr_usd
from amortized
group by month, use_case, country
order by month, use_case, country) 


, cte_check as (
select sc.use_case
, c.country 
, sum(i.amount_usd) as amount
from invoices i
left join subscriptions s  on s.subscription_id  = i.subscription_id
left join schools sc       on sc.school_id        = s.school_id
left join customers c      on c.customer_id       = i.customer_id
group by sc.use_case
, c.country
)

, mrr_agg as 

(
select use_case
, country 
, sum(mrr_usd) as amount
from mrr 
group by use_case
,country
)

select mrr.use_case, mrr.country, mrr.amount as mrr_sum, c.amount as check_sum, 
case when abs(mrr.amount - c.amount)<0.1 then TRUE else false end as check , 
 abs(mrr.amount - c.amount) as diff
from mrr_agg as mrr
left join cte_check c on c.use_case = mrr.use_case and c.country = mrr.country ;