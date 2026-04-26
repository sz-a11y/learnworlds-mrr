# learnworlds_mrr

A dbt project that builds a Monthly Recurring Revenue (MRR) pipeline for LearnWorlds, a SaaS e-learning platform. The pipeline transforms raw invoicing and subscription data into a reporting-ready mart tracking MRR over time, broken down by school use case and customer country.

---

## Project Structure

```
learnworlds_mrr/
├── models/
│   ├── staging/            ← clean and type-cast raw source tables
│   ├── intermediate/       ← amortization logic
│   └── marts/              ← final MRR output table
├── seeds/                  ← raw CSV data loaded into PostgreSQL
└── tests/                  ← custom singular tests for business logic
```

The project follows a three-layer modeling pattern:

**Staging** models are thin wrappers over the seed tables. They apply correct data types, rename nothing (the source column names are already clean), and expose a stable interface for downstream models. All staging models are materialized as views since they perform no computation.

**Intermediate** models contain the core business logic — in this case, the invoice amortization. This layer is materialized as a table because it performs an expensive range join between a month spine and all invoices, and downstream models should not recompute it on every query.

**Mart** models aggregate intermediate results to the required reporting grain. The final mart is materialized as a table for query performance, as this is the layer that BI tools and dashboards would query directly.

---

## Source Data

Five seed tables are provided:

| Table | Description |
|---|---|
| `invoices` | One row per invoice. Contains both regular invoices and credit notes (negative amounts). |
| `customers` | One row per customer account. Provides the country dimension used in reporting. |
| `products` | One row per product. Defines billing frequency (monthly, quarterly, annual). |
| `subscriptions` | One row per subscription. Links customers to schools and tracks status and billing period. |
| `schools` | One row per school. Provides the use case dimension used in reporting. |

---

## Mart Output

**`mart_mrr_by_use_case`**

Grain: one row per calendar month per school use case per customer country.

| Column | Description |
|---|---|
| `month` | First day of the calendar month |
| `use_case` | School use case (b2b_course_sellers, b2c_course_sellers, customer_training, corporate_training, government_ngos) |
| `country` | Country of the customer entity |
| `mrr_usd` | Monthly Recurring Revenue in USD, amortized proportionally from invoice billing periods |

---

## Amortization Logic

Subscriptions are invoiced at different billing frequencies — monthly, quarterly, and annual. A single invoice may therefore span multiple calendar months. To correctly attribute revenue to the period it was earned, each invoice's amount is distributed proportionally across the months its billing period covers, based on the number of days that fall within each month.

The formula applied per invoice per month is:

```
mrr_usd = amount_usd × (days_in_month / total_billing_days)
```

Where `days_in_month` is calculated as the number of days the invoice's billing period overlaps with the calendar month, using `LEAST` and `GREATEST` to handle partial month boundaries cleanly.

A $1,200 annual invoice covering January to December contributes approximately $101.92 to January (31/365 × $1,200), $91.73 to February (28/365 × $1,200), and so on.

The month spine is generated dynamically from the actual `min(billing_start_date)` and `max(billing_end_date)` in the invoices table, so the pipeline adapts automatically as new data arrives.

---

## Handling of Credit Notes

The invoices table contains 170 credit notes (negative `amount_usd` values) out of 2,441 total invoice rows, representing mid-term subscription adjustments.

Credit notes are treated identically to regular invoices in the amortization model. Since each credit note carries its own `billing_start_date` and `billing_end_date`, the negative amount is distributed proportionally across the months it covers, naturally reducing MRR in exactly the right periods. No special handling or pre-aggregation at the subscription level is applied, as the billing period on the credit note already encodes which months should be adjusted.

---

## Handling of Cancelled Subscriptions

Approximately 70% of subscriptions in the dataset are cancelled. Cancelled subscriptions are fully included in MRR calculations. A subscription cancelled in March still generated real, earned revenue in January and February, and excluding it would understate historical MRR. What matters for revenue recognition is the billing period of the invoice, not the current subscription status.

---

## Materialization Decisions

| Layer | Materialization | Rationale |
|---|---|---|
| Staging | View | Thin wrappers with no computation. Always fresh from seeds. |
| Intermediate | Table | Expensive range join between month spine and all invoices. Precomputed to avoid recomputation on every downstream query. |
| Mart | Table | Final reporting layer queried by BI tools. Should be precomputed for performance. |

Incremental materialization was considered for the intermediate model but rejected. The amortization logic is a spanning calculation — a single invoice contributes to multiple months — meaning new invoices can affect past months. An incremental run would miss retroactive changes such as backdated credit notes or corrected billing periods. A full table refresh on each run is therefore the correct approach.

---

## Testing

The project defines 32 tests across two categories.

### Generic tests (30 tests, defined in schema.yml files)

Applied at the staging layer to validate raw data assumptions:

- **Primary key integrity**: `unique` + `not_null` on all entity primary keys (invoice_id, customer_id, product_id, subscription_id, school_id)
- **Foreign key integrity**: `relationships` tests confirming all invoice foreign keys resolve to valid records in customers, subscriptions, and products; and all subscription school_ids resolve to valid schools
- **Enum validation**: `accepted_values` tests on billing_frequency (monthly/quarterly/annual), subscription status (active/cancelled), default_billing_method (credit_card/debit_card/wire), and school use_case (all 5 values)
- **Not null**: Applied to all key reporting columns in the mart (month, use_case, country, mrr_usd)

The intermediate model (`int_invoice_amortized`) is intentionally not tested with generic tests. Testing the inputs at the staging layer and the outputs at the mart layer provides sufficient coverage without over-testing internal transformation steps.

### Singular tests (2 tests, defined in tests/)

**`assert_invoice_date_within_billing_period`**

Validates that `invoice_date` falls within the billing period for every invoice. This confirms a basic business rule — an invoice should not be dated outside the period it covers. The test returns any rows where `invoice_date < billing_start_date` or `invoice_date > billing_end_date`. During EDA this was confirmed to return zero rows, so the test is expected to always pass on this dataset.

**`assert_amortization_reconciles`**

This is the most important test in the project. It validates the end-to-end correctness of the amortization logic by checking that the sum of amortized MRR across all months equals the sum of raw invoice amounts, grouped by use case and country.

The test compares:
- `raw_amount`: the sum of `amount_usd` directly from `stg_invoices`, joined to use case and country
- `amortized_amount`: the sum of `mrr_usd` from `mart_mrr_by_use_case`, aggregated across all months

A tolerance of $0.10 is applied to account for floating point rounding in the day-based proration. Any use case / country combination where the difference exceeds this threshold causes the test to fail.

This test provides mathematical proof that no revenue is created or lost in the amortization process — the pipeline is conservative and the total revenue is preserved exactly across the transformation.

### What is not tested and why

**Composite uniqueness on the mart grain** (`month + use_case + country`): dbt's built-in `unique` test operates on single columns only. A composite uniqueness check would require a custom macro. Given the time constraints of this assignment this is documented as a known gap — in a production project this would be implemented as a singular test or via the `dbt_utils.unique_combination_of_columns` test from the dbt-utils package.

---

## Assumptions

- **Invoice date as billing date**: The `invoice_date` field is treated as metadata only. Revenue attribution is based entirely on `billing_start_date` and `billing_end_date`, not the invoice issue date.
- **Billing period days**: The billing period length is calculated as `billing_end_date - billing_start_date + 1` (inclusive of both endpoints). This is consistent with how calendar day ranges are typically counted.
- **No fixed period assumptions**: EDA revealed that invoice billing periods do not always match the expected duration for their billing frequency (e.g. annual invoices with fewer than 365 days). This is consistent with mid-term cancellations and pro-rated billing. The amortization logic uses actual billing dates rather than assuming fixed period lengths, making it robust to this variation.
- **Country from customer**: The country dimension is taken from the `customers` table. EDA confirmed full join integrity — every invoice has a matching customer with a non-null country.
- **Credit notes included at face value**: Credit notes are not validated against the original invoice they are correcting. They are amortized as-is across their own billing period, which is the correct approach given the data available.

---

## Exploratory Data Analysis

Key findings from EDA conducted before model development:

- 2,441 total invoices, 170 of which are credit notes (negative amounts)
- Full join integrity confirmed — no unmatched foreign keys across any dimension table
- Revenue is evenly distributed across all 5 use cases (invoice count 444–528, total revenue $102K–$117K per use case), consistent with synthetic test data
- Billing period lengths vary significantly within each billing frequency, confirming mid-term cancellations are common and fixed-period assumptions would be incorrect
- All invoice dates fall within their respective billing periods (validated by singular test)
- The amortization reconciliation check confirmed max deviation of $0.08 across all use case / country combinations, well within the $0.10 tolerance

---

## How to Run

```bash
# Install dependencies
pip3 install dbt-core dbt-postgres

# Seed raw data, build all models, run all tests
dbt build

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

Requires a PostgreSQL instance. Connection is configured in `~/.dbt/profiles.yml`.