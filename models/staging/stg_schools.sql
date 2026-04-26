with source as (
    select * from {{ source('learnworlds', 'schools') }}
)

select
    school_id,
    school_name,
    use_case
from source
