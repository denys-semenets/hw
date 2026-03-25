select
    cast(member_id as int) as member_id,
    upper(full_name) as member_name,
    gender,
    cast(birth_date as date) as birth_date,
    cast(join_date as date) as join_date,
    {{ generate_age_category('birth_date') }} as age_segment,
    status
from {{ ref('raw_members') }}