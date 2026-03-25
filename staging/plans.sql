select
    cast(plan_id as int) as plan_id,
    plan_name,

    cast(duration_months as int) as duration_months
from {{ ref('raw_plans') }}