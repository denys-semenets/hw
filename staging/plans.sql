select
    plan_id as plan_id,
    plan_name,
    monthly_price_cents,
    duration_months as duration_months
from {{ ref('raw_plans') }}
