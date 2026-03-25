select
    payment_id as payment_id,
    member_id as member_id,
    plan_id as plan_id,
    amount_cents,
    cast(payment_date as date) as payment_date,
    payment_method
from {{ ref('raw_payments') }}
