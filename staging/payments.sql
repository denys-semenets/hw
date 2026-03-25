select
    cast(payment_id as int) as payment_id,
    cast(member_id as int) as member_id,
    cast(plan_id as int) as plan_id,
    cast(payment_date as date) as payment_date,
    payment_method
from {{ ref('raw_payments') }}