select
    cast(trainer_id as int) as trainer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    specialization
from {{ ref('raw_trainers') }}