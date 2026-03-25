select
    cast(class_id as int) as class_id,
    class_name,
    cast(trainer_id as int) as trainer_id,
    cast(capacity as int) as max_capacity
from {{ ref('raw_classes') }}