select
    cast(location_id as int) as location_id,
    location_name,
    city,
    address
from {{ ref('raw_locations') }}