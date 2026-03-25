select
    cast(checkin_id as int) as checkin_id,
    cast(member_id as int) as member_id,
    cast(checkin_timestamp as timestamp) as checkin_at,
    cast(location_id as int) as location_id
from {{ ref('raw_checkins') }}