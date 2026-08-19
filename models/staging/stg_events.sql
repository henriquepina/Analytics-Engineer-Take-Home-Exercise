-- One-to-one staging model: normalize primitive fields and preserve the
-- properties map without assuming undocumented keys.
select
    nullif(trim(cast(event_id as varchar)), '') as event_id,
    nullif(trim(cast(company_id as varchar)), '') as company_id,
    nullif(trim(cast(profile_id as varchar)), '') as profile_id,
    lower(trim(cast(event_type as varchar))) as event_type,
    cast(occurred_at as timestamp_ntz) as occurred_at_utc,
    cast(created_at as timestamp_ntz) as created_at_utc,
    properties
from {{ source('iceberg', 'events') }}
