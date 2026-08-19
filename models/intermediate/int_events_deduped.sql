{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='append_new_columns',
    cluster_by=['occurred_date']
) }}

{% set overlap_days = var('late_arrival_lookback_days', 3) %}
{% set initial_load_start = var('initial_load_start', none) %}

with candidate_events as (
    select *
    from {{ ref('stg_events') }}

    {% if is_incremental() %}
    -- created_at is the ingestion cursor. The overlap prevents records that
    -- share the prior maximum timestamp from being missed.
    where created_at_utc >= dateadd(
        day,
        -{{ overlap_days }},
        (
            select coalesce(
                max(created_at_utc),
                to_timestamp_ntz('1900-01-01 00:00:00')
            )
            from {{ this }}
        )
    )
    {% elif initial_load_start is not none %}
    where created_at_utc >= to_timestamp_ntz('{{ initial_load_start }}')
    {% endif %}
),

deduped as (
    select *
    from candidate_events
    qualify row_number() over (
        partition by event_id
        order by created_at_utc desc, occurred_at_utc desc
    ) = 1
)

select
    d.event_id,
    d.company_id,
    d.profile_id,
    d.event_type,
    d.occurred_at_utc,
    to_date(d.occurred_at_utc) as occurred_date,
    d.created_at_utc,
    d.properties,
    case
        when d.event_type in (
            {% for event_type in var('active_company_event_types') %}
            '{{ event_type }}'{{ "," if not loop.last }}
            {% endfor %}
        ) then true
        else false
    end as counts_as_active,
    'v1' as definition_version
from deduped d
