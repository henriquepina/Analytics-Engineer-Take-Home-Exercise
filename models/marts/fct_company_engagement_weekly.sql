{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['company_id', 'week_start'],
    on_schema_change='append_new_columns',
    cluster_by=['week_start']
) }}

{% set overlap_days = var('late_arrival_lookback_days', 3) %}

with events as (
    select * from {{ ref('int_events_deduped') }}
),

changed_company_weeks as (
    select distinct
        company_id,
        dateadd(
            day,
            1 - dayofweekiso(occurred_date),
            occurred_date
        ) as week_start
    from events

    {% if is_incremental() %}
    where created_at_utc >= dateadd(
        day,
        -{{ overlap_days }},
        (
            select coalesce(
                max(_max_created_at_utc),
                to_timestamp_ntz('1900-01-01 00:00:00')
            )
            from {{ this }}
        )
    )
    {% endif %}
),

scoped_events as (
    -- Recompute every complete company-week touched by new or corrected rows;
    -- never replace a weekly aggregate with a partial incremental batch.
    select e.*
    from events e
    inner join changed_company_weeks c
        on e.company_id = c.company_id
        and dateadd(
            day,
            1 - dayofweekiso(e.occurred_date),
            e.occurred_date
        ) = c.week_start
),

weekly as (
    select
        company_id,
        dateadd(
            day,
            1 - dayofweekiso(occurred_date),
            occurred_date
        ) as week_start,
        count(*) as total_events,
        count_if(event_type = 'email_send') as email_send_events,
        count_if(event_type = 'email_open') as email_open_events,
        count_if(event_type = 'email_click') as email_click_events,
        count_if(event_type = 'sms_delivered') as sms_delivered_events,

        -- Section 1 signal components. Attempted counts platform use;
        -- engaging counts end-user responses, excluding machine-generated
        -- opens via an assumed property key (disclosed in notes).
        count_if(event_type in (
            {% for t in var('attempted_event_types') %}'{{ t }}'{{ "," if not loop.last }}{% endfor %}
        )) as attempted_events,
        count_if(
            event_type in (
                {% for t in var('engaging_event_types') %}'{{ t }}'{{ "," if not loop.last }}{% endfor %}
            )
            and not (
                event_type = 'email_open'
                and coalesce(properties['$machine_open']::string, 'false') = 'true'
            )
        ) as engaging_events,

        count_if(counts_as_active) as active_events,
        count(distinct iff(counts_as_active, profile_id, null)) as active_profiles,
        count_if(counts_as_active) > 0 as is_active,
        'v1' as metric_definition_version,
        max(created_at_utc) as _max_created_at_utc,
        current_timestamp() as _loaded_at
    from scoped_events
    group by 1, 2
)

select
    md5(concat_ws('|', company_id, to_varchar(week_start, 'YYYY-MM-DD'))) as company_week_key,
    weekly.*
from weekly