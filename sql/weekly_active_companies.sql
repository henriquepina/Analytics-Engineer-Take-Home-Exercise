-- Weekly company activity signals for the previous completed calendar quarter.
--
-- Three company-week signals, computed side by side (see notes.md Section 1):
--   attempted : >=1 email_send or sms_relayed
--   delivered : >=1 sms_delivered, or an email_send with no matching
--               bounce/drop for the same message and profile (inferred)
--   engaging  : >=1 non-machine email_open, email_click, sms_click,
--               or sms_inbound  (recommended headline)
-- Diagnostics are per-company set differences, not subtractions of totals.
--
-- Restatement note: failures are scoped to the quarter window. A failure
-- arriving after quarter close would restate a previously delivered send;
-- whether closed quarters freeze or restate is a reporting-policy decision
-- (notes.md), and the production model recomputes touched weeks either way.
--
-- Source: validated against takehome.raw.events in Snowflake; replace with
-- the source relation in your environment.

with quarter_bounds as (
    select
        dateadd(quarter, -1, date_trunc('quarter', current_date()))::date as quarter_start,
        date_trunc('quarter', current_date())::date                       as quarter_end
),
 
events_deduped as (
    -- replays and corrections: latest created_at wins per event_id
    select
        * 
    from takehome.raw.events
    qualify row_number() over (
        partition by event_id
        order by created_at desc
    ) = 1
),
 
scoped as (
    select
        e.*,
        date_trunc('week', e.occurred_at) as week_start
    from events_deduped e
    cross join quarter_bounds q
    where e.occurred_at >= q.quarter_start
      and e.occurred_at <  q.quarter_end
      and coalesce(e.properties['test']::string, 'false') <> 'true'
),
 
email_failures as (
    -- scoped to the quarter window by design; see header note on restatement
    select
        properties['$message']::string as message_id,
        profile_id
    from scoped
    where event_type in ('email_bounce', 'email_dropped')
),
 
classified as (
    -- ATTEMPTED: any outbound handoff
    select week_start, company_id, 'attempted' as tier
    from scoped
    where event_type in ('email_send', 'sms_relayed')
 
    union all
    -- DELIVERED: email sends with no matched failure (inferred, not confirmed)
    -- + carrier-confirmed SMS deliveries
    select s.week_start, s.company_id, 'delivered' as tier
    from scoped s
    left join email_failures f
      on  f.message_id = s.properties['$message']::string
      and f.profile_id = s.profile_id
    where s.event_type = 'email_send'
      and s.properties['$message'] is not null   -- sends without a message id are not presumed delivered
      and f.message_id is null
    union all
    select week_start, company_id, 'delivered'
    from scoped
    where event_type = 'sms_delivered'
 
    union all
    -- ENGAGING: recipient response only (machine opens excluded)
    select week_start, company_id, 'engaging'
    from scoped
    where (event_type = 'email_open'
           and coalesce(properties['$machine_open']::string, 'false') <> 'true')
       or event_type in ('email_click', 'sms_click', 'sms_inbound')
),
 
-- company-week grain: the reusable layer. Gaps below are true set
-- differences at this grain, not subtractions of independent totals.
company_week_sets as (
    select
        week_start,
        company_id,
        max(iff(tier = 'attempted', 1, 0)) as is_attempted,
        max(iff(tier = 'delivered', 1, 0)) as is_delivered,
        max(iff(tier = 'engaging',  1, 0)) as is_engaging
    from classified
    group by week_start, company_id
)
 
select
    s.week_start,
    count_if(s.is_attempted = 1)                            as active_companies_attempted,
    count_if(s.is_delivered = 1)                            as active_companies_delivered,
    count_if(s.is_engaging  = 1)                            as engaging_companies,        -- recommended headline
    count_if(s.is_attempted = 1 and s.is_delivered = 0)     as deliverability_gap,
    count_if(s.is_delivered = 1 and s.is_engaging  = 0)     as churn_risk_gap,
    s.week_start < b.quarter_start
        or dateadd(day, 7, s.week_start) > b.quarter_end    as is_partial_week
from company_week_sets s
cross join quarter_bounds b
group by s.week_start, b.quarter_start, b.quarter_end
order by s.week_start
 