-- Domain test: the headline flag and component counts must always reconcile.
-- The event types are intentionally hardcoded: changing the metric definition
-- in dbt_project.yml must consciously touch this test too.
select *
from {{ ref('fct_company_engagement_weekly') }}
where active_events <> email_open_events + email_click_events + sms_delivered_events
   or active_events > total_events
   or is_active <> (active_events > 0)
   or attempted_events > total_events
   or engaging_events > total_events
   or engaging_events < 0
   or attempted_events < 0