# Weekly Company Engagement — User Guide

## What this model is

`fct_company_engagement_weekly` is the source of truth for cross-channel weekly
company engagement: one row per company per UTC ISO week with event counts and
a headline active flag.

## Who should use it

- **Product Analytics:** weekly or monthly engagement trends and channel mix.
- **BI:** company-level executive reporting using one governed definition.
- **Customer teams:** account investigation when a company’s recent engagement
  pattern changes.

Do **not** use it for profile journeys or ML event histories, billing or
contract eligibility, real-time alerting, or login/configuration activity.

## Grain and headline definition

One row represents a `(company_id, week_start)` pair. Weeks start Monday and
use UTC event time.

The headline column is:

```text
is_active = at least one email_open, email_click, or sms_delivered event
```

For weekly active companies, count distinct `company_id` where `is_active` is
true. Use `email_open_events`, `email_click_events`, and
`sms_delivered_events` to explain channel mix. Two diagnostic columns sit next
to the headline: `attempted_events` (sends and SMS relays, measuring platform
use) and `engaging_events` (end-user responses such as clicks, inbound SMS,
and opens excluding machine-generated ones). Neither changes `is_active`; they
exist so a trend can be explained without a new definition.
`email_send_events` is provided as an operational diagnostic but does not
qualify a company as active under definition version `v1`.

## Included and excluded

The model includes deduplicated events from the supplied event stream. New or
unclassified event types remain in `total_events` but do not change the active
definition until Analytics Engineering reviews the metric configuration.

The model does not exclude trial, internal, deleted, or churned companies
because the event stream does not provide a governed account-status field. Join
to the approved company/account dimension when contractual eligibility matters.

## Caveats — read before quoting a number

1. **Weeks are UTC.** An event near midnight may appear in a different week
   than a local-time report.
2. **Recent weeks can revise.** Late events recompute the affected company-week.
   Check the pipeline freshness status before publishing time-sensitive totals.
3. **Calendar-quarter boundaries need a decision.** This model stores complete
   ISO weeks. A calendar quarter may start or end mid-week; annotate boundary
   weeks or use an explicitly date-scoped calculation.
4. **Recorded events do not always prove human intent.** The headline
   `is_active` treats any recorded open or click as engagement. The
   `engaging_events` diagnostic additionally excludes machine-generated opens
   using a property key that is not yet governed by the source contract, so
   treat it as directional until that key is confirmed.
5. **Account eligibility is external.** Event activity alone cannot establish
   whether a company is contracted, internal, deleted, or otherwise eligible
   for an executive denominator.
6. **Definitions are versioned.** `metric_definition_version = 'v1'` identifies
   the qualifying event set. Definition changes require review, documentation,
   historical impact analysis, and an intentional rebuild.

## How to get help

Analytics Engineering owns the model and the metric definition. Open an issue
in this repository with the `company_id`, `week_start`, the result you
expected, and a dashboard or query link. For urgent freshness incidents, use
the team's data-incident channel and include the latest successful model
timestamp shown in `_loaded_at`.
