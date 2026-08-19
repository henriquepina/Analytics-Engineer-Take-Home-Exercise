# Analytics Engineer Take-Home Notes

## Section 1 — Stakeholder Ask → Metric Definition

### Slack response

Quick question back before I pull this, because "active" can mean a few different things in our events and the number changes depending on which one you want.

From the event stream I can measure three stages: companies that sent something, companies whose messages actually reached people (some sends bounce or fail), and companies that got a real response back (opens, clicks, replies). These tell different stories. A trend could move because customers stopped using us, because their deliverability got worse, or because their audiences stopped responding, and one single number would hide which of those is happening.

Since you mentioned engagement trends, my plan is to lead with **companies that got at least one real response in the week**, and show the sending and reaching counts next to it for context. I'm filtering out auto-generated opens (Apple fires opens automatically for a big chunk of mail, so raw opens overcount) and test accounts. Weeks run Monday to Sunday and I'm using the last full quarter. Heads up that the first and last weeks of the quarter are partial weeks, so they'll look lower, that's not churn.

A couple of things I want to confirm with you rather than assume: whether "last quarter" means the calendar quarter (assuming yes), whether trial or internal accounts should be out (the events alone can't fully tell me, I'd need account data for that), and one for finance really, since we report on quarters, whether these numbers should freeze once a quarter is closed or update if late data comes in. I'll go with reasonable defaults for now and flag them in the output, none of these block the first pull.

One honest note: the best version of "is a company active" would use product actions like logins or campaign edits, and those aren't in this event stream. If that's the question behind the question, worth a chat.

### Grain and filters (technical notes)

Output is one row per week, Monday start, UTC, previous completed calendar quarter. Distinct company counts for the three signals plus two diagnostics: attempted without delivery, and delivered without engagement. Underneath, the calculation is one row per company per week before the rollup, which is also what the production model would expose, so follow-ups like "which companies" don't need new logic. Deduplicated on event_id keeping the latest ingested record. No "active rate" because the stream has no trustworthy denominator of eligible companies; that needs an account source.

Delivery for email is inferred, not confirmed: a send counts as delivered when there's no matching bounce or drop for the same message and profile. Sends without a message id are not presumed delivered. SMS delivery is its own event, so no inference needed there.

### Edge cases

- Replays and corrections: latest created_at wins per event_id. Exact timestamp ties would need a deterministic ingestion sequence from the producer.
- Late failures and quarter close: a bounce can arrive after the quarter closed and flip a send from delivered to failed. The standalone query scopes failures to the quarter and doesn't decide this; whether closed quarters freeze or restate is a reporting policy call that needs an owner (raised above). The production model recomputes any company-week that late-arriving events touch, so either policy can be implemented once decided; a delivery-inference model, if promoted to production later, would additionally need to link failures back to the original send's week.
- Cross-week timing: a response can land in a different week than its send. The three signals are independent weekly observations per company, not a funnel, which is why the diagnostics are computed as per-company set differences and not by subtracting totals.
- Partial boundary weeks: flagged, not hidden.
- Mid-week onboarding: a company counts from its first qualifying event. I wouldn't prorate a yes/no weekly flag.
- Test traffic and machine opens: identified through event properties that I'm assuming exist as documented. Needs confirming with the event owner.
- Deleted or churned companies: not detectable from events alone, and a churned account with automated flows still running would keep looking active. The fix is joining account/CRM data (contract status, deletion dates, internal flags), which would also let account teams work the no-engagement list instead of just counting it.

SQL: [`sql/weekly_active_companies.sql`](sql/weekly_active_companies.sql)

### AI usage note

- **System:** Claude.
- **Initial prompt:** "Review my three-stage weekly company metric, attempted, delivered and engaging, and correct the Snowflake SQL without replacing my chosen business framing."
- **Human direction and validation:** I replaced the AI's single-metric recommendation with the three-signal framing, kept engaging as the headline since the ask was about engagement, made it disclose that email delivery is inferred and that several property keys are assumptions, corrected the gap calculations to per-company set differences instead of subtracting totals, and reframed late-failure handling as a quarter-close reporting policy question instead of a hardcoded waiting window.

## Section 2 — dbt Model Build

### Architecture

```text
iceberg.events
    -> stg_events   view
        -> int_events_deduped incremental merge on event_id
            -> fct_company_engagement_weekly incremental 
                merge on company_id + week_start
```

The event-type lists that power the metrics live in reviewed dbt project
configuration because the classification is business logic: it should be
visible in a pull request and changed only with stakeholder approval. The mart
keeps one headline flag, is_active, using the canonical definition from
Section 3, and carries the Section 1 signals as component counts (attempted
and engaging events) next to the per-type counts, so every number in the
standalone analysis can be reconciled from the mart without introducing a
second or third definition flag.

Delivered stays out of the mart on purpose. Email delivery is inferred
(a send with no matching failure on the same message and profile), and that
inference depends on a message-instance property the producer contract hasn't
confirmed. It lives in the standalone Section 1 analysis; promoting it to the
mart is a follow-up with its own model and its own review once the contract
question is answered.

### Materializations and scale

**`stg_events` — view.** A one-to-one projection that trims identifiers,
normalizes event-type case, and preserves the source properties map. Blank
identifiers are normalized to null on purpose: an empty string would silently
pass a not_null test and join to nothing, while a null fails tests and shows
up in profiling. A view avoids storing another complete copy merely for light
cleaning.

**`int_events_deduped` — incremental merge.** A full-history window over a
10B-event/day stream is not an acceptable recurring plan. The model processes a
configurable overlap on `created_at`, keeps the latest row per `event_id`, and
merges into an event-grain table. The overlap prevents equal-watermark rows from
being missed and absorbs normal replays. `initial_load_start` supports a bounded
bootstrap before older history is backfilled in controlled batches.

The correctness contract is that `event_id`, `company_id`, and `occurred_at`
are immutable for a logical event; a replay may correct other attributes. If a
producer can move an existing event to a different company or event-time week,
the pipeline must receive an explicit change feed containing both old and new
keys or run a targeted backfill of both aggregates.

**`fct_company_engagement_weekly` — incremental merge.** The output is small
relative to the stream—at most roughly one row per company per observed week.
New or corrected records identify touched `(company_id, week_start)` keys. The
model then rereads all deduplicated events for those keys and merges complete
aggregates. It never overwrites a weekly row with an aggregate calculated from
only the new batch. Because a late-arriving event simply touches its
company-week, corrections restate the affected week automatically; whether
restatements may touch a closed reporting quarter is the policy question
raised in Section 1.

The three-day ingestion overlap is a configurable starting point, not a claim
about production behavior. Because `created_at` represents ingestion time, an
event that occurred months ago but arrives today is still detected. Records
that arrive with a backdated `created_at` outside the overlap require the
contracted backfill process.

### Tests and monitoring

Generic tests cover:

- source and model `not_null` requirements;
- uniqueness of deduplicated `event_id`;
- uniqueness of the mart's stable `company_week_key`;
- accepted values on the classification and definition-version flags.

The featured custom domain test,
`assert_weekly_activity_consistency.sql`, fails when the headline flag or any
signal count stops reconciling with its component counts. Its event types are
intentionally hardcoded even though the classification is configuration:
changing the definition must consciously touch the test too, which turns an
accidental metric change into a reviewed one. Source freshness warns after 12
hours and errors after 24; those thresholds should be aligned to the
production SLA rather than treated as universal defaults.

The exercise allows up to two staging models. With a single source table a
second one would be invention; the natural second staging model is the
CRM/account source discussed in Section 1, once it exists. For the same
reason no company relationship test is included: inventing a dimension would
create false confidence. Once that source exists I would add an
effective-dated company relationship, eligibility filters, lifecycle fields,
and governed internal/test-account status.

### AI usage note

- **System:** Claude Code.
- **Initial prompt:** "Build a small production-minded dbt project for the
  supplied Snowflake event schema, including staging, incremental deduplication,
  a weekly engagement mart, tests, documentation, and late-arrival handling."
- **Human direction and validation:** I directed the design away from a
  full-history deduplication view, required the raw `properties` map to remain
  untouched, and required an ingestion-time overlap with complete touched-week
  recomputation. I decided the mart keeps a single canonical flag with the
  Section 1 signals as counts rather than extra flags, and I kept the inferred
  email-delivery signal out of the mart until the producer contract confirms
  the message identity it depends on, rather than shipping an inference as if
  it were confirmed.

---

## Section 3 — Refactor & Canonicalization

### 1. Canonical definition

I would make the v2 definition canonical: **a company is active in a week when
it has at least one `email_open`, `email_click`, or `sms_delivered` event.**

The decision is stakeholder-driven. Product Analytics needs a cross-channel
trend, and an email-open-only definition makes SMS-active customers invisible.
BI needs one number leadership can quote consistently. Using the existing v2
logic also separates the consolidation from a second, simultaneous metric
redesign.

The canonical definition in this consolidation remains the stated v2 logic.
The three-signal framework in Sections 1, 2, and 4 answers a broader stakeholder
question and should not be introduced as an unrequested third definition during
the migration. The weekly mart retains raw component counts, so compatibility
views can calculate the legacy and v2 flags without duplicating event processing.

### 2. Deprecation and migration plan

**Discovery and baseline (week 0).** Inventory dashboard, scheduled-query,
notebook, reverse-ETL, and ad hoc consumers using dbt lineage, BI metadata,
repository search, and Snowflake query history. Identify an owner and criticality
for each consumer. Produce a prior-quarter comparison between legacy and
canonical results; expected differences should be attributable to email clicks
and SMS deliveries.

**Ship without semantic breakage (week 1).** Release the canonical mart and
replace the two physical legacy models with compatibility views:

- `legacy_company_activity` continues to calculate its legacy flag from
  `email_open_events > 0` in the canonical mart.
- `company_engagement_v2` calculates the canonical flag from
  `email_open_events + email_click_events + sms_delivered_events > 0`.

Both views receive deprecation metadata, an owner, migration instructions, and
a target retirement date. Existing dashboards continue producing the same
semantics while teams move deliberately.

**Migrate and validate (weeks 2–3).** Meet each dashboard owner, repoint the
consumer to the canonical relation, compare a fixed historical period, and
record approval. Differences outside the documented definition change pause
that consumer’s migration until they are reconciled.

**Observe and retire (week 4 or later).** Require zero observed queries for at
least one complete business cycle, owner confirmation, successful scheduled
runs, and no unresolved exceptions. Then remove the compatibility views in a
scheduled release. A critical unresolved dependency receives a documented,
time-bounded exception rather than an unannounced failure.

Migration is complete when all known consumers use the canonical model, the
comparison report is reconciled, owners have signed off, query history shows no
legacy access during the observation window, and the compatibility views are
removed.

### 3. Backwards compatibility and tradeoffs

Compatibility views preserve both schema and meaning while migrations occur.
This is safer than immediately changing the number returned under a legacy
name. The tradeoff is temporary dual semantics: leadership could still see two
different results during the migration. I would mitigate that with prominent
deprecation notices, a single comparison report, and a firm but risk-based
sunset date.

Keeping the component counts in the canonical mart is intentional. They allow
legacy compatibility without retaining duplicate event-processing logic. They
also make future definition proposals testable without creating another
independent source of truth.

### 4. Documentation changes

The dbt docs will contain the canonical definition, grain, qualifying event
types, UTC week convention, definition version, owner, freshness expectation,
and a changelog. Legacy docs will become deprecation pages that state the old
definition, canonical replacement, migration instructions, and retirement
status. Stakeholder documentation will explain the definition in business
language and show the component fields used for reconciliation.

### AI usage note

- **System:** Claude.
- **Initial prompt:** “Rewrite the consolidation plan so it preserves metric
  semantics during migration, remains aligned with the shipped mart, and gives
  concrete completion signals.”
- **Human direction and validation:** I rejected a day-one silent semantic
  change under the legacy name and required compatibility views to preserve old
  behavior temporarily. I chose an owner-based, evidence-driven retirement
  plan rather than allowing either indefinite duplication or avoidable job
  failures.

---

## Section 4 — Stakeholder Documentation

Deliverable: [`stakeholder_doc.md`](stakeholder_doc.md).

### AI usage note

- **System:** Claude.
- **Initial prompt:** “Write a one-page, skimmable user guide for a PA or BI
  partner using the weekly company-engagement mart; avoid dbt jargon and make
  the caveats actionable.”
- **Human direction and validation:** I required the guide to distinguish
  attempted activity and recipient engagement while pointing delivery
  questions to the standalone analysis, explain source limitations, warn
  about calendar-quarter boundaries, and tell users exactly which signal is
  the recommended headline.

---

## Section 5 — AE/DE Collaboration on Table Design

### Assessment of the draft

`days(occurred_at)` is a sound first partition transform. Every described use
case is time-bounded, so day partitioning lets engines skip dates outside a
dashboard, backfill, or feature window. At 10B events/day, a day is not an
obviously small partition.

Day-only partitioning does not address Product Analytics’ dominant query shape:
`company_id` plus a date range. A single-company quarter can still touch files
for every company in 90 daily partitions. Direct identity partitioning by
`company_id` would be worse—approximately 50,000 possible company partitions
per day would create excessive partition and file cardinality.

### Proposed starting specification

```sql
PARTITIONED BY (
    days(occurred_at),
    bucket(256, company_id)
)
```

I would treat 256 buckets as a benchmarkable starting point, not an immutable
answer. The selection should be tested against 128 and 512 buckets using real
record widths, arrival patterns, target file sizes, and representative queries.

- **Product Analytics:** day pruning limits the date range; company hashing
  limits reads to one company bucket per day.
- **BI:** executive rollups across all companies still scan the selected days.
  That is expected; the weekly dbt mart is the primary optimization for repeated
  company-level aggregates.
- **Data Science:** per-profile history reads are not well served by company
  bucketing (one profile can appear under multiple companies), so they would
  need their own arrangement; I would design that with DE if the demand is
  material.

### Tradeoffs and negotiation

Company bucketing improves the common PA lookup but increases the number of
active write destinations per day. More buckets can improve pruning while
reducing the amount of data available to form well-sized files, increasing
write amplification and compaction work. Fewer buckets lower maintenance cost
but make company-scoped queries scan more unrelated data.

I would bring a small workload suite to DE: one company for 90 days, a cohort of
companies for one month, a full-market weekly rollup, and a set of profile
histories. We would compare scan bytes, planning time, file counts, write
throughput, and compaction cost across candidate bucket counts. The final choice
belongs in a shared design record because AE receives the query benefit while
DE owns much of the write and maintenance cost.

### Data contract

I would request the following versioned contract between DE and AE:

- **Schema and semantics:** field types, UTC timestamp convention, nullability,
  event-type ownership, and additive versus breaking schema-change rules.
- **Identity and corrections:** `event_id` identifies one logical event;
  `company_id` and `occurred_at` are immutable for replays, or the change feed
  supplies both old and new keys.
- **Freshness and completeness:** agreed p95/p99 ingestion-lag SLOs, a
  “complete through” watermark or snapshot identifier, and an alert when a
  partition is late, reopened, or backfilled.
- **Quality signals:** row counts, duplicate rates, null rates for required
  fields, producer/schema version, and reconciliation totals by day.
- **Change management:** notice and compatibility windows for renames, type
  changes, removals, partition evolution, and historical restatements.
- **Privacy and governance:** classification of `profile_id` and event
  properties, retention rules, masking or row-access requirements, approved
  roles, auditability, and incident ownership.
- **Operations:** named DE and AE owners, backfill procedure, retry/idempotency
  expectations, and escalation paths.

The important completeness concept is a watermark, not a claim that an event
day can never change. Late events are expected; the contract defines when they
are normal, how they are signaled, and how downstream aggregates are restated.

### AI usage note

- **System:** claude.
- **Initial prompt:** “Give a concrete Iceberg partition recommendation for the
  supplied PA, BI, and per-profile Data Science access patterns, including
  tradeoffs and a data contract.”
- **Human direction and validation:** I required a specific starting partition
  spec, rejected direct partitioning by high-cardinality company or profile
  identifiers, and kept the bucket count subject to workload benchmarks. I also
  required the proposal to address the stated per-profile consumer and privacy
  controls rather than inventing a different Data Science use case.