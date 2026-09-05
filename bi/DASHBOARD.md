# Pulse Marketplace Overview

Run `docker compose run --rm metabase-setup` after the dbt pipeline has populated
its marts. The script initializes a fresh instance, or logs into the existing
instance, verifies the warehouse and all six SQL queries, and reconciles one
`Pulse Marketplace` collection, six questions, and this dashboard. It prints the
dashboard URL. Use the admin credentials configured in `.env`.

The script targets the pinned Metabase v0.63.16.5 API. Run only one setup process
at a time. Reruns reuse object IDs and refresh managed SQL and filter mappings;
existing layouts and unrelated cards are retained. Renaming a managed object
makes it a different object. Duplicate managed names fail clearly. A failed
request to create an object is not automatically retried; rerun the script so
it can discover any object created before the response was lost.

## Dashboard layout

| Position | Saved question | Presentation |
| --- | --- | --- |
| Top left | Revenue overview | Table: revenue, orders, units, weighted AOV by currency |
| Top right | Revenue trend | Line: date on X, revenue on Y, currency series |
| Middle left | Funnel | Daily country stage counts and four adjacent-stage rates |
| Middle right | Top customers by units | Lifetime purchase-unit ranking, payments and distinct orders |
| Bottom left | Top products by units | Lifetime unit ranking, seller, payments and distinct customers |
| Bottom right | Geography | Country stage counts and weighted conversion rates |

## Filters and currency rules

Start date and end date are inclusive. Date/currency apply to the two revenue
cards; date/country apply to funnel and geography. Customer/product cards are
lifetime operational measures and have no compatible filter dimensions.
Currency and country accept exact codes, for example `EUR` and `DE`.

The plain SQL files run directly in PostgreSQL. Provisioning adds optional,
parameterized Metabase variables after the mart FROM clause, before grouping
and limiting, and connects those variables to dashboard filters.

Revenue always remains grouped by currency, even with no filter selected.
There is no exchange-rate model. The upstream customer/product marts contain
currency-less lifetime revenue: the BI queries intentionally omit those amounts
and revenue ranks, and rank purchase units instead. Do not add their revenue
columns to this dashboard. No country-revenue mart exists, so geography shows
nonmonetary funnel measures only. Rates are event-count ratios, not session or
customer cohort conversion; aggregated rates divide summed counts, not averaged
percentages. Zero denominators produce NULL.

## Manual recovery

If a future Metabase version changes its API, use New > Collection to create
`Pulse Marketplace`. For each file in `bi/queries`, choose New > SQL query,
select `Pulse Analytics Warehouse`, paste the SQL and save with the names above.
Create a dashboard named `Pulse Marketplace Overview`, add the six questions,
and arrange them as above. Revenue trend uses event_date and currency as
breakouts, gross_revenue as its metric. Tables keep currency visible.

For filterable manual questions, add the following immediately after FROM:

```sql
WHERE 1=1
[[AND event_date >= {{start_date}}]]
[[AND event_date <= {{end_date}}]]
```

Use Date variables. Add `[[AND currency = {{currency}}]]` as Text to revenue
questions, or `[[AND country = {{country}}]]` as Text to funnel/geography.
In dashboard edit mode add two Single date filters and Text filters for currency
and country; map each to the matching question variable. Do not map filters to
the lifetime customer/product cards. Save and test both filtered and unfiltered
results. Never configure the analytics connection to either metadata database.
