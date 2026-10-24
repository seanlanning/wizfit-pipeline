# WizFit Client Progress Pipeline

An end-to-end data pipeline built on **Databricks** with **PySpark** and **Delta Lake**, using a
bronze → silver → gold (medallion) architecture to turn messy online-coaching data into
business-ready tables and a dashboard.

## The problem
I run WizFit, a 1-on-1 online fitness coaching business. Client data comes in from weekly
check-in forms, nutrition logs, and workout logs, and it's messy: mixed date formats, weights
entered in the wrong unit, duplicate submissions, typos in IDs. I wanted one reliable place to
answer:

- Which clients are on track toward their goal?
- Who is falling off and needs outreach *before* they quit?
- Are clients actually getting stronger?

> ✏️ **Write 2–3 sentences here in your own words** about why this matters to your business.

## Architecture

```
CSV files (Unity Catalog Volume)
        │
   🥉 BRONZE   raw data landed as strings + ingest timestamp + source file
        │
   🥈 SILVER   cleaned, typed, deduplicated, validated
        │
   🥇 GOLD     gold_client_progress · gold_weekly_trends · gold_strength_progress
        │
   📊 Databricks Dashboard
```

## Data quality issues handled

| Issue in raw data | How the pipeline fixes it |
|---|---|
| Client IDs with spaces / lowercase | Trimmed and upper-cased |
| Two date formats (`2026-06-04`, `06/04/2026`) | Pattern-matched to the right parser |
| Weights entered in kg instead of lbs | Detected (< 50% of start weight) and converted |
| Duplicate check-in submissions | Deduplicated on client + date |
| Negative calorie entries | Filtered out and counted |
| Inconsistent exercise names (`DEADLIFT`) | Normalized to title case |

A data quality report prints counts for each issue on every run.

## Gold tables

- **gold_client_progress**: one row per client: weight change, % to goal, check-in rate,
  logging rate, avg protein, and a status flag (🟢 on track / 🟡 slipping / 🔴 at risk)
- **gold_weekly_trends**: weekly weight and training volume per client, built for charting
- **gold_strength_progress**: top load per exercise, first week vs last week

## Tech stack
Databricks Free Edition · PySpark · Spark SQL · Delta Lake · Unity Catalog Volumes · Databricks Dashboards

## How to run it
1. Sign up for Databricks Free Edition.
2. Create a Volume: Catalog → `workspace` → `default` → Create → Volume → name it `wizfit_raw`.
3. Upload the 4 CSVs from `/data` into the Volume.
4. Import `wizfit_pipeline.py` (Workspace → Import) and click **Run all**.

## Results
> ✏️ **Fill in after you run it.** Example: "The pipeline flagged X of 15 clients as at-risk,
> converted Y mis-entered weights, and removed Z duplicate check-ins."
>
> 📸 Add a screenshot of your dashboard here.

## What I'd add next
- Schedule as a weekly Lakeflow Job
- Rebuild the silver layer as a Lakeflow Declarative Pipeline with data-quality expectations
- Connect directly to the check-in form instead of CSV exports

## Note on the data
The data in `/data` is synthetic sample data modeled on my real coaching workflow. No real
client information is included.
