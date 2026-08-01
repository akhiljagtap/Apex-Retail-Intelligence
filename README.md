# Apex Retail Intelligence — End-to-End Data Engineering Pipeline

**Celebal Technologies | CEI'26 Internship Programme | Major Project**

## Overview

An end-to-end data engineering pipeline built on Databricks that transforms raw,
unstructured retail data (customer, product, and sales CSVs) into a business-ready
Star Schema for BI reporting — following the **Medallion Architecture**
(Bronze → Silver → Gold) using Apache Spark (PySpark), Delta Lake, and Unity Catalog.

The pipeline is designed to be:
- **Fault-tolerant** — recovers gracefully from upstream data anomalies
- **Idempotent** — safe to re-run without duplicating or corrupting data
- **Auditable** — every transformation step is logged and verifiable

## Architecture

```
Raw CSV → Raw Zone → Landing (Parquet + Audit) → Bronze (Delta) → Silver (Cleansed) → Gold (Star Schema) → KPIs
```

| Layer | Purpose |
|---|---|
| **Raw** | Ingest CSVs as-is, all columns as String, organized by historical/incremental |
| **Landing** | Convert to Parquet, validate row counts against audit files (PASS/FAIL) |
| **Bronze** | Convert to Delta format, inject `ingested_at` metadata, append-only |
| **Silver** | Data quality rules, Delta MERGE, SCD Type 1 & 2, surrogate key generation |
| **Gold** | Star schema (4 dimensions + 1 fact table), registered in Unity Catalog |

## Tech Stack

- **Compute:** Databricks (Free Edition), Apache Spark / PySpark
- **Storage:** Delta Lake, Unity Catalog Volumes
- **Governance:** Unity Catalog (managed tables under `workspace.GOLD_tables`)
- **Reporting:** PySpark DataFrame / Spark SQL (no external BI tools, per project constraints)

## Key Engineering Decisions

**Three different incremental strategies, one pipeline:**
- **Customer → SCD Type 2** — profile changes preserve history. Old record expired
  (`is_current=False`, `effective_end_date` set), new version inserted as active.
- **Product → SCD Type 1** — changes overwritten in place via `whenMatchedUpdate`,
  no history retained.
- **Sales → Immutable Ledger** — `whenNotMatchedInsertAll()` only, no matched clause.
  Transactions can never be altered once written.

**No watermarking** — all incremental processing uses Delta Lake MERGE semantics
keyed on business primary keys (`customer_id`, `product_id`, `transaction_id`),
per the explicit project constraint.

**Surrogate keys** (`customer_sk`, `product_sk`, `sales_sk`) are generated via
`row_number()` **after** all inserts/merges complete for each table, ensuring keys
map to the final deduplicated record set rather than an intermediate state.

**Idempotency** is enforced by keying every MERGE on business primary keys — matched
records are updated in place or skipped (Sales), unmatched records inserted exactly
once. Re-running any script does not duplicate data.

## Star Schema (Gold Layer)

| Table | Type | Contents |
|---|---|---|
| `dim_customer` | Dimension | Customer attributes + SCD Type 2 history |
| `dim_product` | Dimension | Product catalogue details |
| `dim_promotion` | Dimension | Promotion types and identifiers |
| `dim_date` | Dimension | Freshly generated calendar attributes |
| `fact_sales` | Fact | Transaction metrics + surrogate keys linking all dimensions |

All five tables are registered as managed Delta tables in Unity Catalog under
`workspace.GOLD_tables`.

## Business KPIs (computed inline in PySpark)

1. **Net Margin by Region** — gross revenue minus discounts, by store region
2. **Average Order Value (AOV) by Promotion** — which promotion types drive the highest cart values
3. **Demographic Churn Heatmap** — churn rate by state and loyalty program membership
4. **Product Quality Index** — return rates by product category
5. **Store Traffic by Hour** — busiest transaction hours/days for foot traffic


2. Run notebooks in order: `01_raw_landing` → `02_bronze` → `03_silver` → `04_gold_kpi`
3. Each notebook is self-contained and idempotent — safe to re-run

## Author

Akhil — Data Engineering Intern, Celebal Technologies (CEI'26)
