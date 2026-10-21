# NorthBridge Bank: End-to-End Banking Data Pipeline on Databricks

A medallion-architecture data engineering project built on **Databricks Free Edition** (Unity Catalog, Auto Loader, Delta Lake, Workflows, Lakeflow Declarative Pipelines). It models the data platform of a fictional retail bank, **NorthBridge Bank**, and turns messy, scattered source data into trusted, analytics-ready tables.

> **Status:** Week 1 of 8 (setup). See the [roadmap](#roadmap) for progress.

---

## Problem statement

NorthBridge Bank's data lives in separate systems that do not agree with each other:

- **Core banking:** customers and accounts
- **Payment gateway:** card, transfer and cash transactions
- **General ledger:** the finance team's book of record

Today this causes four problems:

1. **Bad data reaches reports.** Duplicates, nulls and malformed records are not caught.
2. **Reconciliation is manual.** Finance cannot easily tell which gateway transactions are missing from, or mismatched with, the ledger.
3. **Fraud is spotted late.** Suspicious transactions are found after the fact.
4. **PII is exposed.** Analysts can see raw customer identifiers.

**Goal:** build an automated, incremental pipeline that ingests all sources, validates and quarantines bad rows, masks PII, flags likely fraud, reconciles gateway against ledger, and serves the results to a dashboard.

---

## Architecture

```
  SOURCES                  BRONZE                 SILVER                   GOLD                  SERVE
 ----------           ---------------        -----------------       -----------------       -------------
 Core banking  --\                                                    Daily KPIs
 (customers)      \                         Cleaned, deduped         Fraud flags
 Payment       ----> Auto Loader --> Raw --> Validated, typed  -->   Gateway vs ledger  -->  Dashboard
 gateway          /   (incremental)  Delta   PII masked              reconciliation
 (transactions)  /                           SCD2 customer dim       Customer 360
 General ledger -/                           Rejected-rows table
```

Cross-cutting: **Unity Catalog** governance, **Workflows** orchestration, **Lakeflow Declarative Pipelines** (rebuild of the core flow), data quality expectations, and an ops schema for run and quality logging.

---

## Tech stack

| Area | Tools |
|---|---|
| Platform | Databricks Free Edition (serverless) |
| Storage / format | Delta Lake, Unity Catalog volumes |
| Ingestion | Auto Loader (`cloudFiles`) |
| Processing | PySpark, Spark SQL |
| Orchestration | Databricks Workflows, Lakeflow Declarative Pipelines |
| Source data | PaySim synthetic transactions (Kaggle) plus generated customer and ledger data |
| Version control | GitHub, Databricks Git folders |

---

## Catalog layout

Catalog: `northbridge`

| Schema | Purpose |
|---|---|
| `raw` | Landing zone. Volumes: `staging`, `landing`, `_checkpoints`, `_schemas` |
| `bronze` | Raw, append-only Delta tables as ingested |
| `silver` | Cleaned, validated, deduplicated, PII-masked data, plus the rejected-rows table |
| `gold` | Business-ready aggregates, fraud flags, reconciliation and customer 360 |
| `ops` | Pipeline run logs and data quality results |

Setup script: [`sql/00_setup.sql`](sql/00_setup.sql)

---

## Data

- **Transactions:** the PaySim dataset from Kaggle, used as the payment gateway feed.
- **Customer master:** generated deterministically from the transaction account IDs.
- **General ledger:** derived from the gateway data with deliberate defects (missing entries, amount mismatches, duplicates, malformed records) so reconciliation and data quality logic have real work to do.

> Raw data files are not committed to this repo. See `docs/data-prep.md` for how to regenerate them.

---

## Repository structure

```
northbridge-databricks-pipeline/
├── notebooks/      # Setup, data prep, bronze, silver, gold notebooks
├── pipelines/      # Lakeflow Declarative Pipelines definitions
├── sql/            # DDL and setup scripts (00_setup.sql, ...)
├── workflows/      # Job / workflow definitions
├── docs/           # Design notes, data prep, screenshots
└── README.md
```

---

## Roadmap

| Week | Focus | Status |
|---|---|---|
| 1 | Workspace, GitHub, catalog structure, README | Completed |
| 2 | Data prep: PaySim download, customer master, ledger with defects | Completed |
| 3 | Bronze: Auto Loader ingestion | Completed |
| 4 | Silver: cleaning, dedupe, rejected rows, PII masking | Completed |
| 5 | Silver: SCD2 customer dimension | Completed |
| 6 | Gold: KPIs, fraud flags, reconciliation, customer 360 | In progress |
| 7 | Orchestration with Workflows, rebuild in Lakeflow Declarative Pipelines | Not started |
| 8 | Dashboard, documentation, portfolio write-up | Not started |

---

## How to run

1. Create a Databricks Free Edition workspace.
2. Clone this repo into a Databricks Git folder.
3. Run `sql/00_setup.sql` to create the catalog, schemas and volumes.
4. Follow the notebooks in order (added week by week).

---

## Key design decisions

*(Fill in as you build. Examples to document: why Auto Loader over batch reads, how rejected rows are handled, SCD2 approach, fraud rule thresholds, reconciliation tolerance.)*

---

## Results and screenshots

*(Added in week 8: pipeline DAG, reconciliation summary, dashboard.)*

---

## Author

**K Siddharth Pattnaik**
B.Tech CSE (Business Systems), VIT-AP University

- GitHub: [pattnaiksiddharth](https://github.com/pattnaiksiddharth)
- LinkedIn: [k-siddharth-pattnaik](https://www.linkedin.com/in/k-siddharth-pattnaik-4844443ab)
