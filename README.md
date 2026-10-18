# NexusMart Data Analytics Pipeline

End-to-end analytics pipeline for **NexusMart**, a Pakistani online marketplace (CodeConnect'26 Data Analytics Module).
Raw, inconsistent CSV files go in; a clean PostgreSQL **star schema**, an automated **Apache Airflow** pipeline, and an interactive **Power BI** dashboard come out.

```
Raw CSV  ->  Airflow  ->  Python cleaning  ->  PostgreSQL star schema  ->  Power BI  ->  Insights
```

| Layer | Technology |
|---|---|
| Cleaning and validation | Python 3.12, pandas |
| Orchestration | Apache Airflow 2.10 (LocalExecutor) |
| Warehouse | PostgreSQL 16 |
| Modelling and metrics | SQL (star schema) and DAX |
| Reporting | Power BI Desktop |
| Environment | Docker Compose |

---

## Table of contents

1. [Architecture](#architecture)
2. [Repository layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Quick start](#quick-start)
5. [Pipeline tasks](#pipeline-tasks)
6. [Data cleaning rules](#data-cleaning-rules)
7. [Data model](#data-model)
8. [Metric definitions](#metric-definitions)
9. [Power BI dashboard](#power-bi-dashboard)
10. [Verifying a run](#verifying-a-run)
11. [Design decisions](#design-decisions)
12. [Known limitations](#known-limitations)
13. [Troubleshooting](#troubleshooting)

---

## Architecture

```
 data/raw/*.csv (9 files)
        |
        v
 Airflow DAG: nexusmart_pipeline (@daily)
 extract -> validate_raw -> clean -> load_staging -> build_dimensions -> build_fact -> quality_checks
        |                  |                 |
        v                  v                 v
 data/processed/*.csv   data/rejects/    PostgreSQL (stg, dw, ops, mart)
 reports/dq_*.csv|json   rejects.csv            |
                                                v
                                  Power BI (Import mode, dw schema only)
```

PostgreSQL schemas:

| Schema | Purpose |
|---|---|
| `stg` | Staging tables loaded from the cleaned CSVs (truncated and reloaded on each run) |
| `dw` | Star schema: 1 fact table and 5 dimensions |
| `ops` | Operational metadata: rejects, data-quality logs, pipeline run log, quality-check results |
| `mart` | Analytical views (for example the seller scorecard) |

## Repository layout

```
nexusmart/
|-- dags/
|   `-- nexusmart_pipeline.py     # Airflow DAG (7 tasks)
|-- src/nexusmart/
|   |-- cleaning.py               # Cleaning engine: every rule, with before/after logging
|   |-- steps.py                  # Task implementations (also runnable from the CLI)
|   `-- db.py                     # Database helpers (credentials from environment only)
|-- sql/
|   |-- 01_schema.sql             # stg, dw, ops schemas and tables
|   |-- 02_dimensions.sql         # Dimension upserts (incl. Unknown members)
|   |-- 03_fact.sql               # FactSales build
|   `-- 04_analytics.sql          # RFM segments and seller quality score
|-- dax/nexusmart_measures.dax    # All Power BI measures (bulk import)
|-- powerbi/                      # NexusMart_Dashboard.pbix and theme
|-- reports/                      # Data-quality logs, summaries, exported PDFs
|-- docs/                         # Star schema diagram, data quality report
|-- data/
|   |-- raw/                      # Place the 9 source CSVs here (git-ignored)
|   |-- processed/                # Cleaned CSVs (generated)
|   `-- rejects/                  # Quarantined records with reason codes (generated)
|-- docker/init-db.sql            # Creates the `nexusmart` warehouse database
|-- docker-compose.yml
|-- Dockerfile
|-- requirements.txt
`-- .env                          # Local credentials (git-ignored)
```

## Prerequisites

- Docker Desktop (or Docker Engine) with Docker Compose v2, **8 GB RAM or more recommended**
- Power BI Desktop (Windows), only needed to open the dashboard
- Ports **8080** (Airflow) and **5433** (PostgreSQL) free

## Quick start

All commands are run from the project root. PowerShell is shown; use the same `docker compose` commands in any shell.

**1. Place the source data**

Copy the nine CSV files (`customers`, `products`, `sellers`, `locations`, `orders`, `order_items`, `payments`, `shipping`, `returns`) into `data/raw/`.

**2. Create the environment file**

```powershell
@"
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
WAREHOUSE_DB=nexusmart
"@ | Set-Content -Encoding ascii .env
```

These are local demo credentials. Change them for any shared environment.

**3. Build and initialise**

```powershell
docker compose build
docker compose up airflow-init
```

`airflow-init` should finish with `exited with code 0`.

**4. Start the stack**

```powershell
docker compose up -d
docker compose ps
```

Wait about 45 seconds. `postgres` should be `healthy`, and the webserver and scheduler `Up`.

**5. Run the pipeline**

Option A: Airflow UI. Open <http://localhost:8080> (user `admin`, password `admin`), open `nexusmart_pipeline` and press **Trigger**.

Option B: command line.

```powershell
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-scheduler airflow dags trigger nexusmart_pipeline
docker compose exec airflow-scheduler airflow dags list-runs -d nexusmart_pipeline
```

A full run takes roughly 4 to 6 minutes. All 7 tasks should turn green and the run state should be `success`.

Option C: without Airflow (debugging).

```powershell
docker compose exec airflow-scheduler python /opt/airflow/src/nexusmart/steps.py all
```

> Do not run option C while a DAG run is in progress: both truncate the staging tables.

**6. Connect Power BI** (see [Power BI dashboard](#power-bi-dashboard)).

**7. Stop**

```powershell
docker compose down        # keep data
docker compose down -v     # also delete the PostgreSQL volume
```

## Pipeline tasks

| Task | Responsibility |
|---|---|
| `extract` | Read the nine raw CSV files and record row counts |
| `validate_raw` | Check file availability and expected columns; extra columns are reported, missing columns fail the run |
| `clean` | Apply all standardisation and cleaning rules; write processed CSVs, `rejects.csv` and the data-quality logs |
| `load_staging` | Truncate and bulk-load (`COPY`) cleaned data into `stg`, plus rejects and DQ logs into `ops` |
| `build_dimensions` | Upsert the five dimensions (SCD Type 1) including `Unknown` members; refresh RFM segments and seller quality score |
| `build_fact` | Rebuild `dw.fact_sales` at order-line grain, with returns aggregated per line |
| `quality_checks` | Run 37 automated checks; any **critical** failure fails the DAG run |

Operational behaviour:

- **Configuration**: file paths and database URL come from environment variables (`NEXUS_RAW_DIR`, `NEXUS_PROCESSED_DIR`, `NEXUS_REJECTS_DIR`, `NEXUS_REPORTS_DIR`, `NEXUS_SQL_DIR`, `NEXUS_DB_URL`), set in `docker-compose.yml`.
- **Idempotent**: staging is truncated and reloaded, dimensions are upserted on business keys, and the fact table is rebuilt, so repeated runs never create duplicates.
- **Retries**: 2 retries, 30 seconds apart; 30-minute execution timeout per task; `max_active_runs=1`.
- **Logging**: every task writes records read, rejected and loaded to `ops.pipeline_run_log`.
- **Schedule**: `@daily`, `catchup=False`.

## Data cleaning rules

Every rule is implemented in `src/nexusmart/cleaning.py` and logged with before/after counts in `reports/dq_rules_log.csv` and `reports/dq_table_counts.csv`. **Nothing is silently deleted**: removed, merged or nulled records go to `data/rejects/rejects.csv` (and `ops.rejects`) with a reason code.

| Area | Issue | Treatment |
|---|---|---|
| Duplicates | Exact duplicate rows in `orders`, `payments`, `returns` | Removed (first kept), logged as `EXACT_DUPLICATE_ROW` |
| Duplicate entities | Same customer or product under different IDs | Merged to the lowest ID using the **same name AND same email/phone** (customers) or name + category + price within 10% (products); never on name alone. Mappings kept in `customer_map` and `product_map` |
| Labels | Category, province, courier, payment method, order/delivery/return status, channel, gender, tier | Standardised to a canonical set via mapping tables (typos such as `DELIVERD` handled) |
| Numbers | Prices written as `"Rs. 2,290"`, quantities as words (`one`, `two`) | Parsed to numeric types |
| Dates | Mixed formats, impossible dates (`00/00/2024`, `2024-02-30`), out-of-range dates | Parsed as day/month/year for slashes; repaired from payment date (non-COD), then shipped date minus 1 day, then neighbouring order IDs; otherwise quarantined |
| Outliers | Sentinel prices (9999999), zero or negative prices, quantity 500 | Replaced with the product's median valid price, or back-solved from `order_total`; flagged (`price_repaired`, `qty_repaired`) |
| Discounts | Blank, negative, or larger than line gross | Recomputed as line gross x order discount % |
| Missing values | Brand, category, cost price, weight, rating, ship city, dates | Decided per column: inferred, imputed with category/tier median, set to `Unknown`, or left NULL; imputation flags are stored (`cost_imputed`, `rating_imputed`, ...) |
| Broken references | Order items with no order, returns with no order item, unknown customer/product/seller/city | Orphans quarantined; unknown dimension keys mapped to an `Unknown` member (-1 / `UNKNOWN`) |
| Returns | Refund greater than line value, blank refund on valid return | Capped at line net value (`refund_capped`) |

Validation on the real dataset: cleaned line totals reconcile with `order_total` for **99.27%** of 159,512 checked orders (91.9% before cleaning).

## Data model

One fact table and five dimensions. **Dimensions are not linked to each other.** See `docs/star_schema.png`.

**`dw.fact_sales`**, grain: one row per order line item (315,143 rows).

| Group | Columns |
|---|---|
| Keys | `date_key`, `customer_key`, `product_key`, `seller_key`, `location_key`, `return_date_key`, `order_id`, `order_item_id` |
| Measures | `quantity`, `unit_price`, `gross_amount`, `discount_amount`, `net_amount`, `cost_amount`, `shipping_fee`, `shipping_days`, `returned_amount` |
| Flags | `is_cancelled`, `is_delivered`, `is_kept`, `is_returned` |
| Attributes | `order_status`, `payment_method`, `channel`, `return_reason`, `return_status`, `discount_pct`, `courier`, `delivery_status`, `on_time_status`, `delivery_band`, `discount_band`, `is_first_order` |

Notes:

- Returns are **aggregated per order line** using valid (non-Rejected) returns only, so the grain is never violated.
- `shipping_fee` is recorded per order; it is allocated across the order's lines so it is never double counted.

**Dimensions**: `dim_date` (day, week, month, quarter, year, weekend, Ramadan, Eid-ul-Fitr, Eid-ul-Adha, 14 August, 11.11, Black Friday, `festival`), `dim_customer` (deduplicated, SCD Type 1, RFM segment), `dim_product`, `dim_seller` (tier, quality score, rank, band), `dim_location`. Each has an `Unknown` member (`dim_date` uses key -1 with date 1900-01-01 so it can be marked as a date table).

## Metric definitions

Implemented exactly as defined in the brief (all values in PKR).

| Metric | Definition |
|---|---|
| Gross Sales | Sum of quantity x unit price over all lines |
| NMV | Sum of quantity x unit price minus discount, non-cancelled lines (In Transit counts) |
| Net Revenue | NMV minus refunds from valid returns |
| Return Rate | Lines with a valid return / delivered lines |
| Cancellation Rate | Cancelled orders / all orders after duplicate removal |
| COGS / Margin | `cost_price` x quantity on kept lines / Net Revenue minus COGS |
| AOV | NMV / non-cancelled orders |
| Active Customers | Distinct customers with at least one non-cancelled order |

Additional analytics (in `sql/04_analytics.sql`):

- **RFM segmentation**: reference date 2025-07-01; recency, frequency and monetary value scored 1 to 5 (quintiles) and mapped to segments such as Champions, Loyal Customers, At Risk and Hibernating.
- **Seller quality score** (0 to 100): revenue 30%, return rate 30%, on-time delivery 25%, cancellation 15%, each as a percentile rank. Sellers with fewer than 30 order lines are scored but not ranked.
- **On-time threshold**: delivered within **5 days** of shipping. Median delivery is 3 days (p90 is 6), and the return rate rises from about 4.8% to about 6.9% beyond 5 days.
- **Observed CLV**: Net Revenue per active customer over the data period. **Repeat purchase rate**: customers with 2 or more non-cancelled orders / active customers.

## Power BI dashboard

The report connects to the **warehouse tables** (not the CSVs), in Import mode, and uses no Power Query cleaning.

1. In Power BI Desktop: **Get data > PostgreSQL database**, server `localhost:5433`, database `nexusmart`, user `airflow`, password `airflow`. If prompted about encryption, allow the unencrypted local connection.
2. Load the six `dw` tables: `dim_customer`, `dim_date`, `dim_location`, `dim_product`, `dim_seller`, `fact_sales`.
3. Rename them to `DimCustomer`, `DimDate`, `DimLocation`, `DimProduct`, `DimSeller`, `FactSales`.
4. Relationships: `FactSales` (many) to each dimension (one), single direction. `date_key` to `DimDate` is active; `return_date_key` to `DimDate` is inactive. No dimension-to-dimension relationships.
5. Mark `DimDate` as the date table (`full_date`).
6. Open **DAX query view**, paste `dax/nexusmart_measures.dax`, run, then **Update model with changes** to import all 57 measures.
7. Open `powerbi/NexusMart_Dashboard.pbix`, or build from the model, and press **Refresh** after each pipeline run.

Pages: Executive Overview, Time and Festival Analysis, Product and Category, Geography, Customers (RFM and cohort), Seller Performance, Returns and Cancellations, plus Seller Detail (drill-through) and Tooltip helper pages. Interactivity: five synced slicers (date, category, region, seller tier, customer segment), drill-down, drill-through and a Reset filters bookmark.

## Verifying a run

```powershell
# DAG run state
docker compose exec airflow-scheduler airflow dags list-runs -d nexusmart_pipeline

# Per-task read / rejected / loaded counts
docker compose exec postgres psql -U airflow -d nexusmart -c "select task,status,records_read,records_rejected,records_loaded from ops.pipeline_run_log order by id desc limit 7"

# Quality checks for the latest run (expect 37 | 37)
docker compose exec postgres psql -U airflow -d nexusmart -c "select count(*), count(*) filter (where passed) from ops.quality_check_results where run_id = (select run_id from ops.pipeline_run_log order by id desc limit 1)"
```

Expected headline numbers on the provided dataset:

| Check | Value |
|---|---|
| `dw.fact_sales` rows | 315,143 |
| Net Revenue / NMV | 1,142,780,315 / 1,200,386,381 |
| Orders (non-cancelled) / all orders | 145,338 / 160,000 |
| Return Rate / Cancellation Rate | 5.148% / 9.164% |
| Rejects by reason | `EXACT_DUPLICATE_ROW` 6,496; `DUPLICATE_CUSTOMER_MERGED` 2,556; `ORPHAN_ORDER_ITEM` 139; `DUPLICATE_PRODUCT_MERGED` 26 |

The 37 quality checks cover row conservation, fact grain, foreign keys, NMV reconciliation, shipping fee not double counted, refunds matching valid returns, and dimensions not being linked to each other. Critical failures stop the run.

## Design decisions

- **Cleaning in Python, not Power BI**: all cleaning is code-driven and logged. Power Query is used only to load.
- **Config-driven mappings**: label maps and thresholds sit at the top of `cleaning.py`; `validate_raw` fails fast on a missing column and reports extra columns, so a changed schema is caught early instead of silently loaded.
- **Merge, never drop**: duplicate customers/products are merged with a mapping table so related records still link; unrepairable rows are quarantined with a reason code.
- **Unknown members**: broken references keep their sales in the totals (mapped to `Unknown`) instead of disappearing.
- **Flags over silent fixes**: every repaired or imputed value has a boolean flag in the cleaned data.
- **Quality gate**: the DAG fails on a critical check, so bad data never reaches the dashboard unnoticed.

## Known limitations

- **Imputed values**: some `cost_price` values (missing or above 120% of list price) are imputed from the category cost-to-list ratio, so margin is an estimate for those products (`cost_imputed`).
- **Repaired dates**: orders with unparseable dates are repaired from other fields, which can shift an order by a day or more.
- **Recent returns**: the latest months may understate return rate because return windows may not be complete.
- **Cancellation vs delivery time**: cancelled orders are never delivered, so their relationship with delivery speed is an association only.
- **Single-node setup**: Airflow runs with LocalExecutor and a single PostgreSQL instance for local reproducibility, not production scale. Credentials are demo values stored in `.env`.
- **Power BI refresh**: the `.pbix` must be refreshed manually (or with a gateway) after a pipeline run.
- **Schema drift**: new files with different columns fail validation by design; mappings may need extending for unseen label variants (unmapped labels become `Unknown`).

## Troubleshooting

| Problem | Fix |
|---|---|
| `DagNotFound` on trigger | The scheduler has not registered the new file yet. Run `airflow dags reserialize`, or wait about 2 minutes |
| Port 5433 or 8080 in use | Change the port mapping in `docker-compose.yml` |
| Power BI asks about encryption | Allow the unencrypted connection; the database is local |
| Docker engine not running | Start Docker Desktop and wait for "Engine running" |
| A task fails | Open the task's **Log** tab in Airflow, or query `ops.pipeline_run_log` |
| Start from scratch | `docker compose down -v`, then repeat the quick start |

---

*Built for CodeConnect'26, NexusMart Data Analytics Module.*
