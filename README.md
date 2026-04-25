# 💰 Personal Finance Dashboard on Databricks

A production-grade personal-finance dashboard built entirely on Databricks — ingesting transaction emails via an ETL pipeline, landing them in Unity Catalog, syncing to Lakebase (managed Postgres), and surfacing everything through a polished Streamlit app deployed as a Databricks App.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Lakebase-Postgres_17-336791?logo=postgresql&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## ✨ Features

### 📊 Page 1 — Quick View
- **Latest-transaction ticker** — animated banner showing the newest ingested transaction (amount, merchant, bank, timestamp) with pulsing status dot and shimmer animation
- **All-time KPIs** — Volume (Ingestion / Failure) and Money (Credit / Debit) with source-table annotations on every tile
- **Today's KPIs** — scoped to email arrival date (`event_time`), not ingestion date, so re-runs don't inflate "today" to match "all-time"
- **Latest 5 transactions** — date-filtered, with NULL-safe timestamps via `COALESCE`
- **Daily Credit vs Debit** — grouped bar chart from the Lakebase gold table
- **Daily Expenditure vs Threshold** — line chart tracking daily debits against a configurable budget (default ₹400), with amber reference line and alert markers on over-threshold days
- **Top N Merchants** — horizontal bar chart, configurable N
- **Click-to-drill-down failures** — expanders reveal quarantine rows with array-typed failure reasons (numpy-array-safe)

### ⚙️ Page 2 — Job Details
- **Last 10 Pipeline Runs** — from the Databricks Jobs REST API, joined by date with daily reconciliation metrics
- **Bad Records · Quarantine** — grid merged with aggregated failure-reason counts
- **Full Transaction Ledger** — paginated (25/page), sorted by `COALESCE(txn_datetime_local, event_time) DESC`, with CSV export

### 🎨 Design
- Databricks visual language — Lava Red (`#FF3621`) accent, DM Sans typography, light cards with hover states
- Custom Streamlit components: animated ticker banner, dual-value KPI cards, section headers with source attribution

---

## 🏗️ Architecture

---
**Why Lakebase in the middle?** Lakebase is Databricks' managed Postgres. It gives the dashboard sub-100ms point lookups and pagination — far faster than Delta for interactive UIs — while staying inside Databricks for governance.

---

## 🧰 Tech Stack

| Layer | Tech |
|---|---|
| **Compute** | Databricks Apps (serverless), Databricks SQL Warehouse (serverless) |
| **Storage** | Unity Catalog Delta tables, Lakebase (managed Postgres 17, autoscale 0.5–1 CU) |
| **Ingestion** | Databricks Jobs (scheduled batch ETL) |
| **Sync** | Lakebase Synced Tables (CDC-based) |
| **Auth** | Databricks OAuth (SDK-managed token refresh) |
| **App Framework** | Streamlit 1.39 |
| **Data** | pandas 2.2, Plotly 5.24 |
| **SDKs** | databricks-sdk 0.38, databricks-sql-connector 3.7 |

---

## 🗂️ Project Structure
---


## 🚀 Deployment

### Prerequisites
- Databricks workspace with Unity Catalog, a Serverless SQL Warehouse, and Databricks Apps enabled
- A Lakebase database provisioned
- An upstream ingestion pipeline producing these tables (not included in this repo):
  - `<catalog>.<schema>.gmail_bronze`
  - `<catalog>.<schema>.gmail_silver_clean`
  - `<catalog>.<schema>.gmail_silver_quarantine`
  - `<catalog>.<schema>.gmail_gold_recon_metrics`
  - `<catalog>.<schema>.gmail_gold_failure_reasons`
- Lakebase-synced tables:
  - `<catalog>.<schema>.silver_transaction`
  - `<catalog>.<schema>.gold_transaction_summary`
  - `<catalog>.<schema>.gold_merchant_summary_view`

### Deploy

1. **Clone** the repo:
```bash
   git clone https://github.com/<your-username>/databricks-finance-dashboard.git
```

2. **Configure** `app.yaml`:
```yaml
   env:
     - name: "SOURCE_CATALOG"
       value: "your_catalog"
     - name: "SOURCE_SCHEMA"
       value: "your_schema"
     - name: "JOB_IDS"
       value: ""   # Optional: comma-separated job IDs for Page 2
```

3. **Upload to workspace**:
```bash
   databricks workspace import-dir . /Workspace/Users/you@example.com/finance-dashboard
```

4. **Create the app** in Databricks UI → Apps → Create, pointing to that path.

5. **Grant the App's service principal**:
   - `USE CATALOG` + `USE SCHEMA` + `SELECT` on your schema
   - `CAN USE` on your SQL warehouse
   - `CAN VIEW` on each ingestion job (for Page 2 runs panel)

---

## ⚙️ Configuration

| Variable | Default | Purpose |
|---|---|---|
| `SOURCE_CATALOG` | `finance_zerobus` | Unity Catalog catalog |
| `SOURCE_SCHEMA` | `transaction` | Schema within the catalog |
| `JOB_IDS` | *(empty)* | Comma-separated job IDs for Page 2; empty → list all visible jobs |
| `STREAMLIT_THEME_*` | Databricks palette | Colors, fonts, base theme |

See `.env.example` for a full template.

---

## 🛠️ Engineering Highlights

- **Token-refresh resilience** — OAuth tokens refreshed on every connection with retry-on-401/403 fallback; no more 1-hour `403 FORBIDDEN` crashes
- **NULL-safe timestamps** — uses `COALESCE(txn_datetime_local, event_time)` throughout since upstream doesn't always populate the primary timestamp
- **Array-typed column handling** — custom `arr_to_str()` helper handles numpy arrays (what the Databricks SQL connector returns for `ARRAY<STRING>`), Python lists, None, and scalars uniformly
- **Graceful fallbacks** — Page 2 runs panel falls back to daily recon metrics when the SP lacks view permission on jobs
- **Source transparency** — every tile shows its source table(s) in a footer for debugging
- **"Today" means email arrival** — counts use `event_time` rather than `_bronze_ingested_at`, so pipeline re-runs don't inflate today's numbers

---

## 🛡️ Security

- No credentials in code — uses Databricks' OAuth flow via the SDK
- App runs as a service principal with least-privilege grants
- Lakebase connections use SDK-managed tokens, not long-lived passwords
- Authentication is workspace-enforced; no anonymous access (by Databricks Apps design)

---

## 📝 License

MIT — see [LICENSE](LICENSE).

---

## 🙏 Acknowledgments

Built iteratively with [Claude](https://claude.ai) using the Databricks MCP integration. Every tile, query, and CSS rule was live-tested against a real data pipeline.