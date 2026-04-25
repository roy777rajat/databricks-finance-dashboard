<p align="center">
  <img src="docs/Banner.png" alt="Personal Finance Dashboard on Databricks" width="100%"/>
</p>

<h1 align="center">💰 Personal Finance Dashboard on Databricks</h1>

<p align="center">
  A production-grade personal-finance dashboard built entirely on Databricks — ingesting transaction emails via an ETL pipeline, landing them in Unity Catalog, syncing to <b>Lakebase</b> (managed Postgres), and surfacing everything through a polished Streamlit app deployed as a <b>Databricks App</b>.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white" alt="Databricks"/>
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white" alt="Streamlit"/>
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Lakebase-Postgres_17-336791?logo=postgresql&logoColor=white" alt="Lakebase"/>
  <img src="https://img.shields.io/badge/Lakehouse-Delta-00ADD8?logo=apache&logoColor=white" alt="Lakehouse"/>
  <img src="https://img.shields.io/badge/Zerobus-Streaming_Ingest-FF6B35" alt="Zerobus"/>
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"/>
</p>

<p align="center">
  <b>#Databricks</b> · <b>#Lakehouse</b> · <b>#Lakebase</b> · <b>#Zerobus</b> · <b>#DataEngineering</b> · <b>#Streamlit</b> · <b>#PersonalFinance</b> · <b>#FinTech</b> · <b>#MedallionArchitecture</b> · <b>#UnityCatalog</b> · <b>#DeltaLake</b> · <b>#ETL</b> · <b>#OpenSource</b>
</p>

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

<p align="center">
  <img src="docs/Architecture.png" alt="System Architecture" width="85%"/>
</p>

The pipeline follows the **medallion pattern** end-to-end:

1. **Source** — Gmail mailbox watched by a Google Apps Script trigger; matching emails pushed straight into Databricks via the **Zerobus streaming endpoint**
2. **Medallion (Lakehouse)** — Bronze (raw email payloads) → Silver (parsed transactions + a quarantine table for malformed rows) → Gold (daily / merchant / reconciliation / failure-reason summaries)
3. **Serving (Lakebase + APIs)** — Lakebase (managed Postgres) syncs key tables for sub-100ms reads; SQL Warehouse handles ad-hoc queries; Jobs API surfaces pipeline run history
4. **App** — Streamlit deployed as a Databricks App, reading exclusively from the serving layer

---

## 📸 Sample Output

The dashboard renders four primary surfaces — a hero banner with live KPIs, a daily expenditure-vs-threshold chart, an operations grid for pipeline runs, and a paginated transaction ledger.

### 1️⃣ Quick View — KPIs & Latest Activity
<p align="center">
  <img src="docs/Finance-1.JPG" alt="Quick View — KPIs and latest transaction ticker" width="90%"/>
</p>

The landing page leads with the **animated "Latest Ingested" ticker** — pulsing red dot, shimmer effect, color-coded amount — giving you instant visibility into the newest transaction. Below it, four dual-value KPI cards summarize **All-time** and **Today** activity: Volume (Ingestion / Failure) and Money (Credit / Debit). Every tile carries a `Source:` footer naming the exact backing tables, so debugging is trivial.

### 2️⃣ Daily Spend vs Threshold
<p align="center">
  <img src="docs/Finance-2.JPG" alt="Daily expenditure tracked against a configurable threshold" width="90%"/>
</p>

A **configurable spending threshold** (default ₹400) is rendered as an amber dashed reference line; daily debit totals appear as a smooth red spline. Days that exceed the threshold get oversized markers — glanceable at a distance. Four mini-KPIs above the chart summarise Threshold, Days Over, Days Under, and Window Total, with a contextual info-note when the threshold is exceeded.

### 3️⃣ Top Merchants & Daily Trend
<p align="center">
  <img src="docs/Finance-3.JPG" alt="Top N merchants and daily credit vs debit chart" width="90%"/>
</p>

The **Top N Merchants** horizontal bar chart (configurable from the sidebar) reveals where money actually goes — by counterparty, with transaction counts shown alongside totals. Above it, the **Daily Credit vs Debit** grouped bar chart sources from the Lakebase gold summary, automatically annotating chart context when one direction is empty (your data may have only debits at first).

### 4️⃣ Operations & Full Ledger
<p align="center">
  <img src="docs/Finance-4.JPG" alt="Page 2 — Job runs, quarantine, paginated ledger" width="90%"/>
</p>

Page 2 is the **operations view**: the last 10 pipeline runs from the Databricks Jobs REST API (with status, duration, and joined ingest/success/failure counts), the bad-records quarantine grid (numpy-array-safe rendering of the `failure_reasons` column), and the **full transaction ledger** paginated 25-per-page with CSV export.

---

## ⚡ Low-Latency Ingestion — Google Apps Script × Databricks Zerobus

This project skips the traditional "scheduled batch poll" pattern entirely. Instead, the moment a transaction email arrives in Gmail, it lands in the Databricks Lakehouse **within seconds** via the [Zerobus](https://www.databricks.com/blog/announcing-zerobus) streaming-ingest API. The reference implementation is in [`gmail-zerobus-ingest/app.script`](gmail-zerobus-ingest/app.script).

### How it works

1. **Gmail-side trigger (Google Apps Script)**
   A small Apps Script project runs on a 1-minute time-driven trigger, scanning a labelled folder (`label:HDFC_ALERT is:unread`) for new bank notifications. The script de-duplicates against `PropertiesService` (so the same email is never sent twice), marks each processed message as read, and only requests an OAuth token *after* finding fresh records — keeping the script idle-cheap.

2. **Service-principal authentication (OAuth 2.0 client credentials)**
   The script exchanges a Databricks **client_id / client_secret** for an access token via `/oidc/v1/token`, scoped using OAuth `authorization_details` to grant **only** what's needed for this insert path:
   - `USE CATALOG` on the target catalog
   - `USE SCHEMA` on the target schema
   - `SELECT` + `MODIFY` on the target landing table
   This means the token leaving Google's network has zero ability to touch anything else in the workspace — a textbook least-privilege pattern.

3. **Zerobus streaming insert**
   The records are POSTed as JSON to:
   ```
   https://<workspace-id>.zerobus.us-west-2.cloud.databricks.com/zerobus/v1/tables/<catalog>.<schema>.<table>/insert
   ```
   Zerobus is Databricks' purpose-built **direct streaming-ingest endpoint** — no Kafka cluster, no Auto Loader file-drop, no warehouse warm-up. Each row is appended to the bronze Delta table with sub-second commit latency.

### Why this matters

| Traditional batch poll | Apps Script × Zerobus |
|---|---|
| Polls IMAP every N minutes — emails sit waiting | Apps Script trigger fires within 60s of email arrival |
| Needs an always-on worker / cron host | Runs on Google's infrastructure for free |
| Adds 1-N minutes of latency per stage | Sub-second commit to Delta after script fires |
| Requires file-drop landing zone (S3/ADLS) + Auto Loader | Direct row-level streaming insert |
| Schema-on-read messiness | Schema-enforced at the Zerobus endpoint |
| Operational overhead (lambdas, queues, retries) | One Apps Script + one Databricks endpoint |

The result: **a debit notification in your inbox shows up in the dashboard within ~60 seconds**, end-to-end — without running a single piece of infrastructure outside Google + Databricks.

### Reference code

The Apps Script source ships sanitized — every credential and identifier is replaced with `xxxxxxxxxxx` placeholders. To use it, you'll need to:
- Create a Databricks **service principal** with a client secret
- Grant that SP `USE CATALOG`, `USE SCHEMA`, and `SELECT`+`MODIFY` on the target bronze landing table
- Create a Zerobus-enabled landing table in your catalog
- Paste the client credentials and table FQN into the script's `CONFIG` block
- Deploy the script with a 1-minute time trigger

> ⚠️ **Never commit your real client_secret to a public repo.** Use [Apps Script properties](https://developers.google.com/apps-script/guides/properties) or [Google Cloud Secret Manager](https://cloud.google.com/secret-manager) to store them securely.

---

## 🌊 Why Lakehouse + Lakebase Together — The Modern Data Platform Pattern

This dashboard isn't just running on Databricks — it's running on the **two complementary halves** of Databricks' data platform working in concert. Understanding why this pairing matters is understanding why modern apps don't pick "one or the other".

### 🏔️ The Lakehouse (Delta Lake + Unity Catalog) — for Analytics
The Lakehouse is where **truth lives**: open-format Delta tables that your ETL pipeline writes to, governed by Unity Catalog, queryable by Spark, SQL Warehouses, BI tools, and ML notebooks. It's optimized for **scale, schema evolution, time-travel, and analytics queries** — scanning millions of rows, joining wide tables, computing aggregates.

But it's *not* optimized for what an interactive dashboard actually needs: **a single user clicking "next page" and expecting a 50ms response**.

### 🐘 Lakebase (managed Postgres) — for Apps
Lakebase is Databricks' **managed Postgres**, designed specifically for the operational/serving layer. It gives the app:
- **Sub-100ms point lookups** — the ledger pagination feels instant
- **Real ANSI SQL** that Postgres clients understand natively
- **Indexes, constraints, transactions** — relational features Spark doesn't offer
- **Auto-scaling 0.5–1 CU** with scale-to-zero — cheap to run, no warehouse warm-up
- **Synced tables** — Delta changes flow into Postgres automatically via CDC

### 🔗 The Critical Bridge — Synced Tables (Lakehouse → Lakebase via CDC)

The magic isn't either system alone — it's the **Change Data Feed-based sync** between them. Here's the actual flow:

1. **Bronze Delta tables have CDF enabled** — when the medallion job upserts rows, the table records every insert/update/delete in a hidden `_change_data` log:
   ```sql
   ALTER TABLE finance_zerobus.transaction.silver_clean
   SET TBLPROPERTIES (delta.enableChangeDataFeed = true,
                      delta.enableDeletionVectors = false);
   ```

2. **A Lakebase Synced Table observes the CDF** — Databricks runs a continuous reader that pulls `_change_data` rows out of Delta and replays them into the Postgres table. You configure it once in the Catalog UI; Databricks handles checkpointing, schema evolution, and back-pressure.

3. **Postgres mirror stays fresh in near-real-time** — typical lag is seconds, not minutes. Same governance (Unity Catalog), same lineage, same access control. **One write, two read paths.**

4. **The app reads only Postgres** — for paginated ledgers, point lookups by transaction ID, or filtered scans, the dashboard never touches Delta directly. Result: instant interactions, even on cold starts.

### 🚀 The Auto-Trigger Pipeline — Streaming All The Way Down

Beyond CDC, the bronze→silver→gold transformations themselves run as **streaming reads**, which means the pipeline auto-advances whenever the upstream table changes:

```python
# pyspark/01_bronze_to_silver_gmail_txn.py — bronze layer is a streaming read
bronze_df = spark.readStream.table(f"{CATALOG}.{SCHEMA}.gmail_bronze")
```

This unlocks two powerful triggering modes for the wrapping Databricks Job:

- **`Trigger.AvailableNow`** — runs when the job is invoked, processes everything new since the last checkpoint, then exits. Cheap, batch-friendly.
- **`File arrival` / `Continuous`** — Databricks Workflows can launch the job the moment data lands in the landing table, removing the need for cron schedules entirely.

Combined with the Apps Script + Zerobus front-door, the **end-to-end latency** is:

```
Email arrives in Gmail
     ↓ (≤ 60s — Apps Script trigger)
Bronze Delta table (Zerobus row commit)
     ↓ (seconds — streaming readStream picks up)
Silver + Gold Delta tables
     ↓ (seconds — Lakebase CDC sync)
Postgres mirror
     ↓ (instant — app reads on click)
Dashboard
```

**Total: usually under 90 seconds from inbox to dashboard.** Without managing a single message queue, file watcher, or polling loop.

### 🎯 Why This Matters in Modern Platforms

| Without Lakehouse + Lakebase | With Lakehouse + Lakebase |
|---|---|
| Pick Postgres → no analytics, ML, or scale | ✅ Both worlds available, no compromise |
| Pick Delta → slow interactive UIs | ✅ Delta for the truth, Postgres for the app |
| Maintain a separate ETL into Postgres | ✅ Sync handles it; one pipeline, two surfaces |
| Reconcile two security models | ✅ Unity Catalog governs both |
| Pay for an always-on transactional DB | ✅ Lakebase scales to zero when idle |

This pattern — **batch analytics on Delta, low-latency serving on Postgres, unified by sync, kicked off by streaming triggers** — is the emerging standard for data-driven apps. Databricks is the first platform to offer it as a single managed product end-to-end.

In this project specifically:
- **Page 2's Full Ledger** (paginated 25/page) reads from Lakebase — would feel sluggish on Delta
- **Page 1's All-Time aggregates** read from gold Delta tables via the SQL Warehouse — Spark crunches the math faster than Postgres
- **Jobs API** complements both for operational metadata (run history) — no ETL needed
- **Zerobus** handles the inbound streaming firehose without Kafka or Auto Loader

The dashboard is **fast, governed, observable, and cheap** because every layer is doing what it's designed to do.

---

## 🔄 ETL Pipeline — From Inbox to Dashboard

Below is the data-flow story end-to-end — each surface picture reflects a real component you can click through in your own workspace.

### 1. Ingest — Gmail as Source of Truth
<p align="center">
  <img src="docs/01.Gmail.png" alt="Gmail transaction emails" width="80%"/>
</p>

Gmail is the source of truth. Bank-transaction alerts (UPI, ATM, card) are auto-labelled with `HDFC_ALERT`. A Google Apps Script trigger watches that label and pushes new messages to the Databricks **Zerobus** endpoint within ~60 seconds — see the section above for full details. Each row lands in `gmail_bronze` (Delta) with the original payload, sender, subject, body, and `event_time`.

### 2. Transform — Medallion in the Lakehouse
The bronze rows flow through PySpark transformations that parse amount, direction, counterparty, VPA, and timestamps from the body. Successful parses land in `gmail_silver_clean`; malformed rows go to `gmail_silver_quarantine` with an array of `failure_reasons`. Aggregations roll up into four gold tables: daily summary, merchant summary, reconciliation metrics, and failure-reason counts.

> See `pyspark/01_bronze_to_silver_gmail_txn.py` and `pyspark/02_silver_to_gold_gmail_views.py` for the actual transformation code. Both use **streaming reads** (`spark.readStream.table(...)`) so they auto-advance as bronze grows.

### 3. Serve — Lakebase Sync
<p align="center">
  <img src="docs/03.Lakebase.png" alt="Lakebase managed Postgres serving layer" width="80%"/>
</p>

Three Lakebase Synced Tables mirror the silver and gold layers into managed Postgres:
- `silver_transaction` — every parsed transaction, indexed by date and direction
- `gold_transaction_summary` — daily debit/credit rollups
- `gold_merchant_summary_view` — merchant aggregates

CDC keeps these fresh as new bronze data flows through the medallion. The dashboard reads exclusively from these — sub-100ms response on every page click.

### 4. Surface — The Databricks App
<p align="center">
  <img src="docs/04.databricksApp.png" alt="Streamlit deployed as a Databricks App" width="80%"/>
</p>

Streamlit packaged as a Databricks App — same workspace, same auth, same governance. The app's service principal has narrow grants (SELECT on the schema, USE on the warehouse, VIEW on the ETL job) and connects via OAuth tokens that the SDK refreshes automatically.

### 5. Observe — Jobs REST API
<p align="center">
  <img src="docs/05.Job-Databricks API.png" alt="Databricks Jobs REST API for run history" width="80%"/>
</p>

Page 2's "Last 10 Pipeline Runs" calls `w.jobs.list_runs(...)` directly — no system-table grants required, just `CAN_VIEW` on the job itself. Each row joins by date with `gmail_gold_recon_metrics` to enrich the run with ingest/success/failure counts.

---

## 🧰 Tech Stack

| Layer | Tech |
|---|---|
| **Source trigger** | Google Apps Script (time-driven, 1-min interval) |
| **Streaming ingest** | Databricks Zerobus (direct streaming-ingest endpoint) |
| **Compute** | Databricks Apps (serverless), Databricks SQL Warehouse (serverless) |
| **Storage** | Unity Catalog Delta tables, Lakebase (managed Postgres 17, autoscale 0.5–1 CU) |
| **Transformation** | PySpark Structured Streaming (`readStream.table`) + Databricks Jobs |
| **Sync** | Lakebase Synced Tables (Delta CDF → Postgres replication) |
| **Auth** | Databricks OAuth (SDK-managed token refresh on the app side, OAuth 2.0 client_credentials on the script side) |
| **App Framework** | Streamlit 1.39 |
| **Data libs** | pandas 2.2, Plotly 5.24 |
| **SDKs** | databricks-sdk 0.38, databricks-sql-connector 3.7 |

---

## 🗂️ Project Structure

```
databricks-finance-dashboard/
├── app.py                              # Page 1 — Quick View
├── app.yaml                            # Databricks Apps manifest (command, env vars)
├── requirements.txt                    # Python dependencies
├── lib/
│   ├── data.py                         # Warehouse connection, token refresh, query cache
│   ├── formatters.py                   # INR formatter, numpy-safe array-to-string
│   └── theme.py                        # Design system: KPI cards, ticker, plotly defaults
├── pages/
│   └── 1_⚙️_Job_Details.py            # Page 2 — Operations view
├── pyspark/                            # Reference upstream ETL (run as Databricks Jobs)
│   ├── 01_bronze_to_silver_gmail_txn.py
│   └── 02_silver_to_gold_gmail_views.py
├── gmail-zerobus-ingest/               # Low-latency ingestion (Google Apps Script)
│   └── app.script
└── docs/
    ├── Banner.png                      # Hero banner
    ├── Architecture.png                # System architecture diagram
    ├── Finance-1.JPG ... 4.JPG         # Dashboard screenshots
    ├── 01.Gmail.png                    # ETL pipeline visuals
    ├── 03.Lakebase.png
    ├── 04.databricksApp.png
    └── 05.Job-Databricks API.png
```

---

## 🚀 Deployment

### Prerequisites
- Databricks workspace with Unity Catalog, a Serverless SQL Warehouse, and Databricks Apps enabled
- A Lakebase database provisioned
- A **Zerobus-enabled bronze landing table** (created from the Catalog UI)
- A **service principal** with client credentials, granted `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY` on the bronze landing table
- A Google account with Apps Script access for the ingestion trigger
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

3. **Configure ingestion** (`gmail-zerobus-ingest/app.script`):
   - Replace the `xxxxxxxxxxx` placeholders in the `CONFIG` block with your workspace ID, SP credentials, catalog/schema/table
   - Paste the script into a new Apps Script project at [script.google.com](https://script.google.com)
   - Add a **time-driven trigger** for `processHdfcEmails` at 1-minute intervals
   - Run once manually to authorise Gmail + UrlFetch scopes

4. **Upload to workspace**:
   ```bash
   databricks workspace import-dir . /Workspace/Users/you@example.com/finance-dashboard
   ```

5. **Create the app** in Databricks UI → Apps → Create, pointing to that path.

6. **Grant the App's service principal**:
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
- **Idle-cheap ingestion** — Apps Script only requests an OAuth token after finding new emails to send; zero API cost when the inbox is quiet

---

## 🛡️ Security

- No credentials in code — uses Databricks' OAuth flow via the SDK
- App runs as a service principal with least-privilege grants
- Lakebase connections use SDK-managed tokens, not long-lived passwords
- Authentication is workspace-enforced; no anonymous access (by Databricks Apps design)
- Apps Script tokens are scoped via OAuth `authorization_details` to a single table — even if leaked, they can't touch any other resource

---

## 📝 License

MIT — see [LICENSE](LICENSE).

---

## 🙏 Acknowledgments

Built iteratively with [Claude](https://claude.ai) using the Databricks MCP integration. Every tile, query, and CSS rule was live-tested against a real data pipeline.

---

<p align="center">
  <i>From inbox chaos to financial clarity — automatically.</i>
</p>

<p align="center">
  <b>#Databricks</b> · <b>#Lakehouse</b> · <b>#Lakebase</b> · <b>#Zerobus</b> · <b>#DataPlatform</b> · <b>#StreamingETL</b> · <b>#Postgres</b> · <b>#PySpark</b> · <b>#FinTech</b> · <b>#GoogleAppsScript</b>
</p>
