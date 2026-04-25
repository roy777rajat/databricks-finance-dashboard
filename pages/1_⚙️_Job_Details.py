"""
Page 2 — Job Details.

  (a) Last 10 pipeline runs — Databricks Jobs REST API via SDK.
      No system-table permissions needed. Counts joined from gmail_gold_recon_metrics by run-date.
  (b) Bad records grid — gmail_silver_quarantine + gmail_gold_failure_reasons
  (c) Full transactions paginated — silver_transaction (Lakebase), ORDER BY date DESC
"""
from __future__ import annotations

import math

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient

from lib.data import (
    q,
    T_GOLD_RECON, T_SILVER_QUARANTINE, T_GOLD_FAILURES,
    T_LB_SILVER,
)
from lib.formatters import arr_to_str
from lib.theme import apply_theme, hero, section_head, empty_state, info_note

st.set_page_config(page_title="Finance · Job Details", page_icon="⚙️", layout="wide")
apply_theme()

hero(
    kicker="Job Details",
    title="Pipeline Operations",
    sub="End-to-end job runs · quarantine · full ledger",
)


# ═══════════════════════════════════════════════════════════════════════
# (a) Last 10 pipeline runs — from Jobs REST API (no system.* grants needed)
# ═══════════════════════════════════════════════════════════════════════
section_head(
    "Last 10 Pipeline Runs",
    "Databricks Jobs API (SDK) ⨝ gmail_gold_recon_metrics",
)


@st.cache_data(ttl=30, show_spinner=False)
def load_recent_runs(n: int = 10) -> pd.DataFrame:
    """
    Pulls recent runs using explicit job IDs from env (JOB_IDS, comma-separated).
    Falls back to listing all jobs visible to the SP if JOB_IDS is empty.
    Unreadable jobs (no CAN_VIEW) are silently skipped.
    """
    import os
    w = WorkspaceClient()

    job_ids_env = os.environ.get("JOB_IDS", "").strip()
    target_job_ids: list[int] = []

    if job_ids_env:
        for tok in job_ids_env.split(","):
            tok = tok.strip()
            if tok.isdigit():
                target_job_ids.append(int(tok))
    else:
        # Fallback: try to list jobs (may be empty for SP)
        try:
            all_jobs = list(w.jobs.list(limit=50))
            target_job_ids = [
                j.job_id for j in all_jobs
                if j.settings and j.settings.name and any(
                    k in j.settings.name.lower() for k in ("gmail", "transaction", "etl")
                )
            ] or [j.job_id for j in all_jobs]
        except Exception:
            target_job_ids = []

    rows = []
    for job_id in target_job_ids:
        try:
            # Get job name for the display
            job = w.jobs.get(job_id=job_id)
            job_name = job.settings.name if job.settings else f"Job {job_id}"
            # Get completed runs
            runs = list(w.jobs.list_runs(
                job_id=job_id, completed_only=True, limit=n, expand_tasks=False
            ))
            for r in runs:
                rows.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "run_id": r.run_id,
                    "start_ts": r.start_time,
                    "end_ts": r.end_time,
                    "duration_ms": r.run_duration or 0,
                    "result": (r.state.result_state.value
                               if r.state and r.state.result_state else None),
                    "trigger": r.trigger.value if r.trigger else None,
                    "url": r.run_page_url,
                })
        except Exception:
            # SP lacks CAN_VIEW on this job — skip silently
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["start"] = pd.to_datetime(df["start_ts"], unit="ms")
    df["end"]   = pd.to_datetime(df["end_ts"],   unit="ms")
    df = df.sort_values("start", ascending=False).head(n).reset_index(drop=True)
    return df


@st.cache_data(ttl=30, show_spinner=False)
def load_recon_by_date() -> pd.DataFrame:
    try:
        df = q(f"""
            SELECT txn_date, ingested_count, processed_count, failed_count
            FROM {T_GOLD_RECON}
        """)
        if not df.empty:
            df["txn_date"] = pd.to_datetime(df["txn_date"]).dt.date
        return df
    except Exception:
        return pd.DataFrame()


try:
    runs = load_recent_runs(10)
except Exception as e:
    st.error(f"Error loading job runs: {e}")
    runs = pd.DataFrame()

if runs.empty:
    # Fallback to daily recon metrics — always readable
    info_note(
        "Jobs API returned no runs (likely SP lacks CAN_VIEW on additional jobs). "
        "Falling back to daily reconciliation metrics from gmail_gold_recon_metrics."
    )
    recon = load_recon_by_date()
    if recon.empty:
        empty_state("No run history available.")
    else:
        fallback = recon.copy().sort_values("txn_date", ascending=False).head(10)
        fallback["Status"] = fallback["failed_count"].apply(
            lambda x: "✅ SUCCESS" if x == 0 else "⚠️ PARTIAL"
        )
        display = fallback[[
            "txn_date", "ingested_count", "processed_count", "failed_count", "Status"
        ]].rename(columns={
            "txn_date": "Date",
            "ingested_count": "Ingested",
            "processed_count": "Success",
            "failed_count": "Failure",
        })
        st.dataframe(
            display, hide_index=True, use_container_width=True,
            column_config={
                "Date": st.column_config.DateColumn(format="DD MMM YYYY"),
                "Ingested": st.column_config.NumberColumn(format="%d"),
                "Success": st.column_config.NumberColumn(format="%d"),
                "Failure": st.column_config.NumberColumn(format="%d"),
            },
        )
else:
    # Join with recon counts by run-start-date
    recon = load_recon_by_date()
    runs["run_date"] = runs["start"].dt.date
    if not recon.empty:
        runs = runs.merge(recon, left_on="run_date", right_on="txn_date", how="left")
    for col in ("ingested_count", "processed_count", "failed_count"):
        if col not in runs.columns:
            runs[col] = 0
        runs[col] = pd.to_numeric(runs[col], errors="coerce").fillna(0).astype(int)

    # Summary strip
    succ = int((runs["result"] == "SUCCESS").sum())
    fail = int(runs["result"].isin(["FAILED", "INTERNAL_ERROR", "TIMEDOUT", "CANCELED"]).sum())
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Runs shown", len(runs))
    s2.metric("Succeeded", succ)
    s3.metric("Failed / Other", fail)
    s4.metric("Total ingested (matched)", int(runs["ingested_count"].sum()))

    # Grid
    grid = runs.copy()
    grid["Status"] = grid["result"].apply(
        lambda s: "✅ " + s if s == "SUCCESS"
        else "⏳ " + (s or "") if s in (None, "") else "❌ " + (s or "UNKNOWN")
    )
    grid["Duration"] = grid["duration_ms"].apply(
        lambda x: (f"{int(x)//1000}s" if x < 60_000
                    else f"{int(x)//60000}m {int((x%60000)//1000)}s")
    )

    display = grid[[
        "job_id", "job_name", "run_id", "start", "end",
        "Status", "Duration",
        "ingested_count", "processed_count", "failed_count",
    ]].rename(columns={
        "job_id": "Job ID",
        "job_name": "Job Name",
        "run_id": "Run ID",
        "start": "Start",
        "end": "End",
        "ingested_count": "Ingested",
        "processed_count": "Success",
        "failed_count": "Failure",
    })

    st.dataframe(
        display, hide_index=True, use_container_width=True, height=380,
        column_config={
            "Start": st.column_config.DatetimeColumn(format="DD MMM · HH:mm:ss"),
            "End":   st.column_config.DatetimeColumn(format="DD MMM · HH:mm:ss"),
            "Ingested": st.column_config.NumberColumn(format="%d"),
            "Success":  st.column_config.NumberColumn(format="%d"),
            "Failure":  st.column_config.NumberColumn(format="%d"),
        },
    )
    info_note(
        "Runs are sourced from the Databricks Jobs API. "
        "Ingested / Success / Failure counts come from gmail_gold_recon_metrics joined by run date — "
        "if multiple runs happen on the same day, they share that day's daily totals."
    )

    # Quick links
    with st.expander("Open run pages in Databricks"):
        for _, r in runs.iterrows():
            st.markdown(
                f"- **{r['job_name']}** · Run `{r['run_id']}` · "
                f"{r['start'].strftime('%d %b · %H:%M:%S')} → "
                f"[Open run ↗]({r['url']})"
            )


# ═══════════════════════════════════════════════════════════════════════
# (b) Bad dataset grid
# ═══════════════════════════════════════════════════════════════════════
section_head("Bad Records · Quarantine", "gmail_silver_quarantine")

try:
    bad = q(f"""
        SELECT
          _silver_processed_at AS `When Quarantined`,
          id AS `ID`,
          sender AS `Sender`,
          subject AS `Subject`,
          failure_reasons AS `Failure Reasons`,
          amount AS `Amount`,
          direction AS `Dir`,
          txn_type AS `Type`,
          event_time AS `Event Time`
        FROM {T_SILVER_QUARANTINE}
        ORDER BY _silver_processed_at DESC
        LIMIT 200
    """)
except Exception as e:
    st.error(f"Error loading quarantine: {e}")
    bad = pd.DataFrame()

if bad.empty:
    empty_state("Quarantine is empty 🎉", "No malformed records currently.")
else:
    if "Failure Reasons" in bad.columns:
        bad["Failure Reasons"] = bad["Failure Reasons"].apply(arr_to_str)
    if "Amount" in bad.columns:
        bad["Amount"] = pd.to_numeric(bad["Amount"], errors="coerce")
    st.dataframe(
        bad, hide_index=True, use_container_width=True, height=340,
        column_config={
            "Amount": st.column_config.NumberColumn(format="₹ %.2f"),
            "When Quarantined": st.column_config.DatetimeColumn(format="DD MMM · HH:mm"),
            "Event Time": st.column_config.DatetimeColumn(format="DD MMM · HH:mm"),
        },
    )

with st.expander("Aggregated failure reasons (gmail_gold_failure_reasons)"):
    try:
        reasons = q(f"""
            SELECT txn_date AS `Date`, reason AS `Reason`, failure_count AS `Count`
            FROM {T_GOLD_FAILURES}
            ORDER BY txn_date DESC, failure_count DESC
        """)
    except Exception as e:
        st.error(f"Error: {e}")
        reasons = pd.DataFrame()
    if reasons.empty:
        info_note("gmail_gold_failure_reasons is empty — no categorised failures recorded yet.")
    else:
        st.dataframe(reasons, hide_index=True, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# (c) Full transaction ledger — Lakebase, paginated
# ═══════════════════════════════════════════════════════════════════════
section_head("Full Transaction Ledger · Paginated", "silver_transaction (Lakebase)")

PAGE_SIZE = 25


@st.cache_data(ttl=30, show_spinner=False)
def txn_total():
    df = q(f"SELECT COUNT(*) AS n FROM {T_LB_SILVER}")
    return int(df.iloc[0]["n"]) if not df.empty else 0


@st.cache_data(ttl=30, show_spinner=False)
def txn_page(offset: int, limit: int):
    sql = f"""
      SELECT *
      FROM {T_LB_SILVER}
      ORDER BY COALESCE(txn_datetime_local, event_time) DESC
      LIMIT {limit} OFFSET {offset}
    """
    return q(sql)


try:
    total = txn_total()
except Exception as e:
    st.error(f"Error counting: {e}")
    total = 0

if total == 0:
    empty_state("No transactions in Lakebase silver_transaction")
else:
    total_pages = max(1, math.ceil(total / PAGE_SIZE))

    if "txn_page_n" not in st.session_state:
        st.session_state.txn_page_n = 1

    col_info, col_prev, col_page, col_next = st.columns([3, 1, 2, 1])
    start_row = (st.session_state.txn_page_n - 1) * PAGE_SIZE + 1
    end_row = min(st.session_state.txn_page_n * PAGE_SIZE, total)

    with col_info:
        st.markdown(
            f"<div style='padding-top:8px;color:#8A96B0;font-size:13px;'>"
            f"Showing <b style='color:#EAF0FA;'>{start_row}–{end_row}</b> of "
            f"<b style='color:#EAF0FA;'>{total}</b> · Page "
            f"<b>{st.session_state.txn_page_n}</b> / {total_pages}</div>",
            unsafe_allow_html=True,
        )
    with col_prev:
        if st.button("← Prev", use_container_width=True,
                     disabled=st.session_state.txn_page_n <= 1, key="prev_btn"):
            st.session_state.txn_page_n -= 1
            st.rerun()
    with col_page:
        new_page = st.number_input(
            "Page", min_value=1, max_value=total_pages,
            value=st.session_state.txn_page_n,
            label_visibility="collapsed", step=1, key="page_input",
        )
        if int(new_page) != st.session_state.txn_page_n:
            st.session_state.txn_page_n = int(new_page)
            st.rerun()
    with col_next:
        if st.button("Next →", use_container_width=True,
                     disabled=st.session_state.txn_page_n >= total_pages, key="next_btn"):
            st.session_state.txn_page_n += 1
            st.rerun()

    offset = (st.session_state.txn_page_n - 1) * PAGE_SIZE
    try:
        pg = txn_page(offset, PAGE_SIZE)
    except Exception as e:
        st.error(f"Error loading page: {e}")
        pg = pd.DataFrame()

    if not pg.empty:
        # Build a unified `When` column: txn_datetime_local if present, else event_time.
        if "txn_datetime_local" in pg.columns:
            pg["_When"] = pd.to_datetime(pg["txn_datetime_local"], errors="coerce")
        else:
            pg["_When"] = pd.NaT
        if "event_time" in pg.columns:
            pg["_When"] = pg["_When"].fillna(pd.to_datetime(pg["event_time"], errors="coerce"))

        col_order = ["_When"] + [c for c in [
            "bank", "txn_type", "direction", "amount", "currency",
            "counterparty", "vpa", "txn_ref", "card_last4", "account_last4",
            "location_city", "location_place", "available_balance", "id",
        ] if c in pg.columns]
        pg_show = pg[col_order].rename(columns={
            "_When": "When",
            "bank": "Bank", "txn_type": "Type", "direction": "Dir",
            "amount": "Amount", "currency": "Ccy",
            "counterparty": "Counterparty", "vpa": "VPA", "txn_ref": "Ref",
            "card_last4": "Card", "account_last4": "A/c",
            "location_city": "City", "location_place": "Place",
            "available_balance": "Balance", "id": "ID",
        })
        pg_show["Amount"] = pd.to_numeric(pg_show["Amount"], errors="coerce")
        if "Balance" in pg_show.columns:
            pg_show["Balance"] = pd.to_numeric(pg_show["Balance"], errors="coerce")
        st.dataframe(
            pg_show, hide_index=True, use_container_width=True, height=520,
            column_config={
                "Amount": st.column_config.NumberColumn(format="₹ %.2f"),
                "Balance": st.column_config.NumberColumn(format="₹ %.2f"),
                "When": st.column_config.DatetimeColumn(format="DD MMM YYYY · HH:mm"),
            },
        )
        csv = pg.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇ Download current page (CSV)", data=csv,
            file_name=f"transactions_page_{st.session_state.txn_page_n}.csv",
            mime="text/csv",
        )
