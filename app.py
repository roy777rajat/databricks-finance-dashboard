"""
Page 1 — Quick View.

  (a) All-Time  Ingestion / Failure       [expand → quarantine + failure-reason join]
  (b) All-Time  Credit / Debit
  (c) Today     Ingestion / Failure       [expand → today's quarantine]
  (d) Today     Credit / Debit
  (e) Latest 5 transactions — Lakebase silver_transaction, date-filtered
  (f) Daily bar: Credit vs Debit — Lakebase gold_transaction_summary, date-filtered
  (g) Top N merchants — Lakebase silver_transaction (date-filtered)
"""
from __future__ import annotations
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from lib.data import (
    q,
    T_BRONZE, T_SILVER_CLEAN, T_SILVER_QUARANTINE, T_GOLD_FAILURES,
    T_LB_SILVER, T_LB_GOLD_DAILY,
)
from lib.formatters import fmt_inr, fmt_count, arr_to_str
from lib.theme import (
    apply_theme, hero, kpi_pair, section_head, empty_state, info_note,
    plotly_layout, latest_ticker,
    AMBER, GREEN, RED,
)

st.set_page_config(
    page_title="Rajat Finance · Quick View",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_theme()


# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Date Filter")
    st.caption("Applies to: Latest 5 · Daily bar · Top merchants")
    default_start = date.today() - timedelta(days=90)
    default_end = date.today()
    dr = st.date_input(
        "Date range",
        value=(default_start, default_end),
        max_value=date.today(),
        key="qv_dr",
    )
    start_d, end_d = (dr if isinstance(dr, tuple) and len(dr) == 2
                      else (default_start, default_end))
    st.markdown("---")
    top_n = st.slider("Top N merchants", 3, 20, 7)
    st.markdown("---")

    st.markdown("### Spend Monitor")
    st.caption("Applies to: Daily expenditure vs threshold")
    threshold = st.number_input(
        "Daily threshold (₹)", min_value=0, max_value=100000,
        value=400, step=50, key="qv_thresh",
    )
    spend_window = st.slider(
        "Days to show", 7, 90, 30, key="qv_spend_win",
    )
    st.markdown("---")
    st.caption("Data refreshes every 30s")


hero(
    kicker="Quick View",
    title="Daily Finance Dashboard - Rajat",
    sub=f"Date filters: {start_d.strftime('%d %b %Y')} → {end_d.strftime('%d %b %Y')}",
)


# ═══════════════════════════════════════════════════════════════════════
# Latest-ingested marquee banner
# ═══════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=30, show_spinner=False)
def load_latest_one():
    sql = f"""
      SELECT
        COALESCE(txn_datetime_local, event_time) AS txn_ts,
        amount, bank, txn_type, direction, counterparty, vpa
      FROM {T_LB_SILVER}
      ORDER BY COALESCE(txn_datetime_local, event_time) DESC
      LIMIT 1
    """
    try:
        df = q(sql)
        return None if df.empty else df.iloc[0]
    except Exception:
        return None


_latest = load_latest_one()
if _latest is not None:
    _dir = (_latest.get("direction") or "").upper()
    _dir_word = "DEBITED" if _dir == "DEBIT" else ("CREDITED" if _dir == "CREDIT" else _dir)
    _merchant = _latest.get("counterparty") or _latest.get("vpa") or "—"
    _ts = pd.to_datetime(_latest.get("txn_ts"), errors="coerce")
    _when = _ts.strftime("%d %b %Y · %H:%M") if pd.notna(_ts) else "—"
    latest_ticker(
        amount_str=fmt_inr(_latest.get("amount"), compact=False),
        dir_word=_dir_word,
        bank=_latest.get("bank") or "—",
        txn_type=_latest.get("txn_type") or "—",
        merchant=_merchant,
        when_str=_when,
    )


# ═══════════════════════════════════════════════════════════════════════
# All-time + Today aggregates
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=30, show_spinner=False)
def load_alltime():
    sql = f"""
      SELECT
        (SELECT COUNT(*) FROM {T_BRONZE}) AS bronze_total,
        (SELECT COUNT(*) FROM {T_SILVER_CLEAN}) AS silver_clean_total,
        (SELECT COUNT(*) FROM {T_SILVER_QUARANTINE}) AS quarantine_total,
        (SELECT COALESCE(SUM(amount),0) FROM {T_LB_SILVER}
          WHERE direction='CREDIT') AS credit_total,
        (SELECT COALESCE(SUM(amount),0) FROM {T_LB_SILVER}
          WHERE direction='DEBIT') AS debit_total
    """
    return q(sql).iloc[0]


@st.cache_data(ttl=30, show_spinner=False)
def load_today():
    """
    'Today' semantics: we count by the EMAIL arrival date (event_time),
    not by when the ETL last re-ingested the row (_bronze_ingested_at).
    That way, re-runs of the pipeline don't inflate 'today' to match 'all-time'.
    """
    sql = f"""
      SELECT
        (SELECT COUNT(*) FROM {T_BRONZE}
          WHERE CAST(event_time AS DATE)=CURRENT_DATE()) AS bronze_today,
        (SELECT COUNT(*) FROM {T_SILVER_CLEAN}
          WHERE CAST(event_time AS DATE)=CURRENT_DATE()) AS clean_today,
        (SELECT COUNT(*) FROM {T_SILVER_QUARANTINE}
          WHERE CAST(event_time AS DATE)=CURRENT_DATE()) AS q_today,
        (SELECT COALESCE(SUM(amount),0) FROM {T_LB_SILVER}
          WHERE direction='CREDIT'
            AND CAST(COALESCE(txn_datetime_local, event_time) AS DATE)=CURRENT_DATE()) AS credit_today,
        (SELECT COALESCE(SUM(amount),0) FROM {T_LB_SILVER}
          WHERE direction='DEBIT'
            AND CAST(COALESCE(txn_datetime_local, event_time) AS DATE)=CURRENT_DATE()) AS debit_today
    """
    return q(sql).iloc[0]


@st.cache_data(ttl=30, show_spinner=False)
def load_quarantine(today_only: bool = False) -> pd.DataFrame:
    where = "WHERE CAST(_silver_processed_at AS DATE)=CURRENT_DATE()" if today_only else ""
    sql = f"""
      SELECT
        _silver_processed_at AS `When`,
        id AS `ID`,
        sender AS `Sender`,
        subject AS `Subject`,
        failure_reasons AS `Reasons`,
        amount AS `Amount`,
        direction AS `Dir`,
        txn_type AS `Type`
      FROM {T_SILVER_QUARANTINE}
      {where}
      ORDER BY _silver_processed_at DESC
      LIMIT 500
    """
    return q(sql)


@st.cache_data(ttl=30, show_spinner=False)
def load_failure_reasons(today_only: bool = False) -> pd.DataFrame:
    where = "WHERE txn_date=CURRENT_DATE()" if today_only else ""
    sql = f"""
      SELECT txn_date AS `Date`, reason AS `Reason`, failure_count AS `Count`
      FROM {T_GOLD_FAILURES}
      {where}
      ORDER BY txn_date DESC, failure_count DESC
    """
    return q(sql)


try:
    at = load_alltime()
    td = load_today()
except Exception as e:
    st.error(f"Could not load data: {e}")
    st.stop()

total_ingestion = int(at["bronze_total"])
total_failure = int(at["bronze_total"]) - int(at["silver_clean_total"]) + int(at["quarantine_total"])
total_credit = float(at["credit_total"])
total_debit = float(at["debit_total"])

today_ingestion = int(td["bronze_today"])
today_failure = int(td["bronze_today"]) - int(td["clean_today"]) + int(td["q_today"])
today_credit = float(td["credit_today"])
today_debit = float(td["debit_today"])


# ── Row 1: All-time ───────────────────────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    kpi_pair(
        "All-Time · Volume",
        fmt_count(total_ingestion), "Ingestion", "amber",
        fmt_count(total_failure), "Failure", "down",
        "gmail_bronze · gmail_silver_clean · gmail_silver_quarantine",
    )
    with st.expander(f"🔍 Show {total_failure} failure record(s)", expanded=False):
        st.caption("Source: gmail_silver_quarantine (rows) + gmail_gold_failure_reasons (reason counts)")
        qdf = load_quarantine(today_only=False)
        if qdf.empty:
            info_note("Quarantine table is empty — failures counted by the bronze→silver drop-out "
                      "(4 bronze rows didn't produce a silver row). See Job Details page for per-run counts.")
        else:
            # Arrays need stringify
            if "Reasons" in qdf.columns:
                qdf["Reasons"] = qdf["Reasons"].apply(arr_to_str)
            if "Amount" in qdf.columns:
                qdf["Amount"] = pd.to_numeric(qdf["Amount"], errors="coerce")
            st.dataframe(qdf, hide_index=True, use_container_width=True, height=260,
                column_config={
                    "Amount": st.column_config.NumberColumn(format="₹ %.2f"),
                    "When": st.column_config.DatetimeColumn(format="DD MMM YYYY · HH:mm"),
                })
        # Reasons
        rdf = load_failure_reasons(today_only=False)
        if not rdf.empty:
            st.markdown("**Failure reason breakdown**")
            st.dataframe(rdf, hide_index=True, use_container_width=True, height=160)

with c2:
    kpi_pair(
        "All-Time · Money",
        fmt_inr(total_credit, compact=True), "Credit", "up",
        fmt_inr(total_debit, compact=True), "Debit", "down",
        "silver_transaction (Lakebase)",
    )


# ── Row 2: Today ──────────────────────────────────────────────────────────
c3, c4 = st.columns(2)
with c3:
    kpi_pair(
        "Today · Volume",
        fmt_count(today_ingestion), "Ingestion", "amber",
        fmt_count(today_failure), "Failure", "down",
        "gmail_bronze · gmail_silver_clean · gmail_silver_quarantine",
    )
    with st.expander(f"🔍 Show today's {today_failure} failure record(s)", expanded=False):
        st.caption("Source: gmail_silver_quarantine filtered to today")
        qdf_t = load_quarantine(today_only=True)
        if qdf_t.empty:
            info_note("No rows in gmail_silver_quarantine for today. If the failure count is > 0, "
                      "those are bronze→silver drop-outs (no matching silver row produced).")
        else:
            if "Reasons" in qdf_t.columns:
                qdf_t["Reasons"] = qdf_t["Reasons"].apply(arr_to_str)
            if "Amount" in qdf_t.columns:
                qdf_t["Amount"] = pd.to_numeric(qdf_t["Amount"], errors="coerce")
            st.dataframe(qdf_t, hide_index=True, use_container_width=True, height=240)
        rdf_t = load_failure_reasons(today_only=True)
        if not rdf_t.empty:
            st.markdown("**Today's failure reasons**")
            st.dataframe(rdf_t, hide_index=True, use_container_width=True, height=140)

with c4:
    kpi_pair(
        "Today · Money",
        fmt_inr(today_credit, compact=True), "Credit", "up",
        fmt_inr(today_debit, compact=True), "Debit", "down",
        "silver_transaction (Lakebase)",
    )


# ═══════════════════════════════════════════════════════════════════════
# (e) Latest 5 transactions — Lakebase
# ═══════════════════════════════════════════════════════════════════════
section_head("Latest 5 Transactions", "silver_transaction (Lakebase)")

latest_sql = f"""
  SELECT
    COALESCE(txn_datetime_local, event_time) AS `When`,
    bank AS `Bank`,
    txn_type AS `Type`,
    direction AS `Dir`,
    amount AS `Amount`,
    currency AS `Ccy`,
    counterparty AS `Counterparty`,
    vpa AS `VPA`,
    txn_ref AS `Ref`
  FROM {T_LB_SILVER}
  WHERE CAST(COALESCE(txn_datetime_local, event_time) AS DATE) BETWEEN '{start_d}' AND '{end_d}'
  ORDER BY COALESCE(txn_datetime_local, event_time) DESC
  LIMIT 5
"""
try:
    latest = q(latest_sql)
except Exception as e:
    st.error(f"Error loading latest: {e}")
    latest = pd.DataFrame()

if latest.empty:
    empty_state("No transactions in selected range",
                "Try widening the date range in the sidebar.")
else:
    latest["Amount"] = pd.to_numeric(latest["Amount"], errors="coerce")
    st.dataframe(latest, hide_index=True, use_container_width=True,
        column_config={
            "Amount": st.column_config.NumberColumn(format="₹ %.2f"),
            "When": st.column_config.DatetimeColumn(format="DD MMM YYYY · HH:mm"),
        })


# ═══════════════════════════════════════════════════════════════════════
# (f) Daily bar: Credit vs Debit — Lakebase
# ═══════════════════════════════════════════════════════════════════════
section_head("Daily Amount · Credit vs Debit", "gold_transaction_summary (Lakebase)")

daily_sql = f"""
  SELECT txn_date, direction, SUM(total_amount) AS amount
  FROM {T_LB_GOLD_DAILY}
  WHERE txn_date BETWEEN '{start_d}' AND '{end_d}'
  GROUP BY txn_date, direction
  ORDER BY txn_date
"""
try:
    daily = q(daily_sql)
except Exception as e:
    st.error(f"Error loading daily: {e}")
    daily = pd.DataFrame()

if daily.empty:
    empty_state("No aggregated data in range")
else:
    daily["amount"] = pd.to_numeric(daily["amount"], errors="coerce").fillna(0.0)
    daily["txn_date"] = pd.to_datetime(daily["txn_date"])
    directions_present = set(daily["direction"].unique())

    if directions_present == {"DEBIT"}:
        info_note("The dataset currently contains only DEBIT transactions — no credits recorded yet. "
                  "The chart will show only debits; the bar will include credits automatically once they land.")
    elif directions_present == {"CREDIT"}:
        info_note("The dataset currently contains only CREDIT transactions — no debits recorded yet.")

    fig = px.bar(
        daily, x="txn_date", y="amount", color="direction", barmode="group",
        color_discrete_map={"CREDIT": GREEN, "DEBIT": RED},
        labels={"txn_date": "Date", "amount": "Amount (INR)", "direction": ""},
    )
    fig.update_layout(**plotly_layout(
        height=360, legend=dict(orientation="h", y=1.1, x=0), bargap=0.25))
    fig.update_traces(hovertemplate="<b>%{x|%d %b %Y}</b><br>₹%{y:,.0f}<extra>%{fullData.name}</extra>")
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# (f2) Daily Expenditure vs Threshold — line chart with reference line
# ═══════════════════════════════════════════════════════════════════════
section_head(
    f"Daily Expenditure vs Threshold · Last {spend_window} days",
    f"silver_transaction (Lakebase) · threshold ₹{int(threshold)}",
)

spend_sql = f"""
  SELECT
    CAST(COALESCE(txn_datetime_local, event_time) AS DATE) AS day,
    SUM(amount) AS daily_debit,
    COUNT(*) AS txn_count
  FROM {T_LB_SILVER}
  WHERE direction='DEBIT'
    AND CAST(COALESCE(txn_datetime_local, event_time) AS DATE)
        >= DATE_SUB(CURRENT_DATE(), {spend_window})
  GROUP BY CAST(COALESCE(txn_datetime_local, event_time) AS DATE)
  ORDER BY day
"""
try:
    spend = q(spend_sql)
except Exception as e:
    st.error(f"Error loading expenditure: {e}")
    spend = pd.DataFrame()

if spend.empty:
    empty_state("No expenditure in selected window")
else:
    spend["daily_debit"] = pd.to_numeric(spend["daily_debit"], errors="coerce").fillna(0.0)
    spend["txn_count"] = pd.to_numeric(spend["txn_count"], errors="coerce").fillna(0).astype(int)
    spend["day"] = pd.to_datetime(spend["day"])

    # Reindex to continuous date range — fill missing days with 0 for clarity
    full_range = pd.date_range(
        end=pd.Timestamp(date.today()),
        periods=spend_window + 1, freq="D",
    )
    spend_full = (
        spend.set_index("day").reindex(full_range)
             .rename_axis("day").reset_index()
    )
    spend_full["daily_debit"] = spend_full["daily_debit"].fillna(0.0)
    spend_full["txn_count"] = spend_full["txn_count"].fillna(0).astype(int)

    # Count days over/under
    over_days = int((spend_full["daily_debit"] > threshold).sum())
    under_days = int((spend_full["daily_debit"] <= threshold).sum())

    # Summary row
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Threshold", f"₹{int(threshold):,}")
    s2.metric("Days over", over_days)
    s3.metric("Days under", under_days)
    s4.metric(
        "Window total",
        fmt_inr(float(spend_full["daily_debit"].sum()), compact=True),
    )

    fig2 = go.Figure()

    # Threshold line (amber reference)
    fig2.add_hline(
        y=float(threshold),
        line_dash="dash", line_color="#D17B00",
        line_width=2,
        annotation_text=f" Threshold ₹{int(threshold)} ",
        annotation_position="top right",
        annotation_font=dict(color="#D17B00", size=11, family="DM Mono"),
    )

    # Green fill baseline (below threshold — "safe" area)
    fig2.add_trace(go.Scatter(
        x=spend_full["day"], y=[float(threshold)] * len(spend_full),
        mode="lines", line=dict(width=0),
        fill=None, showlegend=False, hoverinfo="skip",
    ))

    # Debit line
    fig2.add_trace(go.Scatter(
        x=spend_full["day"], y=spend_full["daily_debit"],
        mode="lines+markers",
        name="Daily Debit",
        line=dict(color=RED, width=2.5, shape="spline"),
        marker=dict(size=7, color=RED,
                    line=dict(width=1.5, color="#FFFFFF")),
        customdata=spend_full[["txn_count"]].values,
        hovertemplate=("<b>%{x|%d %b %Y}</b><br>"
                       "Debit: ₹%{y:,.0f}<br>"
                       "Transactions: %{customdata[0]}<extra></extra>"),
    ))

    # Over-threshold markers (accented)
    over_mask = spend_full["daily_debit"] > threshold
    if over_mask.any():
        over_df = spend_full[over_mask]
        fig2.add_trace(go.Scatter(
            x=over_df["day"], y=over_df["daily_debit"],
            mode="markers", name="Over threshold",
            marker=dict(size=11, color=RED, symbol="circle",
                        line=dict(width=2, color="#FFFFFF")),
            hovertemplate=("<b>%{x|%d %b %Y}</b><br>"
                           "⚠️ Over by ₹%{customdata:,.0f}<extra></extra>"),
            customdata=(over_df["daily_debit"] - float(threshold)).values,
        ))

    fig2.update_layout(**plotly_layout(
        height=400,
        legend=dict(orientation="h", y=1.12, x=0),
        yaxis=dict(
            gridcolor="#E4E7EB", linecolor="#E4E7EB", zerolinecolor="#E4E7EB",
            title="Amount (INR)", rangemode="tozero",
        ),
        xaxis=dict(
            gridcolor="#E4E7EB", linecolor="#E4E7EB", zerolinecolor="#E4E7EB",
            title="Date", tickformat="%d %b",
        ),
    ))
    st.plotly_chart(fig2, use_container_width=True)

    if over_days > 0:
        info_note(
            f"You exceeded the ₹{int(threshold)} daily threshold on "
            f"**{over_days} day(s)** in the last {spend_window} days."
        )


# ═══════════════════════════════════════════════════════════════════════
# (g) Top N Merchants — Lakebase silver_transaction, date-filtered
# ═══════════════════════════════════════════════════════════════════════
section_head(
    f"Top {top_n} Merchants · Total Value",
    "silver_transaction (Lakebase, date-filtered)",
)

merchant_sql = f"""
  SELECT
    counterparty,
    SUM(amount) AS total_amount,
    COUNT(*) AS txn_count
  FROM {T_LB_SILVER}
  WHERE CAST(COALESCE(txn_datetime_local, event_time) AS DATE) BETWEEN '{start_d}' AND '{end_d}'
    AND counterparty IS NOT NULL
    AND direction = 'DEBIT'
  GROUP BY counterparty
  ORDER BY total_amount DESC
  LIMIT {top_n}
"""
try:
    merchants = q(merchant_sql)
except Exception as e:
    st.error(f"Error loading merchants: {e}")
    merchants = pd.DataFrame()

if merchants.empty:
    empty_state("No merchant data in range",
                "Try widening the date range in the sidebar.")
else:
    merchants["total_amount"] = pd.to_numeric(merchants["total_amount"], errors="coerce")
    merchants["txn_count"] = merchants["txn_count"].astype(int)
    merchants = merchants.sort_values("total_amount", ascending=True)

    fig = px.bar(
        merchants, x="total_amount", y="counterparty", orientation="h",
        text=merchants["txn_count"].apply(lambda n: f"{n} txn"),
        labels={"total_amount": "Amount (INR)", "counterparty": ""},
    )
    fig.update_traces(marker_color=AMBER, textposition="outside",
                       hovertemplate="<b>%{y}</b><br>₹%{x:,.0f}<extra></extra>")
    fig.update_layout(**plotly_layout(
        height=max(300, top_n * 36 + 60),
        margin=dict(l=10, r=60, t=30, b=10),
    ))
    st.plotly_chart(fig, use_container_width=True)


st.markdown(
    '<div style="margin-top:28px;color:#8A96B0;font-size:11px;text-align:center;'
    'font-family:JetBrains Mono, monospace;letter-spacing:0.08em;">'
    '● DATA CACHED 30s · PRESS R TO REFRESH'
    '</div>',
    unsafe_allow_html=True,
)
