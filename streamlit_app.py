"""Lynch-Clark Stock Intelligence Terminal v3.0
================================================
Peter Lynch × Benjamin Graham | Small & Mid-Cap Scanner
SEC 10-Q/10-K + Analyst Consensus | Forward PEG Focus
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sqlite3
import numpy as np

st.set_page_config(
    layout="wide",
    page_title="Lynch-Clark | Stock Intelligence",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

# ── Dark Terminal Theme ──────────────────────────────────────────────────────
st.markdown("""
<style>
/* Global */
.stApp { background-color: #0d1117 !important; color: #c9d1d9 !important; }
.main .block-container { padding-top: 1rem; max-width: 1440px; }

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #161b22 !important;
    border-right: 1px solid #30363d;
}
[data-testid="stSidebar"] * { color: #c9d1d9 !important; }
[data-testid="stSidebar"] hr { border-color: #30363d !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #161b22, #1c2128) !important;
    border: 1px solid #30363d !important;
    border-radius: 10px !important;
    padding: 16px !important;
}
[data-testid="stMetricValue"] { color: #f0f6fc !important; font-family: monospace !important; }
[data-testid="stMetricLabel"] { color: #8b949e !important; font-size: 12px !important; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background-color: #0d1117 !important;
    border-bottom: 1px solid #30363d;
    gap: 2px;
}
.stTabs [data-baseweb="tab"] {
    background-color: transparent !important;
    color: #8b949e !important;
    border-radius: 6px 6px 0 0;
    padding: 8px 18px;
    font-size: 13px;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background-color: #161b22 !important;
    color: #58a6ff !important;
    border-top: 2px solid #58a6ff !important;
}

/* Typography */
h1 { color: #f0f6fc !important; font-family: monospace; letter-spacing: -0.5px; }
h2, h3, h4 { color: #c9d1d9 !important; }
p, li { color: #c9d1d9 !important; }
blockquote { border-left: 3px solid #30363d; padding-left: 12px; color: #8b949e !important; }

/* Dividers */
hr { border-color: #30363d !important; margin: 10px 0 !important; }

/* Inputs */
.stSelectbox > div > div,
.stMultiSelect > div > div { background-color: #161b22 !important; border-color: #30363d !important; }
.stTextInput > div > div { background-color: #161b22 !important; border-color: #30363d !important; }
input { color: #c9d1d9 !important; }
label { color: #8b949e !important; font-size: 12px !important; }

/* Alerts */
[data-testid="stAlert"] { background-color: #161b22 !important; border-color: #30363d !important; }

/* Dataframe */
[data-testid="stDataFrame"] { border: 1px solid #30363d; border-radius: 6px; }

/* Checkboxes, radio */
.stCheckbox span, .stRadio span { color: #c9d1d9 !important; }
.stRadio [role="radiogroup"] label { color: #c9d1d9 !important; }

/* Scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #161b22; }
::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
</style>
""", unsafe_allow_html=True)

# ── Constants ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "scanner.db")
CSV_PATH = os.path.join(BASE_DIR, "best_setups_database.csv")


def peg_color(peg):
    if peg is None:
        return "#555555"
    if peg < 0.20:
        return "#ff4444"
    if peg < 0.50:
        return "#ff8844"
    if peg < 1.00:
        return "#00cc44"
    if peg < 1.50:
        return "#ffaa00"
    return "#cc2222"


def peg_label(peg):
    if peg is None:
        return "N/A"
    if peg < 0.20:
        return "🔥 EXTREME BUY"
    if peg < 0.50:
        return "✅ STRONG BUY"
    if peg < 1.00:
        return "📈 BUY ZONE"
    if peg < 1.50:
        return "⚠️ FAIRLY VALUED"
    return "🔴 OVERVALUED"


def fmt_mcap(v):
    try:
        if v is None or pd.isna(v):
            return "—"
        if v >= 1e9:
            return f"${v/1e9:.1f}B"
        return f"${v/1e6:.0f}M"
    except Exception:
        return "—"


def fmt_num(v, decimals=2, prefix="", suffix=""):
    try:
        if v is None or pd.isna(v):
            return "—"
        return f"{prefix}{v:.{decimals}f}{suffix}"
    except Exception:
        return "—"


# ── Data Loading ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            df = pd.read_sql_query(
                "SELECT * FROM scans ORDER BY composite_score DESC NULLS LAST, scan_date DESC",
                conn,
            )
            conn.close()
            if not df.empty:
                return df, "SQLite"
        except Exception:
            pass
    if os.path.exists(CSV_PATH):
        return pd.read_csv(CSV_PATH), "CSV"
    return pd.DataFrame(), None


def get_col(df, *candidates):
    """Find first matching column, case-insensitive."""
    for name in candidates:
        if name in df.columns:
            return name
        lower = name.lower()
        for col in df.columns:
            if col.lower() == lower:
                return col
    return None


df, source = load_data()

if df.empty:
    st.error("📭 No scan data found.")
    st.info("Run `python3 batch_scanner.py 50` to generate scan results first.")
    st.stop()

# ── Column Map ───────────────────────────────────────────────────────────────
C = {
    "ticker":    get_col(df, "Ticker", "ticker"),
    "signal":    get_col(df, "Signal", "signal"),
    "price":     get_col(df, "Price", "price"),
    "mcap":      get_col(df, "Market_Cap", "market_cap", "Mcap"),
    "ttm_eps":   get_col(df, "TTM_EPS", "Ttm_Eps", "ttm_eps"),
    "eps_torque":get_col(df, "EPS_Torque", "Eps_Torque", "eps_torque"),
    "growth":    get_col(df, "EPS_Growth_Rate", "Eps_Growth_Rate", "eps_growth_rate"),
    "fwd_pe":    get_col(df, "Forward_PE", "Forward_Pe", "forward_pe"),
    "fwd_peg":   get_col(df, "Forward_PEG", "Forward_Peg", "forward_peg"),
    "score":     get_col(df, "Composite_Score", "composite_score"),
    "de":        get_col(df, "Debt_Equity", "debt_equity"),
    "fcf_yield": get_col(df, "FCF_Yield", "Fcf_Yield", "fcf_yield"),
    "inst_own":  get_col(df, "Inst_Ownership_Pct", "inst_ownership_pct"),
    "rev_growth":get_col(df, "Revenue_Growth", "revenue_growth"),
    "roe":       get_col(df, "ROE", "Roe", "roe"),
    "gm":        get_col(df, "Gross_Margin", "gross_margin"),
    "scan_date": get_col(df, "Scan_Date", "scan_date"),
    "eps_source":get_col(df, "EPS_Source", "Eps_Source", "eps_source"),
    "eps_conf":  get_col(df, "EPS_Confidence", "Eps_Confidence", "eps_confidence"),
    "div_pct":   get_col(df, "Divergence_Pct", "divergence_pct"),
    "analyst_eps":get_col(df, "Analyst_Forward_EPS", "analyst_forward_eps"),
    "sec_eps":   get_col(df, "SEC_Extracted_EPS", "sec_extracted_eps"),
    "ps":        get_col(df, "Price_To_Sales", "price_to_sales"),
    "pb":        get_col(df, "Price_To_Book", "price_to_book"),
    "avg_vol":   get_col(df, "Avg_Volume", "avg_volume"),
    "filing":    get_col(df, "Filing_Type", "filing_type"),
}

# Coerce numeric columns
for key in ["fwd_peg", "growth", "fwd_pe", "price", "mcap", "score", "de",
            "fcf_yield", "inst_own", "ps", "pb", "ttm_eps", "eps_torque",
            "rev_growth", "roe", "avg_vol"]:
    col = C.get(key)
    if col and col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

sig_col = C["signal"]
peg_col = C["fwd_peg"]
ticker_col = C["ticker"]
score_col = C["score"]


# ── Graham Score ─────────────────────────────────────────────────────────────
def graham_score(row):
    """0–100 score based on Graham Margin of Safety criteria."""
    score, n = 0, 0

    def add(val, thresholds):
        """thresholds: list of (cutoff, points) from best to worst."""
        nonlocal score, n
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return
        for cutoff, pts in thresholds:
            if val <= cutoff:
                score += pts
                n += 1
                return
        score += 0
        n += 1

    pe = row.get(C["fwd_pe"]) if C["fwd_pe"] else None
    add(pe, [(10, 100), (15, 75), (20, 50), (30, 25)])

    ps = row.get(C["ps"]) if C["ps"] else None
    add(ps, [(0.5, 100), (1.0, 75), (1.5, 50), (3.0, 25)])

    pb = row.get(C["pb"]) if C["pb"] else None
    add(pb, [(1.0, 100), (1.5, 75), (2.5, 40), (4.0, 15)])

    de = row.get(C["de"]) if C["de"] else None
    add(de, [(0.3, 100), (0.5, 75), (1.0, 40), (2.0, 15)])

    fcf = row.get(C["fcf_yield"]) if C["fcf_yield"] else None
    if fcf is not None and not (isinstance(fcf, float) and np.isnan(fcf)):
        if fcf > 10:
            score += 100
        elif fcf > 5:
            score += 75
        elif fcf > 2:
            score += 40
        elif fcf > 0:
            score += 20
        n += 1

    return round(score / n, 1) if n > 0 else None


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Filters")
    st.markdown("---")

    all_sigs = sorted(df[sig_col].dropna().unique().tolist()) if sig_col else []
    sel_signals = st.multiselect(
        "Signal Types",
        options=all_sigs,
        default=[s for s in all_sigs if "BUY" in str(s) or "TURNAROUND" in str(s)],
    )

    st.markdown("---")
    st.markdown("**Market Cap Range**")
    cap_options = {
        "Small Cap  (<$2B)":     (100,   2_000),
        "Small+Mid  (<$5B)":     (100,   5_000),
        "Mid-Large  (<$10B)":    (100,  10_000),
        "All Sizes":             (0,    10_000),
    }
    cap_label = st.selectbox("Cap Preset", list(cap_options.keys()), index=0)
    cap_min_m, cap_max_m = cap_options[cap_label]

    st.markdown("---")
    peg_max = st.slider("Max PEG", 0.1, 5.0, 2.0, 0.1,
                        help="Lynch: PEG < 1.0 = BUY zone. PEG < 0.5 = Strong Buy.")

    st.markdown("---")
    low_vol = st.checkbox(
        "Low Volume Only (<10M)",
        value=False,
        help="Graham: prefer overlooked stocks with limited institutional awareness",
    )

    st.markdown("---")
    method = st.radio(
        "Analysis Mode",
        ["Lynch (PEG-first)", "Graham (Value-first)", "Combined"],
        index=2,
    )

    st.markdown("---")
    st.markdown(f"**Source:** `{source}`")
    st.markdown(f"**Records:** `{len(df):,}`")
    if C["scan_date"] and not df[C["scan_date"]].isna().all():
        latest = str(df[C["scan_date"]].dropna().max())[:10]
        st.markdown(f"**Last Scan:** `{latest}`")


# ── Apply Filters ────────────────────────────────────────────────────────────
filt = df.copy()

if sel_signals and sig_col:
    filt = filt[filt[sig_col].isin(sel_signals)]

if C["mcap"]:
    filt = filt[
        filt[C["mcap"]].isna() |
        ((filt[C["mcap"]] >= cap_min_m * 1e6) & (filt[C["mcap"]] <= cap_max_m * 1e6))
    ]

if peg_col:
    filt = filt[filt[peg_col].isna() | (filt[peg_col] <= peg_max)]

if low_vol and C["avg_vol"]:
    filt = filt[filt[C["avg_vol"]].isna() | (filt[C["avg_vol"]] < 10e6)]

has_data = filt[filt[sig_col] != "NO DATA"].copy() if sig_col else filt.copy()
has_peg  = has_data[has_data[peg_col].notna()].copy() if peg_col else pd.DataFrame()


# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("""
<div style="padding:16px 0 8px 0">
  <span style="font-size:32px;font-family:monospace;font-weight:bold;color:#f0f6fc">
    📈 Lynch-Clark Stock Intelligence Terminal
  </span><br>
  <span style="color:#8b949e;font-size:13px">
    Peter Lynch PEG Method × Benjamin Graham Margin of Safety &nbsp;|&nbsp;
    SEC 10-Q/10-K + Analyst Consensus &nbsp;|&nbsp;
    NASDAQ Small & Mid-Cap Focus
  </span>
</div>
""", unsafe_allow_html=True)
st.markdown("---")


# ── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📡 Command Center",
    "🟢 Lynch Picks",
    "📐 Graham Screen",
    "🔬 PEG Calculator",
    "📋 All Results",
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — COMMAND CENTER
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    total    = len(filt)
    w_data   = len(has_data)
    buy_cnt  = len(filt[filt[sig_col].str.contains("BUY", na=False)]) if sig_col else 0

    best_peg, best_ticker = None, "—"
    if peg_col and not has_peg.empty:
        idx = has_peg[peg_col].idxmin()
        best_peg    = round(has_peg.loc[idx, peg_col], 3)
        best_ticker = has_peg.loc[idx, ticker_col] if ticker_col else "—"

    avg_score = None
    if score_col and not has_data.empty and score_col in has_data.columns:
        avg_score = has_data[score_col].dropna().mean()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📊 Scanned",     f"{total:,}")
    c2.metric("✅ With Data",   f"{w_data:,}")
    c3.metric("🎯 BUY Signals", f"{buy_cnt:,}")
    c4.metric("🏆 Best PEG",
              f"{best_peg:.3f}" if best_peg else "—",
              delta=best_ticker if best_ticker != "—" else None)
    c5.metric("⭐ Avg Score",   f"{avg_score:.1f}/100" if avg_score else "—")

    st.markdown("---")
    col_l, col_r = st.columns(2)

    # ── Signal donut ──
    with col_l:
        if sig_col:
            cats = {
                "🔥 EXTREME BUY": 0, "✅ STRONG BUY": 0, "📈 BUY": 0,
                "⚠️ HOLD": 0, "🔴 SELL": 0, "🔄 TURNAROUND": 0, "NO DATA": 0,
            }
            for s, cnt in filt[sig_col].value_counts().items():
                if "EXTREME"    in str(s): cats["🔥 EXTREME BUY"] += cnt
                elif "STRONG"   in str(s): cats["✅ STRONG BUY"]  += cnt
                elif "BUY"      in str(s): cats["📈 BUY"]          += cnt
                elif "HOLD"     in str(s): cats["⚠️ HOLD"]         += cnt
                elif "TURNAROUND" in str(s): cats["🔄 TURNAROUND"] += cnt
                elif "NO DATA"  in str(s): cats["NO DATA"]          += cnt
                else:                      cats["🔴 SELL"]          += cnt

            cat_df = pd.DataFrame(
                [(k, v) for k, v in cats.items() if v > 0],
                columns=["Signal", "Count"]
            )
            cmap = {
                "🔥 EXTREME BUY": "#ff4444", "✅ STRONG BUY": "#00cc44",
                "📈 BUY": "#4488ff", "⚠️ HOLD": "#ffaa00",
                "🔴 SELL": "#cc2222", "🔄 TURNAROUND": "#aa44ff", "NO DATA": "#444444",
            }
            fig = go.Figure(go.Pie(
                labels=cat_df["Signal"], values=cat_df["Count"],
                marker_colors=[cmap.get(s, "#888") for s in cat_df["Signal"]],
                hole=0.55,
                textinfo="label+value",
                hovertemplate="%{label}: %{value}<extra></extra>",
            ))
            fig.update_layout(
                title=dict(text="Signal Distribution", font=dict(color="#c9d1d9")),
                paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
                font=dict(color="#c9d1d9"), showlegend=False,
                height=310, margin=dict(t=40, b=10, l=10, r=10),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Top picks cards ──
    with col_r:
        st.markdown("### 🏆 Top Picks by Forward PEG")
        if not has_peg.empty:
            for _, row in has_peg.sort_values(peg_col).head(5).iterrows():
                peg_v  = row.get(peg_col)
                color  = peg_color(peg_v)
                ticker = row.get(ticker_col, "?")
                price  = row.get(C["price"], 0)  if C["price"]  else 0
                growth = row.get(C["growth"])     if C["growth"] else None
                mcap   = row.get(C["mcap"])       if C["mcap"]   else None
                sig    = row.get(sig_col, "")     if sig_col     else ""
                st.markdown(f"""
                <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;
                            padding:10px 14px;margin-bottom:6px;
                            display:flex;justify-content:space-between;align-items:center">
                  <div>
                    <span style="color:#f0f6fc;font-weight:bold;font-family:monospace;font-size:17px">{ticker}</span>
                    <span style="color:#8b949e;font-size:12px;margin-left:8px">${price:.2f} &nbsp;{fmt_mcap(mcap)}</span>
                    <br><span style="color:#8b949e;font-size:11px">{sig[:30]}</span>
                  </div>
                  <div style="text-align:right">
                    <span style="color:{color};font-weight:bold;font-family:monospace;font-size:20px">PEG {peg_v:.3f}</span>
                    <br><span style="color:#8b949e;font-size:11px">{f'+{growth:.0f}% EPS growth' if growth else ''}</span>
                  </div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No stocks with PEG data match current filters.")

    # ── PEG histogram ──
    if peg_col and not has_peg.empty:
        st.markdown("---")
        clipped = has_peg[has_peg[peg_col] <= 3][peg_col]
        if not clipped.empty:
            fig2 = go.Figure()
            fig2.add_trace(go.Histogram(
                x=clipped, nbinsx=25,
                marker_color="#4488ff", opacity=0.8, name="PEG",
            ))
            for x_val, lbl, clr in [
                (0.5, "Strong Buy", "#00cc44"),
                (1.0, "Buy Limit",  "#4488ff"),
                (1.5, "Sell Zone",  "#cc2222"),
            ]:
                fig2.add_vline(x=x_val, line_dash="dash", line_color=clr,
                               annotation_text=lbl,
                               annotation_font_color=clr,
                               annotation_position="top right")
            fig2.update_layout(
                title=dict(text="Forward PEG Distribution (capped at 3)", font=dict(color="#c9d1d9")),
                paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
                font=dict(color="#c9d1d9"), height=230,
                margin=dict(t=40, b=30, l=40, r=20),
                xaxis=dict(title="Forward PEG", gridcolor="#30363d"),
                yaxis=dict(title="Count",       gridcolor="#30363d"),
                showlegend=False,
            )
            st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — LYNCH PICKS
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown("### 🟢 Peter Lynch — PEG Ratio Leaderboard")
    st.markdown("""
> *"The P/E ratio of any company that's fairly priced will equal its growth rate."*
> — Peter Lynch, **One Up On Wall Street**

Lynch's Rule: **PEG < 1.0** = growth exceeds price &nbsp;|&nbsp; **PEG < 0.5** = strong buy &nbsp;|&nbsp; **PEG > 1.5** = avoid
""")

    col_a, col_b, col_c = st.columns(3)
    for col_w, rng, clr, bg, msg in [
        (col_a, "PEG < 0.5",    "#00cc44", "#001a0d", "Growing far faster than the price — classic undiscovered gem"),
        (col_b, "0.5 – 1.0",    "#4488ff", "#001133", "Growth exceeds valuation — reasonable entry"),
        (col_c, "1.0 – 1.5",    "#ffaa00", "#1a1100", "Fairly valued — wait for pullback or better data"),
    ]:
        col_w.markdown(f"""
        <div style="background:{bg};border:1px solid {clr};border-radius:8px;
                    padding:12px;text-align:center;height:78px">
          <div style="color:{clr};font-size:22px;font-weight:bold;font-family:monospace">{rng}</div>
          <div style="color:#8b949e;font-size:11px;margin-top:4px">{msg}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    if has_peg.empty:
        st.warning("No stocks with PEG data in current filters.")
    else:
        lynch = has_peg.sort_values(peg_col).copy()

        # ── Scatter: PEG vs Growth ──
        if C["growth"] and C["growth"] in lynch.columns:
            scat = lynch[
                lynch[peg_col].notna() & lynch[C["growth"]].notna() &
                (lynch[peg_col] <= 3.0) & (lynch[C["growth"]] > 0)
            ].copy()
            if not scat.empty:
                fig = px.scatter(
                    scat, x=C["growth"], y=peg_col, text=ticker_col,
                    color=sig_col if sig_col else None,
                    size=C["price"] if C["price"] else None,
                    color_discrete_map={
                        "🔥 EXTREME BUY": "#ff4444", "✅ STRONG BUY": "#00cc44",
                        "📈 BUY": "#4488ff", "⚠️ HOLD": "#ffaa00", "🔴 SELL": "#cc2222",
                    },
                    labels={C["growth"]: "EPS Growth Rate %", peg_col: "Forward PEG"},
                    title="Lynch Sweet Spot — High EPS Growth + Low PEG",
                )
                fig.add_hline(y=1.0, line_dash="dash", line_color="#4488ff",
                              annotation_text="PEG 1.0 — Fair Value",
                              annotation_font_color="#4488ff")
                fig.add_hline(y=0.5, line_dash="dash", line_color="#00cc44",
                              annotation_text="PEG 0.5 — Strong Buy",
                              annotation_font_color="#00cc44")
                fig.update_traces(textposition="top center", textfont_size=9,
                                  marker=dict(line=dict(width=0)))
                fig.update_layout(
                    paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
                    font=dict(color="#c9d1d9"), height=380,
                    margin=dict(t=50, b=50, l=50, r=20),
                    xaxis=dict(gridcolor="#30363d"),
                    yaxis=dict(gridcolor="#30363d"),
                    legend=dict(bgcolor="#161b22", bordercolor="#30363d"),
                )
                st.plotly_chart(fig, use_container_width=True)

        # ── Lynch Table ──
        lcols = [c for c in [
            ticker_col, sig_col,
            C["price"], C["mcap"], C["ttm_eps"], C["eps_torque"],
            C["growth"], C["fwd_pe"], peg_col, score_col,
            C["de"], C["fcf_yield"], C["inst_own"],
            C["eps_source"], C["eps_conf"],
        ] if c and c in lynch.columns]

        lrename = {
            ticker_col: "Ticker",     sig_col:        "Signal",
            C["price"]:  "Price",     C["mcap"]:      "Mkt Cap",
            C["ttm_eps"]:"TTM EPS",   C["eps_torque"]:"EPS Torque",
            C["growth"]: "Growth %",  C["fwd_pe"]:    "Fwd P/E",
            peg_col:     "Fwd PEG",   score_col:      "Score",
            C["de"]:     "D/E",       C["fcf_yield"]: "FCF Yield%",
            C["inst_own"]:"Inst Own%",C["eps_source"]:"EPS Source",
            C["eps_conf"]:"Confidence",
        }
        disp_l = lynch[lcols].head(60).rename(columns={k: v for k, v in lrename.items() if k})
        if "Mkt Cap" in disp_l.columns:
            disp_l["Mkt Cap"] = disp_l["Mkt Cap"].apply(fmt_mcap)
        st.dataframe(disp_l.reset_index(drop=True), use_container_width=True, height=420)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — GRAHAM SCREEN
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### 📐 Benjamin Graham — Margin of Safety Screen")
    st.markdown("""
> *"The margin of safety is always dependent on the price paid."*
> — Benjamin Graham, **The Intelligent Investor**

Graham's criteria combined with low-volume undiscovered stocks: **P/E < 15**, **P/S < 1.5**, **P/B < 1.5**, **D/E < 0.5**, **FCF Yield > 5%**
""")

    g1, g2, g3, g4, g5 = st.columns(5)
    for w, lbl, sub, clr in [
        (g1, "P/E < 15",  "Moderate earnings price",    "#4488ff"),
        (g2, "P/S < 1.5", "Price-to-Sales ratio",       "#00cc44"),
        (g3, "P/B < 1.5", "Trading near book value",    "#ffaa00"),
        (g4, "D/E < 0.5", "Conservative balance sheet", "#aa44ff"),
        (g5, "FCF > 5%",  "Real free cash generation",  "#ff8844"),
    ]:
        w.markdown(f"""
        <div style="background:#161b22;border:1px solid {clr};border-radius:8px;
                    padding:10px;text-align:center;height:76px">
          <div style="color:{clr};font-weight:bold;font-size:15px">{lbl}</div>
          <div style="color:#8b949e;font-size:11px;margin-top:4px">{sub}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    ps_col = C["ps"]
    pb_col = C["pb"]
    has_graham_cols = (ps_col and ps_col in has_data.columns) or (pb_col and pb_col in has_data.columns)

    if not has_graham_cols:
        st.info("""
**P/S and P/B data not yet in your scan results.**

These Graham metrics are fetched from yfinance starting with the updated scanner.
Run a fresh batch to populate them:
```
python3 batch_scanner.py 50 --mode small
```
Graham scoring below uses P/E, D/E, and FCF Yield — which are already in your data.
""")

    if not has_data.empty:
        g_df = has_data.copy()
        g_df["Graham_Score"] = g_df.apply(graham_score, axis=1)
        g_df = g_df[g_df["Graham_Score"].notna()].sort_values("Graham_Score", ascending=False)

        if not g_df.empty:
            gc1, gc2 = st.columns([1, 1])

            with gc1:
                top_g = g_df.head(12)
                tickers_g = top_g[ticker_col].tolist() if ticker_col else []
                scores_g  = top_g["Graham_Score"].tolist()
                colors_g  = [f"rgb({max(0,int((1-s/100)*200))},{int(s/100*200)},60)"
                             for s in scores_g]
                fig_g = go.Figure(go.Bar(
                    x=scores_g, y=tickers_g,
                    orientation="h",
                    marker_color=colors_g,
                    text=[f"{s:.0f}" for s in scores_g],
                    textposition="outside",
                    textfont=dict(color="#c9d1d9", size=11),
                ))
                fig_g.update_layout(
                    title=dict(text="Top Graham Scores", font=dict(color="#c9d1d9")),
                    paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
                    font=dict(color="#c9d1d9"), height=380,
                    margin=dict(t=40, b=20, l=10, r=60),
                    xaxis=dict(title="Score (0–100)", gridcolor="#30363d", range=[0, 115]),
                    yaxis=dict(autorange="reversed", gridcolor="#30363d"),
                )
                st.plotly_chart(fig_g, use_container_width=True)

            with gc2:
                if ps_col and pb_col and ps_col in g_df.columns and pb_col in g_df.columns:
                    scat_g = g_df[g_df[ps_col].notna() & g_df[pb_col].notna()].head(60)
                    if not scat_g.empty:
                        fig_ps = px.scatter(
                            scat_g, x=ps_col, y=pb_col, text=ticker_col,
                            color="Graham_Score",
                            color_continuous_scale=["#cc2222", "#ffaa00", "#00cc44"],
                            title="P/S vs P/B — Graham Sweet Spot: Bottom-Left Corner",
                            labels={ps_col: "Price / Sales", pb_col: "Price / Book"},
                        )
                        fig_ps.add_hline(y=1.5, line_dash="dash", line_color="#ffaa00",
                                         annotation_text="P/B = 1.5",
                                         annotation_font_color="#ffaa00")
                        fig_ps.add_vline(x=1.5, line_dash="dash", line_color="#ffaa00",
                                         annotation_text="P/S = 1.5",
                                         annotation_font_color="#ffaa00")
                        fig_ps.update_traces(textposition="top center", textfont_size=9)
                        fig_ps.update_layout(
                            paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
                            font=dict(color="#c9d1d9"), height=380,
                            margin=dict(t=50, b=40, l=50, r=20),
                            xaxis=dict(gridcolor="#30363d"),
                            yaxis=dict(gridcolor="#30363d"),
                        )
                        st.plotly_chart(fig_ps, use_container_width=True)
                    else:
                        st.info("Not enough P/S + P/B data for scatter plot.")
                else:
                    # Show PE vs D/E as a proxy
                    pe_col = C["fwd_pe"]
                    de_col = C["de"]
                    if pe_col and de_col and pe_col in g_df.columns and de_col in g_df.columns:
                        scat2 = g_df[g_df[pe_col].notna() & g_df[de_col].notna()].head(60)
                        if not scat2.empty:
                            fig_ped = px.scatter(
                                scat2, x=de_col, y=pe_col, text=ticker_col,
                                color="Graham_Score",
                                color_continuous_scale=["#cc2222", "#ffaa00", "#00cc44"],
                                title="P/E vs D/E — Graham proxy (run fresh scan for P/S & P/B)",
                                labels={de_col: "Debt / Equity", pe_col: "Forward P/E"},
                            )
                            fig_ped.add_hline(y=15, line_dash="dash", line_color="#ffaa00",
                                             annotation_text="P/E = 15 (Graham limit)",
                                             annotation_font_color="#ffaa00")
                            fig_ped.add_vline(x=0.5, line_dash="dash", line_color="#4488ff",
                                             annotation_text="D/E = 0.5",
                                             annotation_font_color="#4488ff")
                            fig_ped.update_traces(textposition="top center", textfont_size=9)
                            fig_ped.update_layout(
                                paper_bgcolor="#0d1117", plot_bgcolor="#161b22",
                                font=dict(color="#c9d1d9"), height=380,
                                margin=dict(t=50, b=40, l=50, r=20),
                                xaxis=dict(gridcolor="#30363d"),
                                yaxis=dict(gridcolor="#30363d"),
                            )
                            st.plotly_chart(fig_ped, use_container_width=True)

            # ── Graham Table ──
            g_cols = [c for c in [
                ticker_col, sig_col, "Graham_Score",
                C["price"], C["fwd_pe"], C["ps"], C["pb"],
                C["de"], C["fcf_yield"], C["mcap"], peg_col,
            ] if c and c in g_df.columns]
            g_rename = {
                ticker_col:     "Ticker",     sig_col:       "Signal",
                "Graham_Score": "G-Score",    C["price"]:    "Price",
                C["fwd_pe"]:    "Fwd P/E",    C["ps"]:       "P/S",
                C["pb"]:        "P/B",         C["de"]:       "D/E",
                C["fcf_yield"]: "FCF Yield%", C["mcap"]:     "Mkt Cap",
                peg_col:        "Fwd PEG",
            }
            disp_g = g_df[g_cols].head(50).rename(columns={k: v for k, v in g_rename.items() if k})
            if "Mkt Cap" in disp_g.columns:
                disp_g["Mkt Cap"] = disp_g["Mkt Cap"].apply(fmt_mcap)
            st.dataframe(disp_g.reset_index(drop=True), use_container_width=True, height=380)
        else:
            st.warning("No stocks pass minimum Graham scoring threshold with current data.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — PEG CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown("### 🔬 Forward PEG Calculator & Verification")

    col_fm, col_vf = st.columns([1, 1])

    with col_fm:
        st.markdown("""
#### Formula Breakdown

```
① Forward P/E  = Current Price ÷ EPS Torque
② Growth Rate  = (EPS Torque − TTM EPS) ÷ |TTM EPS| × 100
③ Forward PEG  = Forward P/E ÷ Growth Rate (%)
```

**EPS Torque** = Latest filing EPS × annualization multiplier:

| Filing Period   | Multiplier | Logic                    |
|-----------------|-----------|--------------------------|
| 10-Q quarterly  | × 4       | Q1/Q2/Q3 → annualize     |
| 10-Q 6-month    | × 2       | H1 → double              |
| 10-Q 9-month    | × 1.33    | 9M → scale to 12M        |
| 10-K annual     | × 1       | Already full year        |

---

**Hybrid EPS Sourcing — 3-Tier:**

| Tier | Source                              | Confidence |
|------|-------------------------------------|------------|
| A    | Analyst consensus (`forwardEps`)    | High       |
| B    | SEC + analyst within 30% divergence | High       |
| C    | SEC extraction only                 | Low        |

---

**Lynch PEG Thresholds:**

| PEG Range | Signal         | Lynch Interpretation          |
|-----------|----------------|-------------------------------|
| < 0.20    | 🔥 EXTREME BUY | Massively underpriced growth  |
| 0.20–0.50 | ✅ STRONG BUY  | Classic Lynch gem             |
| 0.50–1.00 | 📈 BUY         | Growth exceeds valuation      |
| 1.00–1.50 | ⚠️ FAIR VALUE  | Priced for growth, no cushion |
| > 1.50    | 🔴 AVOID       | Growth already priced in      |
""")

    with col_vf:
        st.markdown("#### Live Verification — Pick a Stock")
        tickers_w_peg = has_data[has_data[peg_col].notna()][ticker_col].sort_values().tolist() \
            if (peg_col and ticker_col and not has_data.empty) else []

        if tickers_w_peg:
            sel = st.selectbox("Select Ticker:", tickers_w_peg)
            row = has_data[has_data[ticker_col] == sel].iloc[0]

            price_v  = row.get(C["price"])     if C["price"]     else None
            torque_v = row.get(C["eps_torque"])if C["eps_torque"] else None
            ttm_v    = row.get(C["ttm_eps"])   if C["ttm_eps"]   else None
            peg_v    = row.get(peg_col)
            fpe_v    = row.get(C["fwd_pe"])    if C["fwd_pe"]    else None
            grw_v    = row.get(C["growth"])    if C["growth"]    else None
            src_v    = row.get(C["eps_source"])if C["eps_source"] else "unknown"
            cnf_v    = row.get(C["eps_conf"])  if C["eps_conf"]  else "?"
            anl_v    = row.get(C["analyst_eps"])if C["analyst_eps"] else None
            sec_v    = row.get(C["sec_eps"])   if C["sec_eps"]   else None
            filing_v = row.get(C["filing"])    if C["filing"]    else "?"

            # Re-compute from raw values to verify
            calc_fpe   = (price_v / torque_v) if (price_v and torque_v) else None
            calc_grw   = None
            calc_peg   = None
            turnaround = False
            if ttm_v and torque_v:
                if ttm_v < 0 and torque_v > 0:
                    turnaround = True
                elif abs(ttm_v) >= 0.25:
                    calc_grw = ((torque_v - ttm_v) / abs(ttm_v)) * 100
                    if calc_grw and calc_grw > 0 and calc_fpe:
                        calc_peg = calc_fpe / calc_grw

            peg_match = (
                abs(calc_peg - peg_v) < 0.01
                if (calc_peg is not None and peg_v is not None) else None
            )

            # Worked calc card
            def _fmt(v, d=2, pfx="$"):
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    return "—"
                return f"{pfx}{v:.{d}f}" if pfx else f"{v:.{d}f}"

            fpe_disp  = fpe_v  if fpe_v  else calc_fpe
            grw_disp  = grw_v  if grw_v  else calc_grw
            ttm_str   = _fmt(ttm_v)
            torq_str  = _fmt(torque_v)
            fpe_str   = _fmt(fpe_disp, pfx="")
            grw_str   = f"{grw_disp:.1f}%" if grw_disp else "—"

            st.markdown(f"""
<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px;font-family:monospace;font-size:13px">
  <div style="color:#8b949e;margin-bottom:8px">
    Filing: <b style="color:#c9d1d9">{filing_v}</b> &nbsp;|&nbsp;
    Source: <b style="color:#c9d1d9">{src_v}</b> &nbsp;|&nbsp;
    Confidence: <b style="color:#c9d1d9">{cnf_v}</b>
  </div>
  <table style="width:100%;color:#c9d1d9;border-collapse:collapse">
    <tr style="border-bottom:1px solid #30363d">
      <td style="color:#8b949e;padding:4px 0">Current Price</td>
      <td style="text-align:right;color:#f0f6fc">{_fmt(price_v)}</td>
    </tr>
    <tr style="border-bottom:1px solid #30363d">
      <td style="color:#8b949e;padding:4px 0">EPS Torque (fwd)</td>
      <td style="text-align:right;color:#f0f6fc">{torq_str}</td>
    </tr>
    <tr style="border-bottom:1px solid #30363d">
      <td style="color:#8b949e;padding:4px 0">TTM EPS (trailing)</td>
      <td style="text-align:right;color:#f0f6fc">{ttm_str}</td>
    </tr>
    {"<tr><td colspan='2' style='color:#aa44ff;padding:6px 0'>⚠️ Turnaround: TTM negative → EPS now positive</td></tr>" if turnaround else ""}
    {"<tr><td colspan='2' style='height:6px'></td></tr>" }
    <tr style="border-bottom:1px solid #30363d">
      <td style="color:#8b949e;padding:4px 0">① Fwd P/E = {_fmt(price_v)} ÷ {torq_str}</td>
      <td style="text-align:right;color:#4488ff">{fpe_str}</td>
    </tr>
    <tr style="border-bottom:1px solid #30363d">
      <td style="color:#8b949e;padding:4px 0">② Growth = ({torq_str} − {ttm_str}) ÷ |{ttm_str}| × 100</td>
      <td style="text-align:right;color:#4488ff">{grw_str}</td>
    </tr>
    <tr>
      <td style="color:#f0f6fc;font-weight:bold;padding:8px 0;font-size:14px">③ Forward PEG (stored)</td>
      <td style="text-align:right;font-weight:bold;font-size:26px;color:{peg_color(peg_v) if peg_v else '#555'}">{_fmt(peg_v, d=4, pfx='')}</td>
    </tr>
  </table>
  {"<div style='color:#8b949e;font-size:11px;margin-top:6px'>Analyst EPS: " + _fmt(anl_v) + " &nbsp;|&nbsp; SEC extracted: " + _fmt(sec_v) + "</div>" if (anl_v or sec_v) else ""}
</div>
""", unsafe_allow_html=True)

            if peg_match is True:
                st.success("✅ PEG verified — stored value matches the formula exactly")
            elif peg_match is False:
                st.warning(f"⚠️ Discrepancy: stored={peg_v:.4f} vs re-computed={calc_peg:.4f if calc_peg else 'N/A'}")
            elif turnaround:
                st.info("🔄 Turnaround stock — PEG not meaningful when TTM was negative")

            # Large PEG display badge
            if peg_v and not turnaround:
                clr = peg_color(peg_v)
                lbl = peg_label(peg_v)
                st.markdown(f"""
<div style="background:#0d1117;border:2px solid {clr};border-radius:12px;
            padding:20px;text-align:center;margin-top:16px">
  <div style="color:{clr};font-size:42px;font-weight:bold;font-family:monospace">
    PEG {peg_v:.3f}
  </div>
  <div style="color:{clr};font-size:18px;margin-top:6px">{lbl}</div>
</div>
""", unsafe_allow_html=True)
        else:
            st.info("No stocks with PEG data in current filters.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ALL RESULTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown("### 📋 Complete Scan Results")

    sr1, sr2, sr3 = st.columns([2, 1, 1])
    with sr1:
        search = st.text_input("🔍 Search ticker:", placeholder="EZPW, TILE, SENEA …")
    with sr2:
        sort_opts = [c for c in [peg_col, score_col, C["growth"], C["fwd_pe"]] if c]
        sort_labels = {
            peg_col:       "Forward PEG ↑",
            score_col:     "Score ↓",
            C["growth"]:   "EPS Growth ↓",
            C["fwd_pe"]:   "Fwd P/E ↑",
        }
        sort_by = st.selectbox("Sort by:", sort_opts,
                               format_func=lambda c: sort_labels.get(c, c))
    with sr3:
        show_no_data = st.checkbox("Include NO DATA rows", value=False)

    out = filt.copy()
    if not show_no_data and sig_col:
        out = out[out[sig_col] != "NO DATA"]
    if search and ticker_col:
        out = out[out[ticker_col].str.upper().str.contains(search.upper(), na=False)]
    if sort_by and sort_by in out.columns:
        asc = sort_by in [peg_col, C["fwd_pe"]]
        out = out.sort_values(sort_by, ascending=asc, na_position="last")

    all_cols = [c for c in [
        ticker_col, sig_col,
        C["price"], C["mcap"],
        C["ttm_eps"], C["eps_torque"], C["growth"],
        C["fwd_pe"], peg_col, score_col,
        C["de"], C["fcf_yield"], C["inst_own"],
        C["ps"], C["pb"],
        C["rev_growth"], C["roe"],
        C["avg_vol"],
        C["eps_source"], C["eps_conf"],
        C["scan_date"],
    ] if c and c in out.columns]

    all_rename = {
        ticker_col:      "Ticker",      sig_col:        "Signal",
        C["price"]:      "Price",       C["mcap"]:      "Mkt Cap",
        C["ttm_eps"]:    "TTM EPS",     C["eps_torque"]:"EPS Torque",
        C["growth"]:     "Growth %",    C["fwd_pe"]:    "Fwd P/E",
        peg_col:         "Fwd PEG",     score_col:      "Score",
        C["de"]:         "D/E",         C["fcf_yield"]: "FCF Yield%",
        C["inst_own"]:   "Inst Own%",   C["ps"]:        "P/S",
        C["pb"]:         "P/B",         C["rev_growth"]:"Rev Grwth%",
        C["roe"]:        "ROE%",        C["avg_vol"]:   "Avg Volume",
        C["eps_source"]: "EPS Source",  C["eps_conf"]:  "Confidence",
        C["scan_date"]:  "Scan Date",
    }
    final = out[all_cols].rename(columns={k: v for k, v in all_rename.items() if k})
    if "Mkt Cap"   in final.columns: final["Mkt Cap"]   = final["Mkt Cap"].apply(fmt_mcap)
    if "Scan Date" in final.columns: final["Scan Date"] = final["Scan Date"].apply(lambda x: str(x)[:10] if pd.notna(x) else "—")
    if "Avg Volume" in final.columns:
        final["Avg Volume"] = final["Avg Volume"].apply(
            lambda x: f"{x/1e6:.1f}M" if pd.notna(x) else "—"
        )

    st.dataframe(final.reset_index(drop=True), use_container_width=True, height=520)
    st.caption(
        f"Showing {len(final):,} records &nbsp;|&nbsp; "
        f"Cap: {cap_label} &nbsp;|&nbsp; "
        f"Max PEG: {peg_max} &nbsp;|&nbsp; "
        f"Source: {source}"
    )
