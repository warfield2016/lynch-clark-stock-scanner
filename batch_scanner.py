#!/usr/bin/env python3
"""
Lynch-Clark EPS Torque & Forward PEG Batch Scanner v2.0
========================================================
Multi-factor stock screener with hybrid EPS sourcing and composite scoring.

Phases:
  1. Screen NASDAQ via Finviz (configurable cap/volume)
  2. Download latest 10-Q/10-K from SEC EDGAR
  3. Extract EPS via NLP regex from SEC filings
  4. Hybrid EPS resolution (analyst consensus + SEC) & metric calculation
  5. Multi-factor composite score & signal classification
  6. Save to SQLite + CSV
"""

import argparse
import logging
import os
import re
import shutil
import sys
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
import fitz  # PyMuPDF
from datetime import datetime
from finvizfinance.screener.overview import Overview
from sec_edgar_downloader import Downloader

from db import init_db, insert_batch, get_existing_tickers

# ── Logging ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("lynch_clark")

# ── Config ──
SEC_DELAY = 1.0  # seconds between SEC downloads
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SEC_DIR = os.path.join(BASE_DIR, "sec-edgar-filings")
OUTPUT_CSV = os.path.join(BASE_DIR, "best_setups_database.csv")

# Market cap presets
CAP_PRESETS = {
    "micro":      (5e7,  5e8),   # $50M–$500M
    "small":      (1e8,  2e9),   # $100M–$2B  (Lynch sweet spot)
    "small_mid":  (1e8,  5e9),   # $100M–$5B  (extended Lynch)
    "mid":        (2e9,  1e10),  # $2B–$10B
    "all":        (5e7,  1e10),  # $50M–$10B
}


def find_chrome():
    """Auto-detect Chrome/Chromium binary across platforms."""
    candidates = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    env_path = os.environ.get("CHROME_PATH")
    if env_path and os.path.exists(env_path):
        return env_path
    return None


CHROME = find_chrome()


def parse_args():
    parser = argparse.ArgumentParser(description="Lynch-Clark EPS Torque Scanner v2.0")
    parser.add_argument("max_tickers", nargs="?", type=int, default=10,
                        help="Maximum tickers to process per batch (default: 10)")
    parser.add_argument("--mode", choices=CAP_PRESETS.keys(), default="small",
                        help="Market cap range preset (default: small = $100M-$2B)")
    parser.add_argument("--csv", action="store_true", default=True,
                        help="Also export to CSV (default: True)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Concurrent yfinance fetch threads (default: 5)")
    return parser.parse_args()


def phase1_screen(cap_min=1e8, cap_max=2e9):
    """Phase 1: Fetch NASDAQ stocks from Finviz, filter by cap/volume."""
    log.info("═══ Phase 1: Screening NASDAQ via Finviz ═══")
    f = Overview()
    f.set_filter(filters_dict={"Exchange": "NASDAQ"})
    df = f.screener_view()
    if df.empty:
        log.warning("No stocks returned from Finviz.")
        return pd.DataFrame()

    df["Mcap"] = pd.to_numeric(df["Market Cap"], errors="coerce").fillna(0)
    df["Vol"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    out = df[(df["Mcap"] >= cap_min) & (df["Mcap"] <= cap_max) & (df["Vol"] < 1e7)]
    out = out.sort_values("Mcap", ascending=False)
    log.info(f"  {len(out)} stocks pass filter (${cap_min/1e6:.0f}M-${cap_max/1e9:.1f}B cap)")
    return out


def phase2_download(ticker):
    """Phase 2: Download latest 10-Q and 10-K for a single ticker."""
    dl = Downloader("LynchClarkScanner", "scanner@example.com")
    for form in ("10-Q", "10-K"):
        try:
            dl.get(form, ticker, limit=1, download_details=True)
        except Exception as e:
            log.warning(f"  {form} download failed for {ticker}: {e}")
    time.sleep(SEC_DELAY)


def phase3_extract(ticker):
    """Phase 3: Convert SEC HTML→PDF→text, run NLP regex for EPS."""
    if not CHROME:
        log.warning("  Chrome not found — skipping SEC text extraction")
        return None, None, None, None, None

    doc_texts = []
    filing_type = None

    for root, _, files in os.walk(SEC_DIR):
        if ticker not in root:
            continue
        for f in files:
            if not f.endswith("primary-document.html"):
                continue

            html = os.path.abspath(os.path.join(root, f))
            pdf = os.path.join(BASE_DIR, f"{ticker}_tmp.pdf")

            # Determine filing type from path
            if "10-K" in root:
                filing_type = "10-K"
            elif "10-Q" in root:
                filing_type = "10-Q"

            # Convert HTML to PDF via headless Chrome
            try:
                subprocess.run(
                    [CHROME, "--headless", "--disable-gpu",
                     "--no-pdf-header-footer", f"--print-to-pdf={pdf}",
                     f"file://{html}"],
                    check=True, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, timeout=30
                )
            except Exception:
                continue

            # Extract text from first 20 pages
            if os.path.exists(pdf):
                try:
                    doc = fitz.open(pdf)
                    txt = ""
                    for p in range(min(20, len(doc))):
                        txt += doc[p].get_text()
                    doc_texts.append(txt)
                    os.remove(pdf)
                except Exception:
                    pass

    if not doc_texts:
        return None, None, None, None, None

    full = re.sub(r"\s+", " ", " ".join(doc_texts))

    # ── Tier 1: Explicit guidance near EPS ──
    t1 = re.finditer(
        r"([^.]{0,80}?"
        r"(?:guidance|expect|outlook|project|estimate)"
        r"[^.]{0,80}?"
        r"(?:eps|earnings per share|net income per share)"
        r"[^.]{0,60}?\$?(\d+\.\d{2})[^.]{0,40}?\.)",
        full, re.IGNORECASE
    )
    tier1 = [(m.group(1).strip(), float(m.group(2))) for m in t1]

    # ── Tier 2: Loose EPS + number ──
    t2 = re.finditer(
        r"([^.]{0,50}?earnings per share[^.]{0,100}?\$?(\d+\.\d{2})[^.]{0,50}?)",
        full, re.IGNORECASE
    )
    tier2 = [(m.group(1).strip(), float(m.group(2))) for m in t2]

    # ── Filter out historical table extractions ──
    HISTORICAL_MARKERS = [
        "net income", "comprehensive income", "shareholders",
        "stockholders", "prior year", "previous period",
        "comparative", "restated", "consolidated statements",
        "operations", "loss from", "income from",
    ]

    def _is_historical(sentence):
        """Reject sentences that look like income statement tables, not guidance."""
        s = sentence.lower()
        hits = sum(1 for m in HISTORICAL_MARKERS if m in s)
        return hits >= 2

    tier1 = [m for m in tier1 if not _is_historical(m[0])]
    tier2 = [m for m in tier2 if not _is_historical(m[0])]

    # Pick best match
    matches = tier1 if tier1 else tier2
    if not matches:
        return None, None, filing_type, None, None

    def _period_rank(sentence):
        s = sentence.lower()
        if any(k in s for k in ["three months", "third quarter", "second quarter",
                                  "first quarter", "quarterly"]):
            return 0
        if any(k in s for k in ["six months", "6 months"]):
            return 1
        if any(k in s for k in ["nine months", "9 months"]):
            return 2
        return 3

    best = min(matches, key=lambda x: _period_rank(x[0]))
    sentence = best[0]
    eps = best[1]

    # ── Determine annualization multiplier ──
    # 10-K = annual filing → default multiplier is 1 (already annual)
    # 10-Q = quarterly filing → default multiplier is 4 (annualize)
    # Period keywords in the sentence can override this default.
    sentence_l = sentence.lower()

    if any(k in sentence_l for k in ["full year", "fiscal year", "annual",
                                      "full-year", "twelve months", "52 weeks"]):
        multiplier = 1
    elif any(k in sentence_l for k in ["nine months", "9 months", "nine-month"]):
        multiplier = 4 / 3
    elif any(k in sentence_l for k in ["six months", "6 months", "six-month",
                                        "half year", "half-year"]):
        multiplier = 2
    elif any(k in sentence_l for k in ["three months", "third quarter", "second quarter",
                                        "first quarter", "quarterly", "quarter ended"]):
        multiplier = 4
    else:
        # No period keyword found — use filing type as the default
        multiplier = 1 if filing_type == "10-K" else 4

    is_quarterly = (multiplier == 4)
    eps_torque = round(eps * multiplier, 2)
    return eps_torque, sentence, filing_type, is_quarterly, multiplier


def get_forward_eps(sec_eps_torque, yf_info):
    """
    Three-tier EPS sourcing:
      Tier A (high confidence):   yfinance forwardEps (analyst consensus)
      Tier B (medium confidence): SEC extraction validated within 30% of consensus
      Tier C (low confidence):    SEC extraction alone (no consensus available)
    Returns: (eps_value, source_label, confidence, analyst_eps, sec_eps, divergence_pct)
    """
    analyst_eps = yf_info.get("forwardEps")
    sec_eps = sec_eps_torque

    # Both available — cross-validate
    if analyst_eps and analyst_eps > 0 and sec_eps and sec_eps > 0:
        divergence = ((sec_eps - analyst_eps) / analyst_eps) * 100
        # If SEC extraction is within 30% of consensus, use consensus (validated)
        if abs(divergence) <= 30:
            return analyst_eps, "sec_validated", "high", analyst_eps, sec_eps, round(divergence, 1)
        else:
            # Large divergence — trust analyst consensus, flag divergence
            return analyst_eps, "analyst_consensus", "medium", analyst_eps, sec_eps, round(divergence, 1)

    # Only analyst consensus available
    if analyst_eps and analyst_eps > 0:
        return analyst_eps, "analyst_consensus", "high", analyst_eps, sec_eps, None

    # Only SEC extraction available
    if sec_eps and sec_eps > 0:
        return sec_eps, "sec_only", "low", None, sec_eps, None

    return None, None, None, analyst_eps, sec_eps, None


def phase4_calc(ticker, sec_eps_torque):
    """Phase 4: Fetch yfinance data, resolve EPS source, calculate all metrics."""
    try:
        t_obj = yf.Ticker(ticker)
        info = t_obj.info
        price = info.get("currentPrice", 0)
        ttm = info.get("trailingEps", 0)
        mcap = info.get("marketCap", 0)

        de_ratio = info.get("debtToEquity")
        de_normalized = (de_ratio / 100) if de_ratio else None

        inst_ownership = info.get("heldPercentInstitutions")
        revenue_growth = info.get("revenueGrowth")
        roe = info.get("returnOnEquity")
        gross_margin = info.get("grossMargins")

        # Graham metrics
        price_to_sales = info.get("priceToSalesTrailing12Months")
        price_to_book = info.get("priceToBook")
        avg_volume = info.get("averageVolume")

        fcf_per_share = None
        try:
            cf = t_obj.cashflow
            if not cf.empty:
                op_cf = cf.loc["Operating Cash Flow"].iloc[0] if "Operating Cash Flow" in cf.index else None
                capex = cf.loc["Capital Expenditure"].iloc[0] if "Capital Expenditure" in cf.index else None
                if op_cf and capex:
                    fcf = op_cf + capex
                    fcf_per_share = fcf / info.get("sharesOutstanding", 1)
        except Exception:
            pass

    except Exception:
        return None

    # ── Hybrid EPS sourcing ──
    eps_torque, eps_source, eps_confidence, analyst_eps, sec_eps, divergence = \
        get_forward_eps(sec_eps_torque, info)

    if not price or not eps_torque or eps_torque <= 0:
        return None

    MIN_TTM_FOR_GROWTH = 0.25
    MAX_GROWTH_RATE = 500.0

    fwd_pe = price / eps_torque

    growth = None
    fwd_peg = None
    signal_override = None

    if ttm and ttm != 0:
        if ttm < 0 and eps_torque > 0:
            signal_override = "🔄 TURNAROUND"
        elif abs(ttm) < MIN_TTM_FOR_GROWTH:
            growth = None
        else:
            growth = ((eps_torque - ttm) / abs(ttm)) * 100
            if growth > MAX_GROWTH_RATE:
                growth = MAX_GROWTH_RATE
            if growth > 0:
                fwd_peg = fwd_pe / growth

    # ── FCF Yield ──
    fcf_yield = None
    if fcf_per_share and price > 0:
        fcf_yield = (fcf_per_share / price) * 100

    return {
        "Price": round(price, 2),
        "Market_Cap": mcap,
        "TTM_EPS": round(ttm, 2) if ttm else None,
        "EPS_Torque": round(eps_torque, 2),
        "EPS_Source": eps_source,
        "EPS_Confidence": eps_confidence,
        "Analyst_Forward_EPS": round(analyst_eps, 2) if analyst_eps else None,
        "SEC_Extracted_EPS": round(sec_eps, 2) if sec_eps else None,
        "Divergence_Pct": divergence,
        "EPS_Growth_Rate": round(growth, 1) if growth else None,
        "Forward_PE": round(fwd_pe, 2),
        "Forward_PEG": round(fwd_peg, 4) if fwd_peg else None,
        "Debt_Equity": round(de_normalized, 2) if de_normalized else None,
        "FCF_Per_Share": round(fcf_per_share, 2) if fcf_per_share else None,
        "FCF_Yield": round(fcf_yield, 1) if fcf_yield else None,
        "Inst_Ownership_Pct": round(inst_ownership * 100, 1) if inst_ownership else None,
        "Revenue_Growth": round(revenue_growth * 100, 1) if revenue_growth else None,
        "ROE": round(roe * 100, 1) if roe else None,
        "Gross_Margin": round(gross_margin * 100, 1) if gross_margin else None,
        "Price_To_Sales": round(price_to_sales, 2) if price_to_sales else None,
        "Price_To_Book": round(price_to_book, 2) if price_to_book else None,
        "Avg_Volume": int(avg_volume) if avg_volume else None,
        "Signal_Override": signal_override,
    }


def compute_composite_score(metrics):
    """
    Multi-factor composite score (0-100) combining:
      PEG (35%) + FCF Yield (20%) + Balance Sheet (15%) + Quality (15%) + Momentum (15%)
    Returns: (score, breakdown_dict) or (None, None) if insufficient data.
    """
    scores = {}
    weights = {"peg": 0.35, "fcf": 0.20, "balance": 0.15, "quality": 0.15, "momentum": 0.15}

    # ── PEG Score (0-100, lower PEG = higher score) ──
    peg = metrics.get("Forward_PEG")
    if peg is not None and peg > 0:
        if peg < 0.5:
            scores["peg"] = 100 - (peg / 0.5) * 10       # 90-100
        elif peg < 1.0:
            scores["peg"] = 90 - (peg - 0.5) / 0.5 * 20  # 70-90
        elif peg < 1.5:
            scores["peg"] = 70 - (peg - 1.0) / 0.5 * 30  # 40-70
        elif peg < 2.5:
            scores["peg"] = 40 - (peg - 1.5) / 1.0 * 30  # 10-40
        else:
            scores["peg"] = max(0, 10 - (peg - 2.5) * 5)
    else:
        scores["peg"] = None

    # ── FCF Yield Score (0-100, higher yield = better) ──
    fcf_yield = metrics.get("FCF_Yield")
    if fcf_yield is not None:
        scores["fcf"] = min(100, max(0, fcf_yield * 10))  # 10% yield = 100
    else:
        scores["fcf"] = None

    # ── Balance Sheet Score (0-100, lower D/E = better) ──
    de = metrics.get("Debt_Equity")
    if de is not None:
        if de <= 0.3:
            scores["balance"] = 100
        elif de <= 1.0:
            scores["balance"] = 100 - (de - 0.3) / 0.7 * 40  # 60-100
        elif de <= 2.0:
            scores["balance"] = 60 - (de - 1.0) / 1.0 * 40   # 20-60
        else:
            scores["balance"] = max(0, 20 - (de - 2.0) * 10)
    else:
        scores["balance"] = 50  # Neutral if unknown

    # ── Quality Score (0-100, ROE + Gross Margin) ──
    roe = metrics.get("ROE")
    gm = metrics.get("Gross_Margin")
    quality_parts = []
    if roe is not None:
        roe_score = min(100, max(0, roe * 3))  # 33% ROE = 100
        quality_parts.append(roe_score)
    if gm is not None:
        gm_score = min(100, max(0, gm * 1.5))  # 67% gross margin = 100
        quality_parts.append(gm_score)
    scores["quality"] = sum(quality_parts) / len(quality_parts) if quality_parts else None

    # ── Momentum Score (revenue growth as proxy, 0-100) ──
    rev_growth = metrics.get("Revenue_Growth")
    if rev_growth is not None:
        if rev_growth >= 50:
            scores["momentum"] = 100
        elif rev_growth >= 0:
            scores["momentum"] = 50 + (rev_growth / 50) * 50  # 50-100
        elif rev_growth >= -20:
            scores["momentum"] = 50 + (rev_growth / 20) * 30  # 20-50
        else:
            scores["momentum"] = max(0, 20 + rev_growth)
    else:
        scores["momentum"] = None

    # ── Compute weighted composite ──
    total_weight = 0
    weighted_sum = 0
    for key, w in weights.items():
        if scores[key] is not None:
            weighted_sum += scores[key] * w
            total_weight += w

    if total_weight == 0:
        return None, scores

    composite = round(weighted_sum / total_weight, 1)
    return composite, scores


def phase5_signal(metrics):
    """Phase 5: Classify signal using composite score (or PEG fallback)."""
    composite, breakdown = compute_composite_score(metrics)

    if composite is not None:
        if composite >= 85:
            signal = "🔥 EXTREME BUY"
        elif composite >= 70:
            signal = "✅ STRONG BUY"
        elif composite >= 55:
            signal = "📈 BUY"
        elif composite >= 40:
            signal = "⚠️ HOLD"
        else:
            signal = "🔴 SELL"
    else:
        # Fallback to pure PEG if composite can't be computed
        peg = metrics.get("Forward_PEG")
        if peg is None:
            return "NO DATA", None, None
        if peg < 0.50:
            signal = "✅ STRONG BUY"
        elif peg < 1.00:
            signal = "📈 BUY"
        elif peg < 1.50:
            signal = "⚠️ HOLD"
        else:
            signal = "🔴 SELL"

    return signal, composite, breakdown


def fetch_yfinance_batch(tickers, max_workers=5):
    """Pre-fetch yfinance data concurrently for all tickers. ~5x faster."""
    results = {}
    log.info(f"  Pre-fetching yfinance data for {len(tickers)} tickers ({max_workers} threads)...")

    def _fetch(ticker):
        try:
            t_obj = yf.Ticker(ticker)
            return ticker, t_obj.info
        except Exception:
            return ticker, None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch, t) for t in tickers]
        for future in as_completed(futures):
            ticker, info = future.result()
            results[ticker] = info

    fetched = sum(1 for v in results.values() if v is not None)
    log.info(f"  Pre-fetched {fetched}/{len(tickers)} tickers successfully")
    return results


def main():
    args = parse_args()
    start = time.time()

    # Add file logging
    file_handler = logging.FileHandler(os.path.join(BASE_DIR, "scanner.log"))
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(file_handler)

    cap_min, cap_max = CAP_PRESETS[args.mode]
    log.info(f"Lynch-Clark Scanner v2.0 | Mode: {args.mode} | Max tickers: {args.max_tickers}")

    # Initialize SQLite
    db_conn = init_db()
    existing_tickers = get_existing_tickers(db_conn)

    # Also check CSV for backward compatibility
    if os.path.exists(OUTPUT_CSV):
        try:
            ex_df = pd.read_csv(OUTPUT_CSV)
            if "Ticker" in ex_df.columns:
                existing_tickers |= set(ex_df["Ticker"].dropna().tolist())
        except Exception:
            pass

    candidates = phase1_screen(cap_min, cap_max)
    if candidates.empty:
        log.warning("No candidates found. Exiting.")
        return

    initial_count = len(candidates)
    candidates = candidates[~candidates["Ticker"].isin(existing_tickers)]
    remaining_count = len(candidates)

    log.info(f"Database already contains {initial_count - remaining_count} of these candidates.")
    log.info(f"{remaining_count} stocks left to check.")

    candidates = candidates.head(args.max_tickers)
    tickers = candidates["Ticker"].tolist()

    if not tickers:
        log.info("All candidates have been processed. Exiting.")
        return

    log.info(f"═══ Processing {len(tickers)} new tickers ═══")

    rows = []
    for i, t in enumerate(tickers):
        pct = f"[{i+1}/{len(tickers)}]"
        log.info(f"{pct} {t}...")

        # Phase 2: Download SEC filings
        log.info(f"  Downloading SEC filings...")
        phase2_download(t)

        # Phase 3: Extract SEC EPS (may be None — hybrid sourcing handles it)
        log.info(f"  Extracting EPS from SEC filings...")
        sec_eps, sentence, ftype, is_q, multiplier = phase3_extract(t)

        if sec_eps:
            period_label = "Q×4" if is_q else f"×{round(multiplier, 2)}"
            log.info(f"  SEC EPS: ${sec_eps:.2f} ({period_label})")
        else:
            log.info(f"  No EPS in SEC docs, trying analyst consensus...")

        # Phase 4: Hybrid EPS resolution + metrics calculation
        log.info(f"  Calculating metrics (hybrid EPS sourcing)...")
        metrics = phase4_calc(t, sec_eps)

        if metrics is None:
            log.info(f"  No data available for {t}")
            rows.append({
                "Ticker": t,
                "Signal": "NO DATA",
                "Raw_Sentence": sentence[:200] if sentence else "No guidance found",
                "Scan_Date": datetime.now().isoformat(),
                "Filing_Type": ftype,
            })
            continue

        # Phase 5: Composite score + signal classification
        sig, composite, breakdown = phase5_signal(metrics)

        # Apply override and modifiers
        if metrics.get("Signal_Override"):
            sig = metrics["Signal_Override"]
        if metrics.get("Debt_Equity") and metrics["Debt_Equity"] > 1.0:
            sig += " ⚡HIGH DEBT"
        if metrics.get("Inst_Ownership_Pct") and metrics["Inst_Ownership_Pct"] > 50:
            sig += " 👁️WATCHED"
        if metrics.get("Revenue_Growth") is not None and metrics["Revenue_Growth"] < -10:
            sig += " 📉REV DOWN"
        if metrics.get("Divergence_Pct") and abs(metrics["Divergence_Pct"]) > 20:
            direction = "+" if metrics["Divergence_Pct"] > 0 else ""
            sig += f" 🔀DIV({direction}{metrics['Divergence_Pct']}%)"

        src = metrics.get("EPS_Source", "?")
        conf = metrics.get("EPS_Confidence", "?")
        log.info(f"  EPS={metrics['EPS_Torque']} [{src}/{conf}] "
                 f"Fwd P/E={metrics['Forward_PE']}, "
                 f"Growth={metrics.get('EPS_Growth_Rate','?')}%, "
                 f"PEG={metrics.get('Forward_PEG','?')}, "
                 f"Score={composite or '?'} -> {sig}")

        rows.append({
            "Ticker": t,
            **metrics,
            "Composite_Score": composite,
            "Filing_Type": ftype or "Unknown",
            "Is_Quarterly": is_q,
            "Signal": sig,
            "Raw_Sentence": sentence[:300] if sentence else "",
            "Scan_Date": datetime.now().isoformat()
        })

    # ── Phase 6: Save to SQLite + CSV ──
    new_df = pd.DataFrame(rows)

    # Outlier detection on the batch
    if not new_df.empty and "Forward_PEG" in new_df.columns:
        peg_vals = new_df["Forward_PEG"].dropna()
        if len(peg_vals) >= 5:
            mean, std = peg_vals.mean(), peg_vals.std()
            if std > 0:
                new_df["PEG_Outlier"] = new_df["Forward_PEG"].apply(
                    lambda x: abs((x - mean) / std) > 2.5 if pd.notna(x) else False
                )
                outlier_count = new_df["PEG_Outlier"].sum()
                if outlier_count:
                    log.warning(f"{outlier_count} PEG outlier(s) flagged for review")

    # Save to SQLite
    try:
        insert_batch(db_conn, rows)
        db_total = db_conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
        log.info(f"Saved {len(rows)} rows to SQLite ({db_total} total)")
    except Exception as e:
        log.error(f"SQLite save failed: {e}")

    # Save to CSV (backward compatible)
    if args.csv:
        if os.path.exists(OUTPUT_CSV):
            existing_full_df = pd.read_csv(OUTPUT_CSV)
            final_df = pd.concat([existing_full_df, new_df], ignore_index=True)
        else:
            final_df = new_df
        final_df.to_csv(OUTPUT_CSV, index=False)
        log.info(f"Saved to CSV: {OUTPUT_CSV} ({len(final_df)} total rows)")

    elapsed = round(time.time() - start, 1)

    log.info(f"{'='*60}")
    log.info(f"SCAN COMPLETE — {elapsed}s — {len(rows)} tickers processed")
    log.info(f"{'='*60}")

    buys = new_df[new_df["Signal"].str.contains("BUY", na=False)]
    if not buys.empty:
        display_cols = [c for c in ["Ticker", "Price", "EPS_Torque", "EPS_Source",
                        "EPS_Confidence", "Forward_PEG", "Composite_Score", "Signal"]
                        if c in buys.columns]
        log.info(f"TOP NEW SETUPS IN THIS BATCH:")
        print(buys[display_cols].to_string(index=False))
    else:
        log.info("No BUY signals found in this batch.")

    db_conn.close()


if __name__ == "__main__":
    main()
