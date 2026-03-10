#!/usr/bin/env python3
"""
Lynch-Clark EPS Torque & Forward PEG Batch Scanner
===================================================
Phases:
  1. Screen NASDAQ via Finviz (100M-2B cap, <8M vol)
  2. Download latest 10-Q/10-K from SEC EDGAR
  3. Extract EPS Torque via NLP regex
  4. Calculate Forward PEG
  5. Classify signals
  6. Save to best_setups_database.csv
"""

import os, re, sys, time, subprocess
import pandas as pd
import yfinance as yf
import fitz  # PyMuPDF
from datetime import datetime
from finvizfinance.screener.overview import Overview
from sec_edgar_downloader import Downloader

# ── Config ──
MAX_TICKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
SEC_DELAY = 1.0  # seconds between SEC downloads
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SEC_DIR = os.path.join(BASE_DIR, "sec-edgar-filings")
OUTPUT_CSV = os.path.join(BASE_DIR, "best_setups_database.csv")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def phase1_screen():
    """Phase 1: Fetch NASDAQ stocks from Finviz, filter by cap/volume."""
    print("\n═══ Phase 1: Screening NASDAQ via Finviz ═══")
    f = Overview()
    f.set_filter(filters_dict={"Exchange": "NASDAQ"})
    df = f.screener_view()
    if df.empty:
        print("No stocks returned from Finviz.")
        return pd.DataFrame()

    df["Mcap"] = pd.to_numeric(df["Market Cap"], errors="coerce").fillna(0)
    df["Vol"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    out = df[(df["Mcap"] >= 1e8) & (df["Mcap"] <= 2e9) & (df["Vol"] < 8e6)]
    out = out.sort_values("Mcap", ascending=False)
    print(f"  ✓ {len(out)} stocks pass filter (showing top {MAX_TICKERS})")
    return out.head(MAX_TICKERS)


def phase2_download(ticker):
    """Phase 2: Download latest 10-Q and 10-K for a single ticker."""
    dl = Downloader("LynchClarkScanner", "scanner@example.com")
    for form in ("10-Q", "10-K"):
        try:
            dl.get(form, ticker, limit=1, download_details=True)
        except Exception as e:
            print(f"    ⚠ {form} download failed for {ticker}: {e}")
    time.sleep(SEC_DELAY)


def phase3_extract(ticker):
    """Phase 3: Convert SEC HTML→PDF→text, run NLP regex for EPS."""
    doc_texts = []
    filing_type = None
    filing_date = None

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
        return None, None, None, None

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

    # Pick best match
    matches = tier1 if tier1 else tier2
    if not matches:
        return None, None, filing_type, filing_date

    best = max(matches, key=lambda x: x[1])
    sentence = best[0]
    eps = best[1]

    # Determine if quarterly or annual
    is_quarterly = True  # conservative default
    annual_kw = ["full year", "fiscal year", "annual", "full-year"]
    for kw in annual_kw:
        if kw in sentence.lower():
            is_quarterly = False
            break

    eps_torque = eps * 4 if is_quarterly else eps
    return eps_torque, sentence, filing_type, is_quarterly


def phase4_calc(ticker, eps_torque):
    """Phase 4: Calculate Forward P/E, Growth Rate, Forward PEG."""
    try:
        info = yf.Ticker(ticker).info
        price = info.get("currentPrice", 0)
        ttm = info.get("trailingEps", 0)
        mcap = info.get("marketCap", 0)
    except Exception:
        return None

    if not price or not eps_torque or eps_torque <= 0:
        return None

    fwd_pe = price / eps_torque

    growth = None
    fwd_peg = None
    if ttm and ttm != 0:
        growth = ((eps_torque - ttm) / abs(ttm)) * 100
        if growth > 0:
            fwd_peg = fwd_pe / growth

    return {
        "Price": round(price, 2),
        "Market_Cap": mcap,
        "TTM_EPS": round(ttm, 2) if ttm else None,
        "EPS_Torque": round(eps_torque, 2),
        "EPS_Growth_Rate": round(growth, 1) if growth else None,
        "Forward_PE": round(fwd_pe, 2),
        "Forward_PEG": round(fwd_peg, 4) if fwd_peg else None,
    }


def phase5_signal(peg):
    """Phase 5: Classify the signal."""
    if peg is None:
        return "NO DATA"
    if peg < 0:
        return "🔴 DECLINING"
    if peg < 0.20:
        return "🔥 EXTREME BUY"
    if peg < 0.50:
        return "✅ STRONG BUY"
    if peg < 0.75:
        return "⚠️ HOLD"
    return "🔴 SELL"


def main():
    start = time.time()
    candidates = phase1_screen()
    if candidates.empty:
        print("No candidates found. Exiting.")
        return

    tickers = candidates["Ticker"].tolist()
    print(f"\n═══ Processing {len(tickers)} tickers ═══")

    rows = []
    for i, t in enumerate(tickers):
        pct = f"[{i+1}/{len(tickers)}]"
        print(f"\n{pct} {t}...")

        # Phase 2
        print(f"  → Downloading SEC filings...")
        phase2_download(t)

        # Phase 3
        print(f"  → Extracting EPS Torque...")
        eps_torque, sentence, ftype, is_q = phase3_extract(t)

        if eps_torque is None:
            print(f"  ✗ No EPS found in SEC docs")
            rows.append({
                "Ticker": t, "Signal": "NO DATA",
                "Raw_Sentence": "No guidance found",
                "Scan_Date": datetime.now().isoformat()
            })
            continue

        print(f"  → EPS Torque: ${eps_torque:.2f} ({'Q×4' if is_q else 'Annual'})")

        # Phase 4
        print(f"  → Calculating Forward PEG...")
        metrics = phase4_calc(t, eps_torque)

        if metrics is None:
            rows.append({
                "Ticker": t, "EPS_Torque": eps_torque,
                "Signal": "CALC ERROR",
                "Raw_Sentence": sentence[:200] if sentence else "",
                "Scan_Date": datetime.now().isoformat()
            })
            continue

        # Phase 5
        sig = phase5_signal(metrics.get("Forward_PEG"))
        print(f"  ✓ Fwd P/E={metrics['Forward_PE']}, "
              f"Growth={metrics.get('EPS_Growth_Rate','?')}%, "
              f"PEG={metrics.get('Forward_PEG','?')} → {sig}")

        rows.append({
            "Ticker": t,
            **metrics,
            "EPS_Source": ftype or "Unknown",
            "Is_Quarterly": is_q,
            "Signal": sig,
            "Raw_Sentence": sentence[:300] if sentence else "",
            "Scan_Date": datetime.now().isoformat()
        })

    # Phase 6: Save
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False)
    elapsed = round(time.time() - start, 1)

    print(f"\n{'═'*60}")
    print(f"═══ SCAN COMPLETE — {elapsed}s ═══")
    print(f"═══ {len(rows)} tickers processed ═══")
    print(f"═══ Saved to: {OUTPUT_CSV} ═══")

    buys = df[df["Signal"].str.contains("BUY", na=False)]
    if not buys.empty:
        print(f"\n🔥 TOP SETUPS:")
        print(buys[["Ticker", "Price", "EPS_Torque",
                     "Forward_PEG", "Signal"]].to_string(index=False))
    else:
        print("\nNo BUY signals found in this batch.")


if __name__ == "__main__":
    main()
