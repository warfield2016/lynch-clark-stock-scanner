"""
FMP-powered screener — two filters only:
  1. EPS 3-5yr Forward CAGR / Revenue YoY > 2.0  (EPS leverage)
  2. Forward PEG < 1.0
"""

import sqlite3, requests, time, os, json
from pathlib import Path

FMP_KEY = os.environ.get("FMP_KEY", "")   # set via: export FMP_KEY=your_key_here
BASE    = "https://financialmodelingprep.com/api/v3"
HEADERS = {"User-Agent": "research@example.com"}

def get_fmp(endpoint, params={}):
    params["apikey"] = FMP_KEY
    r = requests.get(f"{BASE}/{endpoint}", params=params, timeout=12)
    r.raise_for_status()
    return r.json()

def get_forward_cagr(ticker):
    """
    Pull analyst EPS estimates for next 3-5 years from FMP.
    Compute CAGR from current year to furthest estimate year.
    Returns (cagr_pct, n_years) or (None, None).
    """
    try:
        data = get_fmp(f"analyst-estimates/{ticker}", {"period": "annual"})
    except Exception:
        return None, None

    if not data or not isinstance(data, list):
        return None, None

    # Filter to future years only, take up to 5
    from datetime import datetime
    current_year = datetime.now().year
    future = sorted(
        [d for d in data if int(d["date"][:4]) >= current_year],
        key=lambda x: x["date"]
    )[:5]

    if len(future) < 2:
        return None, None

    eps_start = future[0].get("estimatedEpsAvg")
    eps_end   = future[-1].get("estimatedEpsAvg")
    n         = int(future[-1]["date"][:4]) - int(future[0]["date"][:4])

    if not eps_start or not eps_end or eps_start <= 0 or eps_end <= 0 or n <= 0:
        return None, None

    cagr = ((eps_end / eps_start) ** (1 / n) - 1) * 100
    return round(cagr, 2), n

def get_forward_pe(ticker):
    """Forward P/E from FMP key metrics."""
    try:
        data = get_fmp(f"key-metrics/{ticker}", {"period": "annual", "limit": 1})
        if data:
            return data[0].get("peRatio")
    except Exception:
        pass
    return None

def run_screener():
    if not FMP_KEY:
        print("ERROR: Set your FMP key first:  export FMP_KEY=your_key_here")
        return

    conn   = sqlite3.connect("scanner.db")
    cursor = conn.execute("""
        SELECT ticker, price, market_cap, eps_torque, ttm_eps, revenue_growth, composite_score
        FROM scans
        WHERE eps_torque > 0.10 AND ttm_eps > 0.10 AND price > 3
          AND revenue_growth > 0
          AND signal NOT LIKE '%TURN%'
          AND signal NOT LIKE '%HIGH DEBT%'
          AND eps_confidence = 'high'
        ORDER BY composite_score DESC
        LIMIT 300
    """)
    rows = cursor.fetchall()
    conn.close()

    print(f"Screening {len(rows)} candidates via FMP...\n")

    results = []
    for i, (ticker, price, mcap, torque, ttm, rev_yoy, score) in enumerate(rows, 1):

        cagr, yrs = get_forward_cagr(ticker)
        time.sleep(0.22)   # ~4 req/sec, well within FMP free tier

        if not cagr or cagr <= 0:
            continue

        if not rev_yoy or rev_yoy <= 0:
            continue

        leverage = cagr / rev_yoy
        if leverage < 2.0:
            continue

        fwd_pe  = price / torque if torque else None
        if not fwd_pe:
            continue

        fwd_peg = fwd_pe / cagr
        if fwd_peg >= 1.0:
            continue

        results.append({
            "ticker":   ticker,
            "price":    round(price, 2),
            "mcap_m":   round((mcap or 0) / 1e6),
            "eps_cagr": cagr,
            "yrs":      yrs,
            "rev_yoy":  round(rev_yoy, 1),
            "leverage": round(leverage, 1),
            "fwd_pe":   round(fwd_pe, 2),
            "fwd_peg":  round(fwd_peg, 3),
        })

        if i % 20 == 0:
            print(f"  {i}/{len(rows)} checked, {len(results)} passed so far...")

    results.sort(key=lambda x: x["fwd_peg"])

    print(f"\n{'#':<3} {'Ticker':<6} {'Price':>7} {'MCap$M':>7} {'EPS CAGR':>9} {'Yrs':>4} {'Rev YoY':>8} {'Leverage':>9} {'FwdPE':>6} {'FwdPEG':>8}")
    print("─" * 88)
    for i, r in enumerate(results, 1):
        print(f"{i:<3} {r['ticker']:<6} ${r['price']:>6.2f} {r['mcap_m']:>7,}   {r['eps_cagr']:>7.1f}%  {r['yrs']:>3}yr  {r['rev_yoy']:>6.1f}%   {r['leverage']:>6.1f}×  {r['fwd_pe']:>5.1f}   {r['fwd_peg']:>7.3f}")

    print(f"\n{len(results)} stocks passed both filters.")

    # Save results
    out = Path("fmp_results.json")
    out.write_text(json.dumps(results, indent=2))
    print(f"Results saved to {out}")

if __name__ == "__main__":
    run_screener()
