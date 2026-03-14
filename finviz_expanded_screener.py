"""
Lynch-style expanded screener — $1B–$10B NASDAQ, avg vol < 7M
Data sources:
  Primary   — finvizfinance bulk screener (Valuation + Financial + Performance)
  Fallback  — SEC EDGAR XBRL 3yr historical EPS CAGR (when Finviz EPS Next 5Y missing)
  Detail    — finvizfinance quote page (Sales Q/Q, only for pre-filtered stocks)

Filters applied:
  Market cap:     $1B – $10B
  Avg volume:     < 7M shares/day
  EPS leverage:   EPS 5Y CAGR / Sales Q/Q > 2.0
  Forward PEG:    Fwd P/E / EPS 5Y CAGR < 0.70
"""

import time
import csv
import requests
import pandas as pd
from finvizfinance.screener.valuation import Valuation
from finvizfinance.screener.financial import Financial
from finvizfinance.screener.performance import Performance
from finvizfinance.quote import finvizfinance

EDGAR_HEADERS = {'User-Agent': 'research@example.com'}
SLEEP_DETAIL  = 0.3   # seconds between individual quote page fetches


# ── helpers ────────────────────────────────────────────────────────────────

def _pct(v):
    """'21.70%' → 21.70  |  '-' or None → None"""
    if v is None or str(v).strip() in ('-', '', 'nan', 'NaN'):
        return None
    try:
        return float(str(v).replace('%', '').replace(',', '').strip())
    except Exception:
        return None


def _num(v):
    """'7.85' → 7.85  |  NaN / '-' → None"""
    if v is None:
        return None
    try:
        f = float(v)
        return None if pd.isna(f) else f
    except Exception:
        return None


# ── Phase 1: bulk screener ──────────────────────────────────────────────────

def _bulk_screener(cap_filter: str) -> pd.DataFrame:
    """Run Valuation + Financial + Performance screeners for one cap tier."""

    base_filters = {'Market Cap.': cap_filter, 'Exchange': 'NASDAQ'}

    val = Valuation()
    val.set_filter(filters_dict=base_filters)
    df_val = val.screener_view(order='Ticker')
    df_val = df_val[['Ticker', 'Market Cap', 'Fwd P/E', 'EPS Next 5Y', 'Volume']].copy()

    fin = Financial()
    fin.set_filter(filters_dict=base_filters)
    df_fin = fin.screener_view(order='Ticker')
    df_fin = df_fin[['Ticker', 'Debt/Eq', 'ROE']].copy()

    perf = Performance()
    perf.set_filter(filters_dict=base_filters)
    df_perf = perf.screener_view(order='Ticker')
    df_perf = df_perf[['Ticker', 'Avg Volume']].copy()

    df = df_val.merge(df_fin, on='Ticker', how='left') \
               .merge(df_perf, on='Ticker', how='left')
    return df


def get_bulk_universe() -> pd.DataFrame:
    """Fetch NASDAQ mid + large caps and combine into one DataFrame."""
    print("  Fetching mid-cap ($2B–$10B)...")
    df_mid = _bulk_screener('Mid ($2bln to $10bln)')

    print("  Fetching large-cap ($10B–$200B, will trim to $20B)...")
    df_lrg = _bulk_screener('Large ($10bln to $200bln)')

    df = pd.concat([df_mid, df_lrg], ignore_index=True)
    df = df.drop_duplicates(subset='Ticker').reset_index(drop=True)
    return df


# ── Phase 2: bulk pre-filter ────────────────────────────────────────────────

def pre_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Apply market-cap, volume, and rough PEG pre-filter in pandas."""

    # market cap $2B – $20B  (stored in raw dollars)
    df['_mcap'] = pd.to_numeric(df['Market Cap'], errors='coerce')
    df = df[(df['_mcap'] >= 2e9) & (df['_mcap'] <= 20e9)].copy()

    # avg volume < 10M  (Performance view stores as string like '2.28M')
    def parse_vol(v):
        if v is None or str(v).strip() in ('-', '', 'nan'):
            return None
        v = str(v).strip()
        try:
            if v.endswith('M'):
                return float(v[:-1]) * 1e6
            elif v.endswith('K'):
                return float(v[:-1]) * 1e3
            else:
                return float(v.replace(',', ''))
        except Exception:
            return None

    df['_avg_vol'] = df['Avg Volume'].apply(parse_vol)
    df = df[df['_avg_vol'].notna() & (df['_avg_vol'] < 10_000_000)].copy()

    # forward P/E must exist
    df['_fwd_pe'] = pd.to_numeric(df['Fwd P/E'], errors='coerce')
    df = df[df['_fwd_pe'].notna() & (df['_fwd_pe'] > 0)].copy()

    # store EPS 5Y for later use (no pre-filter on PEG)
    df['_eps5y_raw'] = df['EPS Next 5Y'].apply(_pct)
    df['_pre_peg'] = None

    return df.reset_index(drop=True)


# ── Phase 3a: Sales Q/Q from individual quote pages ─────────────────────────

def get_sales_qoq(ticker: str) -> float | None:
    """Fetch Sales Q/Q from Finviz quote page."""
    try:
        stock = finvizfinance(ticker)
        d = stock.ticker_fundament()
        return _pct(d.get('Sales Q/Q'))
    except Exception:
        return None


# ── Phase 3b: SEC EDGAR XBRL fallback for EPS CAGR ─────────────────────────


_cik_map: dict[str, str] | None = None   # loaded once

def _load_cik_map():
    global _cik_map
    if _cik_map is not None:
        return
    try:
        r = requests.get('https://www.sec.gov/files/company_tickers.json',
                         headers=EDGAR_HEADERS, timeout=15)
        data = r.json()
        _cik_map = {v['ticker'].upper(): str(v['cik_str']).zfill(10) for v in data.values()}
    except Exception:
        _cik_map = {}


def edgar_eps_cagr(ticker: str) -> tuple[float | None, str]:
    """
    Compute 3-year EPS CAGR from SEC EDGAR 10-K annual filings.
    Returns (cagr_pct, source_label).
    source_label is 'EDGAR-3yr' when successful, 'EDGAR-err' on failure.
    """
    _load_cik_map()
    cik = (_cik_map or {}).get(ticker.upper())
    if not cik:
        return None, 'EDGAR-no-CIK'
    try:
        url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
        r = requests.get(url, headers=EDGAR_HEADERS, timeout=15)
        facts = r.json()
        eps_items = (facts.get('facts', {})
                         .get('us-gaap', {})
                         .get('EarningsPerShareBasic', {})
                         .get('units', {})
                         .get('USD/shares', []))
        annual = sorted(
            [x for x in eps_items if x.get('form') == '10-K' and x.get('val') is not None],
            key=lambda x: x['end']
        )
        # need at least 4 data points for a 3yr CAGR; take last 4
        annual = annual[-4:]
        if len(annual) < 4:
            return None, 'EDGAR-insufficient-data'
        vals = [a['val'] for a in annual]
        v0, vn = vals[0], vals[-1]
        if v0 <= 0 or vn <= 0:
            return None, 'EDGAR-negative-eps'
        cagr = ((vn / v0) ** (1 / 3) - 1) * 100
        return round(cagr, 1), 'EDGAR-3yr'
    except Exception as e:
        return None, f'EDGAR-err:{e}'


# ── Phase 4: main screener logic ────────────────────────────────────────────

def run_screener():
    print("=" * 65)
    print("LYNCH SCREENER — $2B–$20B NASDAQ, low volume, high leverage")
    print("  Market cap:     $2B – $20B")
    print("  Avg volume:     < 10M shares/day")
    print("  EPS leverage:   EPS 5Y CAGR / Sales Q/Q > 2.0x")
    print("  Forward PEG:    < 0.70")
    print("  EPS source:     Finviz primary · SEC EDGAR fallback")
    print("=" * 65)

    # ── Phase 1 ─────────────────────────────────────────────────────────────
    print("\n[1/4] Bulk screener fetch...")
    df = get_bulk_universe()
    print(f"      Universe: {len(df)} tickers")

    # ── Phase 2 ─────────────────────────────────────────────────────────────
    print("[2/4] Applying bulk pre-filters...")
    df = pre_filter(df)
    finviz_ok    = df['_eps5y_raw'].notna()
    finviz_miss  = df['_eps5y_raw'].isna()
    print(f"      Pre-filtered: {len(df)} stocks "
          f"({finviz_ok.sum()} Finviz EPS, {finviz_miss.sum()} need EDGAR fallback)")

    # ── Phase 3 ─────────────────────────────────────────────────────────────
    print(f"[3/4] Fetching Sales Q/Q + EDGAR fallbacks for {len(df)} stocks...")
    results = []

    for i, row in df.iterrows():
        t       = row['Ticker']
        fwd_pe  = row['_fwd_pe']
        eps5y   = row['_eps5y_raw']   # may be pandas NaN — normalise to None
        if eps5y is not None and (not isinstance(eps5y, (int, float)) or eps5y != eps5y):
            eps5y = None   # catches pandas NaN (nan != nan is True)
        mcap_b  = row['_mcap'] / 1e9
        avg_vol = row['_avg_vol'] / 1e6
        de      = _num(row.get('Debt/Eq'))
        roe     = _num(row.get('ROE'))
        eps_src = 'Finviz'

        # EDGAR fallback
        if eps5y is None:
            eps5y, eps_src = edgar_eps_cagr(t)
            time.sleep(0.15)   # gentle on SEC servers

        if eps5y is None or eps5y <= 0:
            continue

        # Sales Q/Q from individual quote page
        rev_qoq = get_sales_qoq(t)
        time.sleep(SLEEP_DETAIL)

        if rev_qoq is None or rev_qoq <= 0:
            continue

        # leverage and PEG
        leverage = eps5y / rev_qoq
        fwd_peg  = round(fwd_pe / eps5y, 3)

        if leverage < 2.0 or fwd_peg >= 0.70:
            continue

        results.append({
            'ticker':   t,
            'fwd_peg':  fwd_peg,
            'fwd_pe':   round(fwd_pe, 2),
            'eps5y':    round(eps5y, 1),
            'eps_src':  eps_src,
            'rev_qoq':  round(rev_qoq, 1),
            'leverage': round(leverage, 1),
            'avg_vol_M': round(avg_vol, 2),
            'mcap_b':   round(mcap_b, 2),
            'de':       de,
            'roe':      roe,
        })

        done = i + 1
        pct  = done / len(df) * 100
        bar  = '█' * int(pct / 5) + '░' * (20 - int(pct / 5))
        print(f"  [{bar}] {done}/{len(df)} ({pct:.0f}%)  ✓ {len(results)} passing  [{t}]")

    results.sort(key=lambda x: x['fwd_peg'])

    def _print_table(rows, title):
        print(f"\n{'─'*85}")
        print(f"  {title}")
        print(f"{'─'*85}")
        print(
            f"{'TICK':<6} {'FwdPEG':>7} {'FwdPE':>7} {'EPS5Y%':>8} "
            f"{'Src':<10} {'RevQ%':>6} {'Torque':>7} {'Vol(M)':>7} "
            f"{'MCap$B':>7} {'D/E':>5}"
        )
        print("-" * 85)
        for r in rows:
            print(
                f"{r['ticker']:<6} {r['fwd_peg']:>7.3f} {r['fwd_pe']:>7.1f} "
                f"{r['eps5y']:>7.1f}% {r['eps_src']:<10} {r['rev_qoq']:>5.1f}% "
                f"{r['leverage']:>7.1f}x {r['avg_vol_M']:>7.2f} "
                f"{r['mcap_b']:>7.2f} {r['de'] or 0:>5.2f}"
            )

    # ── Phase 4: output ──────────────────────────────────────────────────────
    print(f"\n[4/4] Done. {len(results)} stocks passed all filters.")

    _print_table(results, "RANKED BY LOWEST FORWARD PEG  (best value relative to growth)")

    by_torque = sorted(results, key=lambda x: x['leverage'], reverse=True)
    _print_table(by_torque, "RANKED BY HIGHEST EPS TORQUE  (EPS 5Y CAGR ÷ Revenue Q/Q)")

    if results:
        with open('expanded_screener_results.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"\nSaved → expanded_screener_results.csv")
        missing_src = [r for r in results if r['eps_src'] != 'Finviz']
        if missing_src:
            print(f"\n⚠ {len(missing_src)} stocks used EDGAR fallback (historical, not forward):")
            for r in missing_src:
                print(f"  {r['ticker']} — {r['eps_src']}")

    return results


if __name__ == '__main__':
    run_screener()
