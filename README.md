# Lynch-Clark Stock Scanner

A Peter Lynch / Benjamin Graham inspired stock screener that identifies undervalued growth companies using **Forward PEG ratio** and **EPS Torque** (earnings leverage over revenue growth) as primary signals.

---

## What It Does

The screener applies a two-signal filter across NASDAQ-listed stocks in a configurable market cap range:

| Signal | Formula | Threshold |
|--------|---------|-----------|
| **Forward PEG** | Forward P/E ÷ EPS 5Y CAGR | < 0.70 |
| **EPS Torque** | EPS 5Y CAGR ÷ Revenue Q/Q | > 2.0× |

A PEG below 0.70 means you are paying less than 70 cents for every dollar of expected annual growth.
EPS Torque above 2× means earnings are expanding more than twice as fast as sales — operating leverage in action.

**Data sources:**
- **Primary** — [Finviz](https://finviz.com) bulk screener via `finvizfinance` (EPS Next 5Y, Forward P/E, Avg Volume, Debt/Equity)
- **Secondary** — Finviz individual quote pages (Sales Q/Q)
- **Fallback** — [SEC EDGAR XBRL API](https://data.sec.gov) 3-year historical EPS CAGR when Finviz consensus is missing

---

## Screeners

### 1. `finviz_expanded_screener.py` — Main Screener

Lynch-style expanded screener. Scans NASDAQ mid + large caps ($2B–$20B, volume < 10M/day).

```bash
python3 finviz_expanded_screener.py
```

**Output:** Two ranked tables printed to console + `expanded_screener_results.csv`
- Table 1: ranked by lowest Forward PEG
- Table 2: ranked by highest EPS Torque

**Current filters:**
- Exchange: NASDAQ
- Market cap: $2B – $20B
- Avg daily volume: < 10M shares
- Forward P/E: must exist and > 0
- EPS Torque: > 2.0×
- Forward PEG: < 0.70
- EPS source: Finviz forward consensus → SEC EDGAR 3yr CAGR fallback

---

### 2. `batch_scanner.py` — EPS Torque Batch Scanner

Scans batches of NASDAQ tickers and stores results in a local SQLite database (`scanner.db`). Uses SEC EDGAR 10-K/10-Q filings to compute EPS Torque from actual reported earnings.

```bash
# Scan a batch of N tickers (default 100)
python3 batch_scanner.py

# Scan a specific number
python3 batch_scanner.py 50

# Scan a specific cap tier
python3 batch_scanner.py --mode small    # $300M–$2B
python3 batch_scanner.py --mode mid      # $2B–$10B
python3 batch_scanner.py --mode large    # $10B–$200B
```

Results are stored in `scanner.db` (SQLite) — query with any SQLite client or `db.py`.

---

### 3. `streamlit_app.py` — Interactive Dashboard

Web dashboard to explore scan results stored in `scanner.db`.

```bash
streamlit run streamlit_app.py
```

Opens at `http://localhost:8501`. Features:
- Filter by signal, market cap, PEG, volume
- Sort by any column
- Export filtered results to CSV

---

### 4. `fmp_screener.py` — Financial Modeling Prep Screener

Alternative screener using the [FMP API](https://financialmodelingprep.com) for richer fundamental data.

```bash
export FMP_KEY=your_api_key_here
python3 fmp_screener.py
```

Requires a free or paid FMP API key. Output saved to `fmp_results.json`.

---

## Installation

### Requirements
- Python 3.11+
- No paid API keys required for the main screener (`finviz_expanded_screener.py`)

### Setup

```bash
# Clone the repo
git clone https://github.com/warfield2016/lynch-clark-stock-scanner.git
cd lynch-clark-stock-scanner

# Create virtual environment
python3 -m venv venv
source venv/bin/activate        # macOS/Linux
# venv\Scripts\activate         # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## Quick Start

```bash
# Activate virtualenv
source venv/bin/activate

# Run the main Lynch screener (takes 5–10 min, fetches ~400 stock pages)
python3 -u finviz_expanded_screener.py

# Or run the batch scanner (faster, uses local SEC filings)
python3 batch_scanner.py 100

# Launch the dashboard
streamlit run streamlit_app.py
```

---

## Understanding the Output

```
TICK    FwdPEG   FwdPE   EPS5Y% Src         RevQ%  Torque  Vol(M)  MCap$B   D/E
BMRN     0.200    11.0    55.3% Finviz      21.2%     2.6x    2.48   11.25  0.11
GLBE     0.318    23.8    74.8% Finviz      28.1%     2.7x    1.54    5.89  0.03
CORT     0.473    24.7    52.3% Finviz      11.1%     4.7x    2.28    3.42  0.01
```

| Column | Meaning |
|--------|---------|
| `FwdPEG` | Forward P/E ÷ EPS Next 5Y — lower is cheaper relative to growth |
| `FwdPE` | Forward price-to-earnings |
| `EPS5Y%` | Analyst consensus 5-year EPS CAGR |
| `Src` | Data source: `Finviz` (forward) or `EDGAR-3yr` (historical fallback) |
| `RevQ%` | Most recent quarter revenue growth year-over-year |
| `Torque` | EPS leverage: EPS5Y ÷ RevQ — how much faster earnings grow vs sales |
| `Vol(M)` | Average daily volume in millions |
| `MCap$B` | Market cap in billions |
| `D/E` | Debt-to-equity ratio |

---

## Lynch's Criteria Applied

This screener is modeled on Peter Lynch's framework from *One Up on Wall Street*:

- **Low PEG** — Lynch considered PEG < 1.0 a buy, PEG < 0.5 exceptional value
- **High EPS torque** — earnings growing faster than sales = operating leverage = scalable business
- **Low volume** — undiscovered by institutions, Lynch's favorite hunting ground
- **Moderate cap** — $2B–$20B: too large to be ignored, too small for most fund mandates
- **Low debt** — companies that can survive a recession

> *"The person that turns over the most rocks wins."* — Peter Lynch

---

## SEC EDGAR Notice

The EDGAR fallback makes direct requests to `https://data.sec.gov`. Per SEC policy, requests must include a `User-Agent` header with contact information. The placeholder `research@example.com` in the source satisfies this requirement — replace it with your own contact if you plan to run heavy workloads.

---

## Disclaimer

This software is for **educational and research purposes only**. It does not constitute financial advice. Always conduct your own due diligence before making investment decisions.

---

## License

MIT
