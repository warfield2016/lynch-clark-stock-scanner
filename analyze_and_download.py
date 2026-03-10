import yfinance as yf
import pandas as pd
from sec_edgar_downloader import Downloader
import os
import glob
import subprocess
import time

tickers = ["MU", "WDC", "STRL", "KTOS", "CRM", "MNDY", "NVDA", "GE", "HWM", "CRS"]
results = []

print("Fetching data...")
for ticker in tickers:
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        peg = info.get("pegRatio")
        if peg is None:
            peg = info.get("trailingPegRatio")
            
        fpe = info.get("forwardPE")
        growth = info.get("earningsGrowth")
        
        # Manual fallback calculation
        if peg is None and fpe is not None and growth is not None and growth > 0:
            peg = fpe / (growth * 100)
            
        eps_fwd = info.get("forwardEps")
        
        results.append({
            "Ticker": ticker,
            "Forward PEG": round(peg, 3) if peg else None,
            "Forward PE": round(fpe, 2) if fpe else None,
            "EPS Growth": f"{(growth*100):.1f}%" if growth else None,
            "Forward EPS": eps_fwd,
            "Company Name": info.get("shortName", ticker),
        })
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

df = pd.DataFrame(results)
# Sort by PEG, putting NaNs at the bottom
df_sorted = df.sort_values(by="Forward PEG", na_position='last')
print("\n--- Tickers Ranked by Lowest PEG ---")
print(df_sorted.to_string(index=False))

df_sorted.to_csv("peg_rankings.csv", index=False)
print("\nSaved rankings to peg_rankings.csv")

# Get top 3 tickers that actually have data
top_tickers = df_sorted['Ticker'].head(3).tolist()
dl = Downloader("MyStockScanner", "user@example.com")

print("\nDownloading latest SEC filings for top 3 tickers...")
for t in top_tickers:
    print(f"  Downloading for {t}...")
    try:
        dl.get("10-K", t, limit=1, download_details=True)
        dl.get("10-Q", t, limit=1, download_details=True)
    except Exception as e:
        print(f"Error downloading filings for {t}: {e}")

print("\nAttempting to convert SEC HTML filings to PDF...")
base_sec_dir = "sec-edgar-filings"
if os.path.exists(base_sec_dir):
    for root, dirs, files in os.walk(base_sec_dir):
        for file in files:
            if file.endswith("primary-document.html"):
                html_path = os.path.join(root, file)
                parts = html_path.split(os.sep)
                if len(parts) >= 4:
                    ticker = parts[1]
                    filing_type = parts[2]
                    accession = parts[3]
                    pdf_name = f"{ticker}_{filing_type}_{accession}.pdf".replace("-", "_")
                    pdf_path = os.path.join(".", pdf_name)
                    
                    if not os.path.exists(pdf_path):
                        print(f"Creating PDF: {pdf_path}")
                        try:
                            chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                            abs_html_path = os.path.abspath(html_path)
                            abs_pdf_path = os.path.abspath(pdf_path)
                            cmd = [
                                chrome_path,
                                "--headless",
                                "--disable-gpu",
                                "--no-pdf-header-footer",
                                f"--print-to-pdf={abs_pdf_path}",
                                f"file://{abs_html_path}"
                            ]
                            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            print(f"  -> Successfully created {pdf_name}")
                        except Exception as e:
                            print(f"Failed to convert {html_path} to PDF using Chrome: {e}")

print("\nDone!")
