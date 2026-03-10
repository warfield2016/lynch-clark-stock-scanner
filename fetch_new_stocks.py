import os
import subprocess
from finvizfinance.screener.overview import Overview
from sec_edgar_downloader import Downloader

print("Fetching stocks from finviz...")
fOverview = Overview()
# We want NASDAQ
filters = {
    "Exchange": "NASDAQ",
    "Market Cap.": "-Small (under $2bln)", 
    "Average Volume": "Under 1M" 
}

try:
    fOverview.set_filter(filters_dict=filters)
    df = fOverview.screener_view()
    if df.empty:
        print("No stocks found.")
        exit(0)
        
    print(f"Found {len(df)} stocks. Taking top 3...")
    tickers_to_fetch = df['Ticker'].head(3).tolist()
    
    print(f"Tickers: {tickers_to_fetch}")
    
    dl = Downloader("MyStockScanner", "user@example.com")
    
    for t in tickers_to_fetch:
        print(f"Downloading EDGAR filings for {t}...")
        try:
            dl.get("10-K", t, limit=1, download_details=True)
            dl.get("10-Q", t, limit=1, download_details=True)
            
            # Convert to PDF
            base_sec_dir = "sec-edgar-filings"
            if os.path.exists(base_sec_dir):
                for root, dirs, files in os.walk(base_sec_dir):
                    if t in root:
                        for file in files:
                            if file.endswith("primary-document.html"):
                                html_path = os.path.join(root, file)
                                parts = html_path.split(os.sep)
                                if len(parts) >= 4:
                                    tag = parts[1]
                                    filing_type = parts[2]
                                    accession = parts[3]
                                    pdf_name = f"{tag}_{filing_type}_{accession}.pdf".replace("-", "_")
                                    pdf_path = os.path.abspath(pdf_name)
                                    
                                    if not os.path.exists(pdf_path):
                                        chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                                        abs_html_path = os.path.abspath(html_path)
                                        cmd = [
                                            chrome_path, "--headless", "--disable-gpu", 
                                            "--no-pdf-header-footer", f"--print-to-pdf={pdf_path}", 
                                            f"file://{abs_html_path}"
                                        ]
                                        print(f" Converting {pdf_name}...")
                                        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"Error processing {t}: {e}")
            
    print("Done fetching new stocks!")

except Exception as e:
    print(f"Error: {e}")
