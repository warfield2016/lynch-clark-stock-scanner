import os
import subprocess
import fitz # PyMuPDF
import re
import pandas as pd
import yfinance as yf
from finvizfinance.screener.overview import Overview
from sec_edgar_downloader import Downloader

print("1. Fetching base NASDAQ list from Finviz...")
fOverview = Overview()

# Broad strict criteria filters
filters = {
    "Exchange": "NASDAQ"
    # We will filter exactly by Cap and Volume in Pandas below
}

try:
    fOverview.set_filter(filters_dict=filters)
    df = fOverview.screener_view()
    
    if df.empty:
        print("No stocks found.")
        exit(0)
        
    def parse_mcap(val):
        try:
            return float(val)
        except:
            return 0
            
    def parse_vol(val):
        try:
            return float(val)
        except:
            return 0

    df['Mcap_Raw'] = df['Market Cap'].apply(parse_mcap)
    df['Vol_Raw'] = df['Volume'].apply(parse_vol)
    
    # Exact Pandas filtering
    filtered_df = df[
        (df['Mcap_Raw'] >= 100 * 1e6) & 
        (df['Mcap_Raw'] <= 2.0 * 1e9) & 
        (df['Vol_Raw'] < 8.0 * 1e6)
    ]
    
    # Sort by closest to $2B to get some recognizable names
    filtered_df = filtered_df.sort_values('Mcap_Raw', ascending=False)
    
    # Take a small batch to test parsing (first 5 to not overwhelm EDGAR)
    tickers_to_process = filtered_df['Ticker'].head(5).tolist()
    print(f"\nFound {len(filtered_df)} exact matching stocks. Testing on top 5: {tickers_to_process}")
    
    dl = Downloader("MyStockScanner", "user@example.com")
    results = []
    
    for t in tickers_to_process:
        print(f"\nProcessing {t}...")
        
        # 1. Get Trailing PE / Current Price (Needed to calculate custom Forward PE)
        current_price = 0
        try:
            current_price = yf.Ticker(t).info.get('currentPrice', 0)
        except:
            pass
            
        # 2. Download EDGAR
        try:
            dl.get("10-Q", t, limit=1, download_details=True)
            dl.get("10-K", t, limit=1, download_details=True)
        except Exception as e:
            print(f"Error downloading {t}: {e}")
            
        doc_texts = []
        base_sec_dir = "sec-edgar-filings"
        if os.path.exists(base_sec_dir):
            for root, dirs, files in os.walk(base_sec_dir):
                if t in root:
                    for file in files:
                        if file.endswith("primary-document.html"):
                            html_path = os.path.join(root, file)
                            pdf_path = os.path.abspath(f"{t}_TEMP.pdf")
                            
                            # Convert to PDF Headless
                            chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                            abs_html_path = os.path.abspath(html_path)
                            cmd = [
                                chrome_path, "--headless", "--disable-gpu", 
                                "--no-pdf-header-footer", f"--print-to-pdf={pdf_path}", 
                                f"file://{abs_html_path}"
                            ]
                            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            
                            # Extract Text
                            if os.path.exists(pdf_path):
                                try:
                                    pdf_doc = fitz.open(pdf_path)
                                    text = ""
                                    for page_num in range(min(15, len(pdf_doc))): # First 15 pages usually contain outlook
                                        text += pdf_doc[page_num].get_text()
                                    doc_texts.append(text)
                                    os.remove(pdf_path)
                                except:
                                    pass
        
        # 3. Analyze Text for "Torque EPS" and Guidance
        full_text = " ".join(doc_texts)
        full_text_clean = re.sub(r'\s+', ' ', full_text)
        
        eps_guidance = None
        guidance_sentence = "Not Found"
        
        # Super loose regex: Just look for "earnings per share" followed by any number within 100 characters
        loose_matches = re.finditer(r'(.{0,50}?earnings per share.{0,100}?\$?(\d+\.\d{2}).{0,50}?)', full_text_clean, re.IGNORECASE)
        
        all_numbers = []
        sentences = []
        for m in loose_matches:
            sentences.append(m.group(1).strip())
            try:
                all_numbers.append(float(m.group(2)))
            except:
                pass
                
        if all_numbers:
            # We boldly assume the highest EPS number mentioned in the first 15 pages is the forward annual/quarterly peak
            eps_guidance = max(all_numbers)
            # Find the sentence that contained this max number
            for s in sentences:
                if str(eps_guidance) in s:
                    guidance_sentence = s
                    break

        # Calculate custom Forward metrics if we found guidance
        custom_forward_pe = None
        if eps_guidance and current_price and eps_guidance > 0:
            custom_forward_pe = current_price / eps_guidance
            
        results.append({
            "Ticker": t,
            "Price": f"${current_price}",
            "Max Extracted EPS": f"${eps_guidance}" if eps_guidance else "Not Found",
            "Custom Fwd P/E": round(custom_forward_pe, 2) if custom_forward_pe else "N/A",
            "Found Sentence": guidance_sentence[:150] + "..." if len(guidance_sentence) > 150 else guidance_sentence
        })

    print("\n--- Custom EDGAR Scanner Results ---")
    res_df = pd.DataFrame(results)
    
    # Adjust pandas display options so we can actually read the sentence
    pd.set_option('display.max_colwidth', None)
    print(res_df.to_string(index=False))

except Exception as e:
    print(f"Error: {e}")
