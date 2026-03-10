import streamlit as st
import yfinance as yf
import pandas as pd
import os
import subprocess
from finvizfinance.screener.overview import Overview
from sec_edgar_downloader import Downloader

st.set_page_config(layout="wide", page_title="Stock Scanner")

st.title("Lynch-Clark Stock Scanner")
st.markdown("Filter NASDAQ companies by Volume and Market Cap, and download EDGAR files.")

@st.cache_data(ttl=3600)
def fetch_screener_data(max_mcap_b, max_vol_m, min_pe, max_pe):
    """Fetch NASDAQ stocks and filter by parameters."""
    fOverview = Overview()
    
    # Finviz base filter
    filters = {
        "Exchange": "NASDAQ",
        "Market Cap.": "-Small (under $2bln)", # User asked for < 2B
        "Average Volume": "Any", # Filtered exactly by Pandas later
        "P/E": "Profitable (>0)" # Base filter, we will refine in pandas
    }
    
    try:
        fOverview.set_filter(filters_dict=filters)
        df = fOverview.screener_view()
    except Exception as e:
        st.error(f"Error fetching from Finviz: {e}")
        return pd.DataFrame()
        
    if df.empty:
        return df
        
    # Clean Market Cap - Finviz returns strings like "1.20B", "500.00M"
    def parse_mcap(val):
        if not isinstance(val, str):
            return 0
        val = val.replace(',', '')
        if val.endswith('B'):
            return float(val[:-1]) * 1e9
        elif val.endswith('M'):
            return float(val[:-1]) * 1e6
        else:
            try:
                return float(val)
            except:
                return 0
                
    def parse_vol(val):
        if not isinstance(val, str):
            return 0
        val = val.replace(',', '')
        if val.endswith('M'):
            return float(val[:-1]) * 1e6
        elif val.endswith('K'):
            return float(val[:-1]) * 1e3
        else:
            try:
                return float(val)
            except:
                return 0

    df['Mcap_Raw'] = df['Market Cap'].apply(parse_mcap)
    df['Vol_Raw'] = df['Volume'].apply(parse_vol)
    
    # Clean P/E
    def parse_pe(val):
        if pd.isna(val) or val == '-':
            return 0
        try:
            return float(val)
        except:
            return 0
            
    df['PE_Raw'] = df['P/E'].apply(parse_pe)

    # Filter based on user input
    filtered_df = df[
        (df['Mcap_Raw'] <= max_mcap_b * 1e9) & 
        (df['Vol_Raw'] <= max_vol_m * 1e6) &
        (df['PE_Raw'] >= min_pe) &
        (df['PE_Raw'] <= max_pe)
    ]
    
    # Clean up display
    display_cols = ['Ticker', 'Company', 'Sector', 'Industry', 'Market Cap', 'P/E', 'Price', 'Change', 'Volume']
    return filtered_df[display_cols]

st.subheader("1. Screen for Stocks")
col1, col2, col3, col4 = st.columns(4)
with col1:
    max_mcap = st.number_input("Max Market Cap ($B)", min_value=0.1, max_value=10.0, value=2.0, step=0.1)
with col2:
    max_vol = st.number_input("Max Daily Volume (M)", min_value=0.1, max_value=20.0, value=10.0, step=0.1)
with col3:
    min_pe = st.number_input("Min P/E (TTM)", min_value=0.01, max_value=50.0, value=0.01, step=0.1)
with col4:
    max_pe = st.number_input("Max P/E (TTM)", min_value=0.1, max_value=100.0, value=0.7, step=0.1)

if st.button("Run Scanner"):
    progress_bar = st.progress(0, text="Initializing screener...")
    
    # Simulate progress since Finviz is blocking
    import time
    for p in range(0, 90, 10):
        time.sleep(0.1)
        progress_bar.progress(p, text="Fetching data from Finviz API...")
        
    df = fetch_screener_data(max_mcap, max_vol, min_pe, max_pe)
    
    progress_bar.progress(100, text="Data fetch complete!")
    time.sleep(0.5)
    progress_bar.empty()
    
    st.session_state['filtered_df'] = df
        
if 'filtered_df' in st.session_state and not st.session_state['filtered_df'].empty:
    df = st.session_state['filtered_df']
    st.success(f"Found {len(df)} matching stocks.")
    st.dataframe(df, use_container_width=True)
    
    st.subheader("EDGAR SEC 10-K/10-Q Downloader")
    selected_ticker = st.selectbox("Select a ticker to download filings:", df['Ticker'].tolist())
    
    if st.button(f"Download SEC Filings for {selected_ticker}"):
        with st.spinner(f"Downloading EDGAR filings for {selected_ticker}..."):
            dl = Downloader("MyStockScanner", "user@example.com")
            try:
                dl.get("10-K", selected_ticker, limit=1, download_details=True)
                dl.get("10-Q", selected_ticker, limit=1, download_details=True)
                
                # Convert to PDF via headless Chrome
                base_sec_dir = "sec-edgar-filings"
                pdf_count = 0
                downloaded_pdfs = []
                
                if os.path.exists(base_sec_dir):
                    for root, dirs, files in os.walk(base_sec_dir):
                        if selected_ticker in root:
                            for file in files:
                                if file.endswith("primary-document.html"):
                                    html_path = os.path.join(root, file)
                                    parts = html_path.split(os.sep)
                                    if len(parts) >= 4:
                                        t = parts[1]
                                        filing_type = parts[2]
                                        accession = parts[3]
                                        pdf_name = f"{t}_{filing_type}_{accession}.pdf".replace("-", "_")
                                        pdf_path = os.path.abspath(pdf_name)
                                        
                                        if not os.path.exists(pdf_path):
                                            chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                                            abs_html_path = os.path.abspath(html_path)
                                            cmd = [
                                                chrome_path, "--headless", "--disable-gpu", 
                                                "--no-pdf-header-footer", f"--print-to-pdf={pdf_path}", 
                                                f"file://{abs_html_path}"
                                            ]
                                            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                                            pdf_count += 1
                                            downloaded_pdfs.append(pdf_path)
                                        else:
                                            # Found existing PDF
                                            pdf_count += 1
                                            downloaded_pdfs.append(pdf_path)
                st.success(f"Successfully downloaded and converted {pdf_count} filings to PDF for {selected_ticker}!")
                
                # --- PDF Extraction Logic Skeleton ---
                st.subheader(f"Analyzing {selected_ticker} SEC Filings")
                analyze_progress = st.progress(0, text="Reading PDFs...")
                
                import fitz # PyMuPDF
                import re
                
                for i, pdf in enumerate(downloaded_pdfs):
                    analyze_progress.progress((i+1)/len(downloaded_pdfs), text=f"Scanning {os.path.basename(pdf)} for Forward Guidance...")
                    try:
                        doc = fitz.open(pdf)
                        extracted_text = ""
                        # Only scan first 20 pages to save time (guidance usually upfront)
                        for page_num in range(min(20, len(doc))):
                            extracted_text += doc[page_num].get_text()
                        
                        # Basic Regex to find Forward P/E, PEG, or EPS estimates
                        pe_matches = re.findall(r'(?i)(forward p/e|forward pe|peg ratio|price-to-earnings).{0,30}?(\d+\.\d+|\d+)', extracted_text)
                        eps_matches = re.findall(r'(?i)(guidance|expect|project|estimate).{0,40}?(eps|earnings per share).{0,30}?\$?(\d+\.\d+|\d+)', extracted_text)
                        
                        st.write(f"#### 📄 Analysis for `{os.path.basename(pdf)}`")
                        
                        if pe_matches:
                            st.write("**Extracted Valuation Metrics:**")
                            for m in pe_matches:
                                st.write(f"- {m[0].title()}: **{m[1]}**")
                        else:
                            st.write("- *No explicit Forward P/E or PEG extracted.*")
                            
                        if eps_matches:
                            st.write("**Extracted EPS Guidance:**")
                            for m in eps_matches:
                                st.write(f"- {m[0].title()} {m[1].upper()}: **${m[2]}**")
                        else:
                            st.write("- *No explicit EPS guidance numbers extracted.*")
                            
                    except Exception as e:
                        st.error(f"Error reading PDF {pdf}: {e}")
                
                analyze_progress.progress(100, text="Analysis complete!")
                
            except Exception as e:
                st.error(f"Error downloading filings: {e}")
