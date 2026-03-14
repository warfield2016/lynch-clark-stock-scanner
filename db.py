"""
SQLite database layer for Lynch-Clark Scanner.
Replaces CSV append-only storage with indexed, queryable persistence.
"""

import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    conn = get_connection()
    conn.execute("""CREATE TABLE IF NOT EXISTS scans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        scan_date TEXT NOT NULL,
        signal TEXT,
        composite_score REAL,
        price REAL,
        market_cap REAL,
        ttm_eps REAL,
        eps_torque REAL,
        eps_source TEXT,
        eps_confidence TEXT,
        analyst_forward_eps REAL,
        sec_extracted_eps REAL,
        divergence_pct REAL,
        eps_growth_rate REAL,
        forward_pe REAL,
        forward_peg REAL,
        fcf_per_share REAL,
        fcf_yield REAL,
        debt_equity REAL,
        inst_ownership_pct REAL,
        revenue_growth REAL,
        roe REAL,
        gross_margin REAL,
        filing_type TEXT,
        is_quarterly INTEGER,
        raw_sentence TEXT,
        signal_override TEXT,
        peg_outlier INTEGER DEFAULT 0,
        price_to_sales REAL,
        price_to_book REAL,
        avg_volume REAL,
        UNIQUE(ticker, scan_date)
    )""")
    # Migrate existing databases that predate these columns
    for col, ctype in [("price_to_sales", "REAL"), ("price_to_book", "REAL"), ("avg_volume", "REAL")]:
        try:
            conn.execute(f"ALTER TABLE scans ADD COLUMN {col} {ctype}")
        except Exception:
            pass  # Column already exists
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON scans(ticker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal ON scans(signal)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON scans(composite_score)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date ON scans(scan_date)")
    conn.commit()
    return conn


def insert_scan(conn, row):
    """Insert a single scan result row (dict)."""
    conn.execute("""INSERT OR REPLACE INTO scans (
        ticker, scan_date, signal, composite_score, price, market_cap,
        ttm_eps, eps_torque, eps_source, eps_confidence,
        analyst_forward_eps, sec_extracted_eps, divergence_pct,
        eps_growth_rate, forward_pe, forward_peg,
        fcf_per_share, fcf_yield, debt_equity, inst_ownership_pct,
        revenue_growth, roe, gross_margin,
        filing_type, is_quarterly, raw_sentence, signal_override, peg_outlier,
        price_to_sales, price_to_book, avg_volume
    ) VALUES (
        :ticker, :scan_date, :signal, :composite_score, :price, :market_cap,
        :ttm_eps, :eps_torque, :eps_source, :eps_confidence,
        :analyst_forward_eps, :sec_extracted_eps, :divergence_pct,
        :eps_growth_rate, :forward_pe, :forward_peg,
        :fcf_per_share, :fcf_yield, :debt_equity, :inst_ownership_pct,
        :revenue_growth, :roe, :gross_margin,
        :filing_type, :is_quarterly, :raw_sentence, :signal_override, :peg_outlier,
        :price_to_sales, :price_to_book, :avg_volume
    )""", {
        "ticker": row.get("Ticker"),
        "scan_date": row.get("Scan_Date"),
        "signal": row.get("Signal"),
        "composite_score": row.get("Composite_Score"),
        "price": row.get("Price"),
        "market_cap": row.get("Market_Cap"),
        "ttm_eps": row.get("TTM_EPS"),
        "eps_torque": row.get("EPS_Torque"),
        "eps_source": row.get("EPS_Source"),
        "eps_confidence": row.get("EPS_Confidence"),
        "analyst_forward_eps": row.get("Analyst_Forward_EPS"),
        "sec_extracted_eps": row.get("SEC_Extracted_EPS"),
        "divergence_pct": row.get("Divergence_Pct"),
        "eps_growth_rate": row.get("EPS_Growth_Rate"),
        "forward_pe": row.get("Forward_PE"),
        "forward_peg": row.get("Forward_PEG"),
        "fcf_per_share": row.get("FCF_Per_Share"),
        "fcf_yield": row.get("FCF_Yield"),
        "debt_equity": row.get("Debt_Equity"),
        "inst_ownership_pct": row.get("Inst_Ownership_Pct"),
        "revenue_growth": row.get("Revenue_Growth"),
        "roe": row.get("ROE"),
        "gross_margin": row.get("Gross_Margin"),
        "filing_type": row.get("Filing_Type"),
        "is_quarterly": 1 if row.get("Is_Quarterly") else 0,
        "raw_sentence": row.get("Raw_Sentence"),
        "signal_override": row.get("Signal_Override"),
        "peg_outlier": 1 if row.get("PEG_Outlier") else 0,
        "price_to_sales": row.get("Price_To_Sales"),
        "price_to_book": row.get("Price_To_Book"),
        "avg_volume": row.get("Avg_Volume"),
    })


def insert_batch(conn, rows):
    """Insert multiple scan result rows."""
    for row in rows:
        insert_scan(conn, row)
    conn.commit()


def get_existing_tickers(conn):
    """Get set of all tickers already in the database."""
    cursor = conn.execute("SELECT DISTINCT ticker FROM scans")
    return {row[0] for row in cursor.fetchall()}


def get_all_scans_df(conn):
    """Return all scans as a pandas DataFrame."""
    return pd.read_sql_query("SELECT * FROM scans ORDER BY composite_score DESC", conn)


def get_buy_signals(conn, limit=50):
    """Return top buy signals sorted by composite score."""
    return pd.read_sql_query(
        "SELECT * FROM scans WHERE signal LIKE '%BUY%' ORDER BY composite_score DESC LIMIT ?",
        conn, params=(limit,)
    )


def get_ticker_history(conn, ticker):
    """Return all scans for a specific ticker, sorted by date."""
    return pd.read_sql_query(
        "SELECT * FROM scans WHERE ticker = ? ORDER BY scan_date",
        conn, params=(ticker,)
    )
