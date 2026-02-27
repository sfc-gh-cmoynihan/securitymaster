# ============================================
# SECURITY MASTER STREAMLIT APP
# Portfolio Analysis & Trade Viewer
# OPTIMIZED FOR PERFORMANCE
# ============================================

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Security Master EDM",
    page_icon="❄️",
    layout="wide"
)

# Custom CSS - Clean white theme with teal accents
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@300;400;600;700&display=swap');
    
    :root {
        --primary: #0d9488;
        --primary-light: #14b8a6;
        --accent: #f43f5e;
        --bg-white: #ffffff;
        --bg-light: #f8fafc;
        --bg-card: #ffffff;
        --border: #e2e8f0;
        --text-primary: #0f172a;
        --text-secondary: #475569;
        --text-muted: #94a3b8;
        --gain: #10b981;
        --loss: #ef4444;
    }
    
    .stApp { background-color: var(--bg-white) !important; }
    .main .block-container { padding-top: 0.5rem; max-width: 1400px; background-color: var(--bg-white); }
    h1, h2, h3 { font-family: 'Outfit', sans-serif !important; color: var(--text-primary) !important; }
    .stMarkdown p, .stMarkdown li { font-family: 'Outfit', sans-serif; color: var(--text-secondary); }
    [data-testid="stMetric"] { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 1rem 1.5rem; box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1); }
    [data-testid="stMetricLabel"] { font-family: 'JetBrains Mono', monospace !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.1em; color: var(--primary) !important; }
    [data-testid="stMetricValue"] { font-family: 'Outfit', sans-serif !important; font-weight: 700 !important; font-size: 1.8rem !important; color: var(--text-primary) !important; }
    .stSelectbox > div > div { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; font-family: 'JetBrains Mono', monospace; }
    .stDataFrame { border-radius: 12px; overflow: hidden; border: 1px solid var(--border); }
    .info-card { background: var(--bg-light); border: 1px solid var(--border); border-radius: 16px; padding: 1.5rem; margin-bottom: 1rem; }
    .gain-text { color: var(--gain) !important; font-weight: 600; }
    .loss-text { color: var(--loss) !important; font-weight: 600; }
    .stTabs [data-baseweb="tab-list"] { gap: 4px; background: #e5e7eb; border-radius: 8px; padding: 0.4rem 0.5rem; border: 1px solid var(--border); width: 100%; justify-content: space-between; }
    .stTabs [data-baseweb="tab"] { font-family: 'JetBrains Mono', monospace; font-size: 0.95rem; font-weight: 600; border-radius: 6px; padding: 0.5rem 0.75rem; color: var(--text-secondary); flex: 1; text-align: center; justify-content: center; }
    .stTabs [data-baseweb="tab"]:nth-child(even) { background: #dbeafe !important; }
    .stTabs [aria-selected="true"] { background: linear-gradient(135deg, var(--primary), var(--primary-light)) !important; color: white !important; }
    section[data-testid="stSidebar"] { background-color: var(--bg-light) !important; }
    div[data-testid="stToolbar"] { background-color: var(--bg-white) !important; }
</style>
""", unsafe_allow_html=True)

# Get Snowflake session
session = get_active_session()

# Header
st.markdown('''
<div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.5rem;">
    <div style="width: 40px;"></div>
    <span style="font-family: 'Outfit', sans-serif; font-size: 1.75rem; font-weight: 700; 
                 background: linear-gradient(135deg, #29b5e8 0%, #0d9488 100%); 
                 -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
        Security Master EDM
    </span>
    <img src="https://upload.wikimedia.org/wikipedia/commons/f/ff/Snowflake_Logo.svg" 
         style="height: 40px;" alt="Snowflake">
</div>
''', unsafe_allow_html=True)

# ============================================
# OPTIMIZED DATA LOADING FUNCTIONS
# Increased TTL, added LIMIT clauses
# ============================================

@st.cache_data(ttl=900)
def load_securities():
    return session.sql("""
        SELECT SYMBOL, SECURITY_NAME, GICS_SECTOR, GICS_SUB_INDUSTRY, HEADQUARTERS
        FROM SECURITY_MASTER_DB.SECURITIES.SP500 
        ORDER BY SYMBOL
    """).to_pandas()

@st.cache_data(ttl=300)
def get_portfolio_summary_fast():
    return session.sql("""
        SELECT SYMBOL, COUNT(*) as TRADE_COUNT,
            SUM(CASE WHEN SIDE = 'BUY' THEN QUANTITY ELSE 0 END) as TOTAL_BOUGHT,
            SUM(CASE WHEN SIDE = 'SELL' THEN QUANTITY ELSE 0 END) as TOTAL_SOLD,
            SUM(CASE WHEN SIDE = 'BUY' THEN TOTAL_VALUE ELSE 0 END) as BUY_VALUE,
            SUM(CASE WHEN SIDE = 'SELL' THEN TOTAL_VALUE ELSE 0 END) as SELL_VALUE,
            SUM(CASE WHEN SIDE = 'BUY' THEN QUANTITY ELSE 0 END) - SUM(CASE WHEN SIDE = 'SELL' THEN QUANTITY ELSE 0 END) as NET_POSITION,
            AVG(PRICE) as AVG_PRICE
        FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES
        GROUP BY SYMBOL
        ORDER BY ABS(BUY_VALUE - SELL_VALUE) DESC
        LIMIT 100
    """).to_pandas()

@st.cache_data(ttl=300)
def get_sector_breakdown_fast():
    return session.sql("""
        SELECT s.GICS_SECTOR, COUNT(DISTINCT t.SYMBOL) as SECURITIES_TRADED,
            SUM(t.TOTAL_VALUE) as TOTAL_VALUE,
            SUM(CASE WHEN t.SIDE = 'BUY' THEN t.TOTAL_VALUE ELSE 0 END) as BUY_VALUE,
            SUM(CASE WHEN t.SIDE = 'SELL' THEN t.TOTAL_VALUE ELSE 0 END) as SELL_VALUE
        FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
        JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
        GROUP BY s.GICS_SECTOR
        ORDER BY TOTAL_VALUE DESC
    """).to_pandas()

@st.cache_data(ttl=300)
def get_quick_metrics():
    return session.sql("""
        SELECT COUNT(*) as TOTAL_TRADES, COUNT(DISTINCT SYMBOL) as UNIQUE_SYMBOLS
        FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES
    """).to_pandas()

@st.cache_data(ttl=900)
def get_tradeable_securities():
    return session.sql("""
        SELECT DISTINCT SYMBOL, SECURITY_NAME, GICS_SECTOR
        FROM SECURITY_MASTER_DB.SECURITIES.SP500
        ORDER BY SYMBOL
    """).to_pandas()

@st.cache_data(ttl=120)
def get_live_stock_price(symbol):
    import json
    try:
        result = session.sql(f"""
            SELECT SECURITY_MASTER_DB.TRADES.GET_STOCK_PRICE('{symbol}') as PRICE_DATA
        """).to_pandas()
        if not result.empty:
            price_data = result.iloc[0]['PRICE_DATA']
            if isinstance(price_data, str):
                return json.loads(price_data)
            return price_data
    except Exception as e:
        return {"error": str(e)}
    return None

@st.cache_data(ttl=900)
def get_tradeable_bonds():
    return session.sql("""
        SELECT DISTINCT CUSIP, BOND_ID, ISSUER_NAME, TICKER, COUPON_RATE, CURRENT_YIELD, CREDIT_RATING, MATURITY_DATE, PAR_VALUE
        FROM SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS
        WHERE MATURITY_DATE > CURRENT_DATE()
        ORDER BY ISSUER_NAME
    """).to_pandas()

@st.cache_data(ttl=600)
def get_bond_holdings():
    return session.sql("""
        SELECT DISTINCT b.CUSIP, b.ISSUER_NAME, b.COUPON_RATE, b.CURRENT_YIELD, b.CREDIT_RATING,
            SUM(CASE WHEN t.SIDE = 'BUY' THEN t.FACE_VALUE ELSE -t.FACE_VALUE END) as NET_POSITION
        FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES t
        JOIN SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS b ON t.CUSIP = b.CUSIP
        GROUP BY b.CUSIP, b.ISSUER_NAME, b.COUPON_RATE, b.CURRENT_YIELD, b.CREDIT_RATING
        HAVING NET_POSITION > 0
        ORDER BY b.ISSUER_NAME
    """).to_pandas()

# Load minimal data for initial render
securities = load_securities()
quick_metrics = get_quick_metrics()

# Top metrics - use cached quick metrics
col1, col2, col3, col4, col5 = st.columns(5)
total_trades = quick_metrics['TOTAL_TRADES'].iloc[0] if not quick_metrics.empty else 0
total_aum = 100_000_000_000
cash_balance = 50_000_000
us_treasury_bonds = 10_000_000_000
net_pnl = 2_450_000_000

with col1:
    st.metric("Total AUM", f"${total_aum/1e9:.0f}B")
with col2:
    st.metric("Cash Balance", f"${cash_balance/1e6:.0f}M")
with col3:
    st.metric("US Treasury (4.25%)", f"${us_treasury_bonds/1e9:.0f}B")
with col4:
    st.metric("Total Trades", f"{int(total_trades):,}")
with col5:
    st.metric("Realized P&L", f"🟢 ${net_pnl:,.0f}")

# Tabs
tab1, tab2, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs(["📊 Portfolio", "🔍 Trade History", "🔗 Equity Trades", "📉 Bond Trades", "✏️ Master Data", "📜 Master History", "📋 Settlement Details", "📝 Stock / ETF Order", "🏦 Bond Order"])
st.markdown('<hr style="border: none; height: 4px; background: linear-gradient(90deg, #29b5e8, #0d9488); margin: 0.5rem 0 1rem 0;">', unsafe_allow_html=True)

# ============================================
# TAB 1: PORTFOLIO (Lazy loaded)
# ============================================
with tab1:
    st.markdown("""<div style="background: linear-gradient(135deg, #93c5fd 0%, #60a5fa 50%, #3b82f6 100%); border-radius: 10px; padding: 0.5rem 1rem; margin-bottom: 1rem;">
        <h4 style="color: white; margin: 0; font-weight: 600;">📊 Portfolio Summary by Security</h4></div>""", unsafe_allow_html=True)
    
    portfolio_summary = get_portfolio_summary_fast()
    portfolio_with_pnl = portfolio_summary.copy()
    portfolio_with_pnl['REALIZED_PNL'] = portfolio_with_pnl['SELL_VALUE'] - portfolio_with_pnl['BUY_VALUE']
    
    st.subheader("📈 Top 10 Equity Performers")
    portfolio_with_names = portfolio_with_pnl.merge(securities[['SYMBOL', 'SECURITY_NAME']], on='SYMBOL', how='left')
    
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.markdown("🟢 **Top 10 Gainers**")
        gainers_chart = portfolio_with_names.nlargest(10, 'REALIZED_PNL')[['SECURITY_NAME', 'REALIZED_PNL']].copy()
        gainers_chart = gainers_chart.sort_values('REALIZED_PNL', ascending=True).set_index('SECURITY_NAME')
        st.bar_chart(gainers_chart, use_container_width=True, height=300)
    
    with chart_col2:
        st.markdown("🔴 **Top 10 Losers**")
        losers_chart = portfolio_with_names.nsmallest(10, 'REALIZED_PNL')[['SECURITY_NAME', 'REALIZED_PNL']].copy()
        losers_chart['REALIZED_PNL'] = losers_chart['REALIZED_PNL'].abs()
        losers_chart = losers_chart.sort_values('REALIZED_PNL', ascending=True).set_index('SECURITY_NAME')
        st.bar_chart(losers_chart, use_container_width=True, height=300)
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 🟢 Top Gainers")
        gainers = portfolio_with_pnl.nlargest(10, 'REALIZED_PNL')[['SYMBOL', 'TRADE_COUNT', 'REALIZED_PNL']].copy()
        gainers['REALIZED_PNL'] = gainers['REALIZED_PNL'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(gainers.rename(columns={'SYMBOL': 'Symbol', 'TRADE_COUNT': 'Trades', 'REALIZED_PNL': 'P&L'}), use_container_width=True)
    
    with col2:
        st.markdown("#### 🔴 Top Losers")
        losers = portfolio_with_pnl.nsmallest(10, 'REALIZED_PNL')[['SYMBOL', 'TRADE_COUNT', 'REALIZED_PNL']].copy()
        losers['REALIZED_PNL'] = losers['REALIZED_PNL'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(losers.rename(columns={'SYMBOL': 'Symbol', 'TRADE_COUNT': 'Trades', 'REALIZED_PNL': 'P&L'}), use_container_width=True)
    
    st.markdown("#### 📈 Full Equity Portfolio")
    display_portfolio = portfolio_summary.copy()
    display_portfolio['BUY_VALUE'] = display_portfolio['BUY_VALUE'].apply(lambda x: f"${x:,.0f}")
    display_portfolio['SELL_VALUE'] = display_portfolio['SELL_VALUE'].apply(lambda x: f"${x:,.0f}")
    display_portfolio['AVG_PRICE'] = display_portfolio['AVG_PRICE'].apply(lambda x: f"${x:.2f}")
    st.dataframe(display_portfolio.rename(columns={'SYMBOL': 'Symbol', 'TRADE_COUNT': 'Trades', 'TOTAL_BOUGHT': 'Bought', 'TOTAL_SOLD': 'Sold', 'BUY_VALUE': 'Buy Value', 'SELL_VALUE': 'Sell Value', 'NET_POSITION': 'Net Pos', 'AVG_PRICE': 'Avg Price'}), use_container_width=True, height=300)
    
    st.markdown("---")
    st.subheader("📈 Sector Analysis")
    
    sector_data = get_sector_breakdown_fast()
    
    if not sector_data.empty:
        sector_display = sector_data.copy()
        sector_display['REALIZED_PNL'] = sector_display['SELL_VALUE'] - sector_display['BUY_VALUE']
        
        st.bar_chart(sector_display.set_index('GICS_SECTOR')['TOTAL_VALUE'], use_container_width=True)
        
        st.markdown("#### 📊 Equity Sector Breakdown")
        sector_display['TOTAL_VALUE'] = sector_display['TOTAL_VALUE'].apply(lambda x: f"${x:,.0f}")
        sector_display['BUY_VALUE'] = sector_display['BUY_VALUE'].apply(lambda x: f"${x:,.0f}")
        sector_display['SELL_VALUE'] = sector_display['SELL_VALUE'].apply(lambda x: f"${x:,.0f}")
        sector_display['REALIZED_PNL'] = sector_display['REALIZED_PNL'].apply(lambda x: f"${x:,.0f}")
        st.dataframe(sector_display.rename(columns={'GICS_SECTOR': 'Sector', 'SECURITIES_TRADED': 'Securities', 'TOTAL_VALUE': 'Total Value', 'BUY_VALUE': 'Buy Value', 'SELL_VALUE': 'Sell Value', 'REALIZED_PNL': 'Realized P&L'}), use_container_width=True)
    
    st.markdown("---")
    st.markdown("#### 🏦 Fixed Income Sector Analysis")
    
    @st.cache_data(ttl=600)
    def get_bond_sector_breakdown_portfolio():
        return session.sql("""
            SELECT b.SECTOR, COUNT(*) as BOND_COUNT, SUM(t.TOTAL_VALUE) as TOTAL_VALUE,
                AVG(t.YIELD) as AVG_YIELD, AVG(t.PRICE) as AVG_PRICE
            FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES t
            JOIN SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS b ON t.CUSIP = b.CUSIP
            GROUP BY b.SECTOR
            ORDER BY TOTAL_VALUE DESC
        """).to_pandas()
    
    bond_sector_pf = get_bond_sector_breakdown_portfolio()
    if not bond_sector_pf.empty:
        st.bar_chart(bond_sector_pf.set_index('SECTOR')['TOTAL_VALUE'], use_container_width=True)
        bond_sector_pf['TOTAL_VALUE'] = bond_sector_pf['TOTAL_VALUE'].apply(lambda x: f"${x:,.0f}")
        bond_sector_pf['AVG_YIELD'] = bond_sector_pf['AVG_YIELD'].apply(lambda x: f"{x:.2f}%")
        bond_sector_pf['AVG_PRICE'] = bond_sector_pf['AVG_PRICE'].apply(lambda x: f"{x:.4f}")
        st.dataframe(bond_sector_pf.rename(columns={'SECTOR': 'Sector', 'BOND_COUNT': 'Bonds', 'TOTAL_VALUE': 'Total Value', 'AVG_YIELD': 'Avg Yield', 'AVG_PRICE': 'Avg Price'}), use_container_width=True)
    
    st.markdown("---")
    st.subheader("💵 Bond Portfolio - Yield Analysis")
    
    @st.cache_data(ttl=600)
    def get_active_bonds_by_yield():
        return session.sql("""
            SELECT BOND_ID, TICKER, ISSUER_NAME, COUPON_RATE, CURRENT_YIELD, MATURITY_DATE, CREDIT_RATING, PAR_VALUE, CURRENCY
            FROM SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS
            WHERE MATURITY_DATE > CURRENT_DATE()
            ORDER BY CURRENT_YIELD DESC
            LIMIT 50
        """).to_pandas()
    
    active_bonds = get_active_bonds_by_yield()
    bond_col1, bond_col2 = st.columns(2)
    
    with bond_col1:
        st.markdown("📈 **Top 10 Highest Yielding Bonds**")
        top_yield_chart = active_bonds.head(10)[['ISSUER_NAME', 'CURRENT_YIELD']].copy()
        top_yield_chart = top_yield_chart.sort_values('CURRENT_YIELD', ascending=True).set_index('ISSUER_NAME')
        st.bar_chart(top_yield_chart['CURRENT_YIELD'], use_container_width=True, height=250)
        
        top_yield = active_bonds.head(10)[['TICKER', 'ISSUER_NAME', 'CURRENT_YIELD', 'MATURITY_DATE', 'CREDIT_RATING']].copy()
        top_yield['CURRENT_YIELD'] = top_yield['CURRENT_YIELD'].apply(lambda x: f"{x:.2f}%")
        top_yield['MATURITY_DATE'] = top_yield['MATURITY_DATE'].astype(str).str[:10]
        st.dataframe(top_yield.rename(columns={'TICKER': 'Ticker', 'ISSUER_NAME': 'Issuer', 'CURRENT_YIELD': 'Yield', 'MATURITY_DATE': 'Maturity', 'CREDIT_RATING': 'Rating'}), use_container_width=True, height=250)
    
    with bond_col2:
        st.markdown("📉 **Bottom 10 Lowest Yielding Bonds**")
        bottom_yield_chart = active_bonds.tail(10)[['ISSUER_NAME', 'CURRENT_YIELD']].copy()
        bottom_yield_chart = bottom_yield_chart.sort_values('CURRENT_YIELD', ascending=True).set_index('ISSUER_NAME')
        st.bar_chart(bottom_yield_chart['CURRENT_YIELD'], use_container_width=True, height=250)
        
        bottom_yield = active_bonds.tail(10)[['TICKER', 'ISSUER_NAME', 'CURRENT_YIELD', 'MATURITY_DATE', 'CREDIT_RATING']].copy()
        bottom_yield['CURRENT_YIELD'] = bottom_yield['CURRENT_YIELD'].apply(lambda x: f"{x:.2f}%")
        bottom_yield['MATURITY_DATE'] = bottom_yield['MATURITY_DATE'].astype(str).str[:10]
        st.dataframe(bottom_yield.rename(columns={'TICKER': 'Ticker', 'ISSUER_NAME': 'Issuer', 'CURRENT_YIELD': 'Yield', 'MATURITY_DATE': 'Maturity', 'CREDIT_RATING': 'Rating'}), use_container_width=True, height=250)

# ============================================
# TAB 2: TRADE HISTORY (Lazy loaded)
# ============================================
with tab2:
    @st.cache_data(ttl=300)
    def load_trades_fast(symbol=None):
        if symbol and symbol != "All Securities":
            return session.sql(f"""
                SELECT t.TRADE_ID, t.SYMBOL, s.SECURITY_NAME, t.TRADE_DATE, t.SIDE, t.QUANTITY, t.PRICE, t.TOTAL_VALUE
                FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
                JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
                WHERE t.SYMBOL = '{symbol}'
                ORDER BY t.TRADE_DATE DESC
                LIMIT 500
            """).to_pandas()
        return session.sql("""
            SELECT t.TRADE_ID, t.SYMBOL, s.SECURITY_NAME, t.TRADE_DATE, t.SIDE, t.QUANTITY, t.PRICE, t.TOTAL_VALUE
            FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
            JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
            ORDER BY t.TRADE_DATE DESC
            LIMIT 500
        """).to_pandas()
    
    col1, col2 = st.columns([1, 3])
    with col1:
        symbol_options = ["All Securities"] + [f"{row['SYMBOL']} - {row['SECURITY_NAME']}" for _, row in securities.head(100).iterrows()]
        selected_display = st.selectbox("Select Security", options=symbol_options, index=0)
        selected_symbol = "All Securities" if selected_display == "All Securities" else selected_display.split(" - ")[0]
    
    if selected_symbol != "All Securities":
        security_info = securities[securities['SYMBOL'] == selected_symbol]
        if not security_info.empty:
            info = security_info.iloc[0]
            with col2:
                st.markdown(f"""<div class="info-card"><h3 style="margin:0; color: #0d9488;">{info['SECURITY_NAME']}</h3>
                    <p style="margin: 0.5rem 0 0 0;">{info['GICS_SECTOR']} • {info['GICS_SUB_INDUSTRY']}<br/>📍 {info['HEADQUARTERS']}</p></div>""", unsafe_allow_html=True)
    
    st.subheader("📋 Trade History")
    trades = load_trades_fast(selected_symbol)
    
    if not trades.empty:
        display_df = trades.copy()
        display_df['TOTAL_VALUE'] = display_df['TOTAL_VALUE'].apply(lambda x: f"${x:,.2f}")
        display_df['PRICE'] = display_df['PRICE'].apply(lambda x: f"${x:.2f}")
        display_df['QUANTITY'] = display_df['QUANTITY'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(display_df.rename(columns={'TRADE_ID': 'ID', 'SYMBOL': 'Symbol', 'SECURITY_NAME': 'Security', 'TRADE_DATE': 'Date', 'SIDE': 'Type', 'QUANTITY': 'Qty', 'PRICE': 'Price', 'TOTAL_VALUE': 'Total Value'}), use_container_width=True)
    else:
        st.info("No trades found.")

# ============================================
# TAB 4: EQUITY TRADES (Lazy loaded)
# ============================================
with tab4:
    st.subheader("🔗 Trade Matching to NYSE Security Master")
    
    @st.cache_data(ttl=300)
    def get_trade_match_summary_fast():
        return session.sql("""
            SELECT CASE WHEN n.SYMBOL IS NOT NULL THEN 'Matched' ELSE 'Unmatched' END as MATCH_STATUS,
                COUNT(*) as TRADE_COUNT, COUNT(DISTINCT t.SYMBOL) as UNIQUE_SYMBOLS, SUM(t.TOTAL_VALUE) as TOTAL_VALUE
            FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
            LEFT JOIN SECURITY_MASTER_DB.EQUITY.NYSE_SECURITIES n ON t.SYMBOL = n.SYMBOL
            GROUP BY CASE WHEN n.SYMBOL IS NOT NULL THEN 'Matched' ELSE 'Unmatched' END
        """).to_pandas()
    
    @st.cache_data(ttl=300)
    def get_trades_with_nyse_fast():
        return session.sql("""
            SELECT t.TRADE_ID, t.SYMBOL, n.SECURITY_NAME as NYSE_COMPANY_NAME, n.ISIN as FIGI,
                t.TRADE_DATE, t.SIDE, t.QUANTITY, t.PRICE, t.TOTAL_VALUE,
                CASE WHEN n.SYMBOL IS NOT NULL THEN 'Matched' ELSE 'Unmatched' END as MATCH_STATUS
            FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
            LEFT JOIN SECURITY_MASTER_DB.EQUITY.NYSE_SECURITIES n ON t.SYMBOL = n.SYMBOL
            ORDER BY t.TRADE_DATE DESC
            LIMIT 500
        """).to_pandas()
    
    match_summary = get_trade_match_summary_fast()
    trades_with_nyse = get_trades_with_nyse_fast()
    
    mcol1, mcol2, mcol3, mcol4 = st.columns(4)
    matched_row = match_summary[match_summary['MATCH_STATUS'] == 'Matched']
    unmatched_row = match_summary[match_summary['MATCH_STATUS'] == 'Unmatched']
    
    matched_trades = int(matched_row['TRADE_COUNT'].values[0]) if not matched_row.empty else 0
    matched_symbols = int(matched_row['UNIQUE_SYMBOLS'].values[0]) if not matched_row.empty else 0
    unmatched_trades = int(unmatched_row['TRADE_COUNT'].values[0]) if not unmatched_row.empty else 0
    unmatched_symbols = int(unmatched_row['UNIQUE_SYMBOLS'].values[0]) if not unmatched_row.empty else 0
    total_trades_match = matched_trades + unmatched_trades
    match_rate = (matched_trades / total_trades_match * 100) if total_trades_match > 0 else 0
    
    with mcol1:
        st.metric("Matched Trades", f"{matched_trades:,}", f"{matched_symbols} symbols")
    with mcol2:
        st.metric("Unmatched Trades", f"{unmatched_trades:,}", f"{unmatched_symbols} symbols")
    with mcol3:
        st.metric("Match Rate", f"{match_rate:.1f}%")
    with mcol4:
        matched_value = float(matched_row['TOTAL_VALUE'].values[0]) if not matched_row.empty else 0
        st.metric("Matched Value", f"${matched_value:,.0f}")
    
    st.markdown("---")
    match_filter = st.selectbox("Filter by Match Status", options=["All", "Matched", "Unmatched"], index=0)
    
    filtered_trades = trades_with_nyse if match_filter == "All" else trades_with_nyse[trades_with_nyse['MATCH_STATUS'] == match_filter]
    st.markdown(f"*Showing {len(filtered_trades):,} trades*")
    
    if not filtered_trades.empty:
        display_matched = filtered_trades.copy()
        display_matched['TOTAL_VALUE'] = display_matched['TOTAL_VALUE'].apply(lambda x: f"${x:,.2f}")
        display_matched['PRICE'] = display_matched['PRICE'].apply(lambda x: f"${x:.2f}")
        display_matched['QUANTITY'] = display_matched['QUANTITY'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(display_matched.rename(columns={'TRADE_ID': 'ID', 'SYMBOL': 'Symbol', 'NYSE_COMPANY_NAME': 'NYSE Company', 'FIGI': 'Bloomberg FIGI', 'TRADE_DATE': 'Date', 'SIDE': 'Type', 'QUANTITY': 'Qty', 'PRICE': 'Price', 'TOTAL_VALUE': 'Total Value', 'MATCH_STATUS': 'Status'}), use_container_width=True, height=400)

# ============================================
# TAB 5: BOND TRADES (Lazy loaded)
# ============================================
with tab5:
    header_col1, header_col2 = st.columns([4, 1])
    with header_col1:
        st.subheader("📉 Bond Trading Activity")
    with header_col2:
        if st.button("🔄 Refresh", key="refresh_bond_trades", use_container_width=True):
            st.cache_data.clear()
            st.experimental_rerun()
    
    @st.cache_data(ttl=300)
    def load_bond_trades_fast():
        return session.sql("""
            SELECT t.TRADE_ID, t.CUSIP, b.ISSUER_NAME, b.TICKER, b.CREDIT_RATING, t.TRADE_DATE, t.SIDE,
                t.FACE_VALUE as QUANTITY, t.PRICE, t.YIELD as YIELD_AT_TRADE, t.TOTAL_VALUE, t.COUNTERPARTY, b.SECTOR
            FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES t
            JOIN SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS b ON t.CUSIP = b.CUSIP
            ORDER BY t.TRADE_DATE DESC
            LIMIT 500
        """).to_pandas()
    
    @st.cache_data(ttl=300)
    def get_bond_summary_fast():
        return session.sql("""
            SELECT b.ISSUER_NAME, b.TICKER, COUNT(*) as TRADE_COUNT,
                SUM(CASE WHEN t.SIDE = 'BUY' THEN t.TOTAL_VALUE ELSE 0 END) as BUY_VALUE,
                SUM(CASE WHEN t.SIDE = 'SELL' THEN t.TOTAL_VALUE ELSE 0 END) as SELL_VALUE
            FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES t
            JOIN SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS b ON t.CUSIP = b.CUSIP
            GROUP BY b.ISSUER_NAME, b.TICKER
            ORDER BY SUM(t.TOTAL_VALUE) DESC
            LIMIT 20
        """).to_pandas()
    
    @st.cache_data(ttl=300)
    def get_counterparty_fast():
        return session.sql("""
            SELECT COUNTERPARTY, COUNT(*) as TRADE_COUNT, SUM(TOTAL_VALUE) as TOTAL_VALUE, AVG(PRICE) as AVG_PRICE
            FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES
            GROUP BY COUNTERPARTY
            ORDER BY TOTAL_VALUE DESC
        """).to_pandas()
    
    bond_trades = load_bond_trades_fast()
    
    btcol1, btcol2, btcol3, btcol4 = st.columns(4)
    total_bond_trades = len(bond_trades)
    total_bond_value = bond_trades['TOTAL_VALUE'].sum() if not bond_trades.empty else 0
    buy_trades = bond_trades[bond_trades['SIDE'] == 'BUY'] if not bond_trades.empty else pd.DataFrame()
    sell_trades = bond_trades[bond_trades['SIDE'] == 'SELL'] if not bond_trades.empty else pd.DataFrame()
    
    with btcol1:
        st.metric("Total Bond Trades", f"{total_bond_trades:,}")
    with btcol2:
        st.metric("Total Trade Value", f"${total_bond_value/1e9:.2f}B")
    with btcol3:
        st.metric("Buy Trades", f"{len(buy_trades):,}")
    with btcol4:
        st.metric("Sell Trades", f"{len(sell_trades):,}")
    
    st.markdown("---")
    
    if not bond_trades.empty:
        display_bt = bond_trades[['TRADE_ID', 'CUSIP', 'TICKER', 'ISSUER_NAME', 'CREDIT_RATING', 'TRADE_DATE', 'SIDE', 'QUANTITY', 'PRICE', 'YIELD_AT_TRADE', 'TOTAL_VALUE', 'COUNTERPARTY']].head(200).copy()
        display_bt['TOTAL_VALUE'] = display_bt['TOTAL_VALUE'].apply(lambda x: f"${x:,.0f}")
        display_bt['PRICE'] = display_bt['PRICE'].apply(lambda x: f"{x:.4f}")
        display_bt['YIELD_AT_TRADE'] = display_bt['YIELD_AT_TRADE'].apply(lambda x: f"{x:.3f}%" if pd.notna(x) else "N/A")
        display_bt['QUANTITY'] = display_bt['QUANTITY'].apply(lambda x: f"{x:,}")
        st.dataframe(display_bt.rename(columns={'TRADE_ID': 'ID', 'CUSIP': 'CUSIP', 'TICKER': 'Ticker', 'ISSUER_NAME': 'Issuer', 'CREDIT_RATING': 'Rating', 'TRADE_DATE': 'Date', 'SIDE': 'Type', 'QUANTITY': 'Qty', 'PRICE': 'Price', 'YIELD_AT_TRADE': 'Yield', 'TOTAL_VALUE': 'Value', 'COUNTERPARTY': 'Counterparty'}), use_container_width=True, height=350)
    
    st.markdown("---")
    summary_col1, summary_col2 = st.columns(2)
    
    with summary_col1:
        st.markdown("#### 🏢 Top Issuers")
        top_issuers = get_bond_summary_fast()
        if not top_issuers.empty:
            top_issuers['BUY_VALUE'] = top_issuers['BUY_VALUE'].apply(lambda x: f"${x/1e6:.1f}M")
            top_issuers['SELL_VALUE'] = top_issuers['SELL_VALUE'].apply(lambda x: f"${x/1e6:.1f}M")
            st.dataframe(top_issuers.rename(columns={'TICKER': 'Ticker', 'ISSUER_NAME': 'Issuer', 'TRADE_COUNT': 'Trades', 'BUY_VALUE': 'Buys', 'SELL_VALUE': 'Sells'}), use_container_width=True, height=300)
    
    with summary_col2:
        st.markdown("#### 🤝 By Counterparty")
        cp_data = get_counterparty_fast()
        if not cp_data.empty:
            cp_data['TOTAL_VALUE'] = cp_data['TOTAL_VALUE'].apply(lambda x: f"${x/1e6:.1f}M")
            cp_data['AVG_PRICE'] = cp_data['AVG_PRICE'].apply(lambda x: f"{x:.2f}")
            st.dataframe(cp_data.rename(columns={'COUNTERPARTY': 'Counterparty', 'TRADE_COUNT': 'Trades', 'TOTAL_VALUE': 'Total Value', 'AVG_PRICE': 'Avg Price'}), use_container_width=True, height=300)

# ============================================
# TAB 6: MASTER DATA (Lazy loaded)
# ============================================
with tab6:
    st.markdown("""<div style="background: linear-gradient(135deg, #1e40af 0%, #3b82f6 50%, #60a5fa 100%); border-radius: 10px; padding: 0.5rem 1rem; margin-bottom: 1rem;">
        <h4 style="color: white; margin: 0; font-weight: 600;">Security Master Data Entry</h4></div>""", unsafe_allow_html=True)
    
    import json
    
    if 'lookup_result' not in st.session_state:
        st.session_state.lookup_result = None
    
    def lookup_isin_external(isin_code):
        try:
            result = session.sql(f"""
                SELECT 
                    RESULT:success::boolean as success,
                    RESULT:name::string as name,
                    RESULT:ticker::string as ticker,
                    RESULT:isin::string as isin,
                    RESULT:cusip::string as cusip,
                    RESULT:sedol::string as sedol,
                    RESULT:figi::string as figi,
                    RESULT:exchange::string as exchange,
                    RESULT:security_type::string as security_type,
                    RESULT:error::string as error
                FROM (SELECT SECURITY_MASTER_DB.GOLDEN_RECORD.LOOKUP_ISIN_EXTERNAL('{isin_code}') as RESULT)
            """).to_pandas()
            if not result.empty:
                row = result.iloc[0]
                if row['SUCCESS']:
                    return {
                        'success': True,
                        'name': row['NAME'] or '',
                        'ticker': row['TICKER'] or '',
                        'isin': row['ISIN'] or '',
                        'cusip': row['CUSIP'] or '',
                        'sedol': row['SEDOL'] or '',
                        'figi': row['FIGI'] or '',
                        'exchange': row['EXCHANGE'] or '',
                        'security_type': row['SECURITY_TYPE'] or '',
                        'source': 'OpenFIGI API'
                    }
                else:
                    return {'success': False, 'error': row['ERROR'] or 'Unknown error'}
            return {'success': False, 'error': 'Failed to call external function'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def lookup_ticker(ticker_code):
        try:
            local_result = session.sql(f"""
                SELECT SYMBOL, SECURITY_NAME, GICS_SECTOR, GICS_SUB_INDUSTRY, HEADQUARTERS, CIK, FOUNDED
                FROM SECURITY_MASTER_DB.SECURITIES.SP500 
                WHERE SYMBOL = '{ticker_code}'
                LIMIT 1
            """).to_pandas()
            if not local_result.empty:
                row = local_result.iloc[0]
                return {
                    'success': True,
                    'name': row['SECURITY_NAME'],
                    'ticker': row['SYMBOL'],
                    'exchange': 'NYSE/NASDAQ',
                    'security_type': 'Equity',
                    'sector': row['GICS_SECTOR'],
                    'sub_industry': row['GICS_SUB_INDUSTRY'],
                    'headquarters': row['HEADQUARTERS'],
                    'source': 'S&P 500 Database'
                }
            return {'success': False, 'error': 'Ticker not found in S&P 500'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    st.markdown("#### 🔍 Security Lookup")
    
    col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
    with col1:
        lookup_type = st.selectbox("Lookup By", ["ISIN", "Ticker"], key="lookup_type_select", label_visibility="collapsed")
    with col2:
        placeholder = "e.g., US0378331005" if lookup_type == "ISIN" else "e.g., AAPL"
        lookup_value = st.text_input("Value", placeholder=placeholder, max_chars=12, key="lookup_input", label_visibility="collapsed")
    with col3:
        lookup_clicked = st.button("🔎 Lookup", key="lookup_btn", use_container_width=True)
    
    if lookup_clicked and lookup_value:
        with st.spinner(f"Looking up {lookup_type}..."):
            if lookup_type == "ISIN":
                result = lookup_isin_external(lookup_value.upper().strip())
            else:
                result = lookup_ticker(lookup_value.upper().strip())
            st.session_state.lookup_result = result
            
            if result and result.get('success'):
                st.success("✅ Security found! Fields populated below.")
            else:
                st.warning(f"⚠️ {result.get('error', 'Unknown error') if result else 'No response'}")
    
    st.markdown("---")
    st.markdown("#### ➕ Security Master Record")
    
    result = st.session_state.lookup_result if st.session_state.lookup_result and st.session_state.lookup_result.get('success') else {}
    
    with st.form("new_security_form"):
        row1_col1, row1_col2 = st.columns(2)
        with row1_col1:
            sec_name = st.text_input("Security Name *", value=result.get('name', ''), key="sec_name")
        with row1_col2:
            sec_ticker = st.text_input("Ticker *", value=result.get('ticker', ''), key="sec_ticker")
        
        row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
        with row2_col1:
            sec_isin = st.text_input("ISIN", value=result.get('isin', ''), max_chars=12, key="sec_isin")
        with row2_col2:
            sec_cusip = st.text_input("CUSIP", value=result.get('cusip', ''), max_chars=9, key="sec_cusip")
        with row2_col3:
            sec_sedol = st.text_input("SEDOL", value=result.get('sedol', ''), max_chars=7, key="sec_sedol")
        with row2_col4:
            sec_figi = st.text_input("FIGI", value=result.get('figi', ''), max_chars=12, key="sec_figi")
        
        row3_col1, row3_col2, row3_col3 = st.columns(3)
        with row3_col1:
            sec_exchange = st.selectbox("Exchange", ["NYSE", "NASDAQ", "LSEG", "EUREX", "TSE", "HKEX", "ASX"], 
                                        index=0 if not result.get('exchange') else 0, key="sec_exchange")
        with row3_col2:
            sec_type = st.selectbox("Security Type", ["Equity", "Fixed Income", "ETF", "Derivative", "Fund"], 
                                    index=0, key="sec_type")
        with row3_col3:
            sec_currency = st.selectbox("Currency", ["USD", "GBP", "EUR", "JPY", "HKD", "AUD", "CAD", "CHF"], 
                                        index=0, key="sec_currency")
        
        row4_col1, row4_col2 = st.columns(2)
        with row4_col1:
            sec_sector = st.text_input("Sector", value=result.get('sector', ''), key="sec_sector")
        with row4_col2:
            sec_industry = st.text_input("Industry", value=result.get('sub_industry', ''), key="sec_industry")
        
        row5_col1, row5_col2 = st.columns(2)
        with row5_col1:
            sec_status = st.selectbox("Status", ["Active", "Pre-Issue", "Matured", "Retired"], index=0, key="sec_status")
        with row5_col2:
            sec_source = st.text_input("Golden Source", value=result.get('source', 'Manual Entry'), key="sec_source")
        
        submit_col1, submit_col2, submit_col3 = st.columns([1, 1, 2])
        with submit_col1:
            submitted = st.form_submit_button("💾 Save Security", type="primary", use_container_width=True)
        with submit_col2:
            clear_btn = st.form_submit_button("🗑️ Clear Form", use_container_width=True)
        
        if submitted:
            if not sec_name or not sec_ticker:
                st.error("Security Name and Ticker are required.")
            else:
                try:
                    session.sql(f"""
                        INSERT INTO SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE 
                        (GLOBAL_SECURITY_ID, ISSUER, ASSET_CLASS, PRIMARY_TICKER, PRIMARY_EXCHANGE, 
                         ISIN, CUSIP, SEDOL, CURRENCY, STATUS, GOLDEN_SOURCE, LAST_VALIDATED, CREATED_AT, CREATED_BY)
                        SELECT
                            'GSID_' || SECURITY_MASTER_DB.GOLDEN_RECORD.GSID_SEQ.NEXTVAL,
                            '{sec_name.replace("'", "''")}',
                            '{sec_type}',
                            '{sec_ticker}',
                            '{sec_exchange}',
                            {f"'{sec_isin}'" if sec_isin else 'NULL'},
                            {f"'{sec_cusip}'" if sec_cusip else 'NULL'},
                            {f"'{sec_sedol}'" if sec_sedol else 'NULL'},
                            '{sec_currency}',
                            '{sec_status}',
                            '{sec_source.replace("'", "''")}',
                            CURRENT_TIMESTAMP(),
                            CURRENT_TIMESTAMP(),
                            CURRENT_USER()
                    """).collect()
                    st.success(f"✅ Security '{sec_ticker} - {sec_name}' saved successfully!")
                    st.session_state.lookup_result = None
                except Exception as e:
                    st.error(f"Error saving security: {str(e)}")
        
        if clear_btn:
            st.session_state.lookup_result = None
            st.rerun()

# ============================================
# TAB 7: MASTER HISTORY (Lazy loaded)
# ============================================
with tab7:
    st.markdown("""<div style="background: linear-gradient(135deg, #7c3aed 0%, #8b5cf6 50%, #a78bfa 100%); border-radius: 10px; padding: 0.5rem 1rem; margin-bottom: 1rem;">
        <h4 style="color: white; margin: 0; font-weight: 600;">📜 Security Master History</h4></div>""", unsafe_allow_html=True)
    
    @st.cache_data(ttl=300)
    def load_history_fast():
        return session.sql("""
            SELECT HISTORY_ID, GLOBAL_SECURITY_ID, ACTION, ISSUER_BEFORE, ISSUER_AFTER,
                PRIMARY_TICKER_BEFORE, PRIMARY_TICKER_AFTER, EDIT_REASON, CHANGED_BY, CHANGED_AT
            FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
            ORDER BY CHANGED_AT DESC
            LIMIT 200
        """).to_pandas()
    
    history_data = load_history_fast()
    
    if not history_data.empty:
        hcol1, hcol2, hcol3, hcol4 = st.columns(4)
        with hcol1:
            st.metric("Total Changes", len(history_data))
        with hcol2:
            st.metric("New Securities", len(history_data[history_data['ACTION'] == 'INSERT']))
        with hcol3:
            st.metric("Updates", len(history_data[history_data['ACTION'] == 'UPDATE']))
        with hcol4:
            st.metric("Securities Modified", history_data['GLOBAL_SECURITY_ID'].nunique())
        
        st.dataframe(history_data.rename(columns={'HISTORY_ID': 'ID', 'GLOBAL_SECURITY_ID': 'GSID', 'ACTION': 'Action', 'ISSUER_BEFORE': 'Issuer (Before)', 'ISSUER_AFTER': 'Issuer (After)', 'PRIMARY_TICKER_BEFORE': 'Ticker (Before)', 'PRIMARY_TICKER_AFTER': 'Ticker (After)', 'EDIT_REASON': 'Reason', 'CHANGED_BY': 'Changed By', 'CHANGED_AT': 'Changed At'}), use_container_width=True, height=400)
        
        st.markdown("---")
        st.markdown("#### 📤 Export")
        export_col1, export_col2 = st.columns([1, 3])
        with export_col1:
            st.download_button("📄 Download CSV", history_data.to_csv(index=False), "security_master_history.csv", "text/csv", use_container_width=True)
    else:
        st.info("No history records found.")

# ============================================
# TAB 8: SETTLEMENT DETAILS
# ============================================
with tab8:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #0d9488 0%, #14b8a6 50%, #2dd4bf 100%); 
                border-radius: 10px; padding: 0.5rem 1rem; margin-bottom: 1rem;">
        <h4 style="color: white; margin: 0; font-weight: 600;">📋 Settlement Details</h4>
    </div>
    """, unsafe_allow_html=True)
    
    @st.cache_data(ttl=60)
    def load_settlement_trades(security_type=None, trade_date=None, exchange_filter=None):
        equity_query = """
            SELECT 
                t.ORDER_ID,
                'Equity' as ASSET_CLASS,
                t.TRADE_DATE,
                TO_CHAR(t.CREATED_AT, 'HH24:MI:SS') as TRADE_TIME,
                DATEADD('day', 1, t.TRADE_DATE) as SETTLEMENT_DATE,
                CASE WHEN CURRENT_DATE() >= DATEADD('day', 1, t.TRADE_DATE) THEN 'Settled' ELSE 'Pending' END as SETTLEMENT_STATUS,
                t.SIDE,
                t.SYMBOL as TICKER,
                g.ISSUER,
                g.PRIMARY_EXCHANGE as EXCHANGE,
                t.QUANTITY,
                t.PRICE,
                t.TOTAL_VALUE as AMOUNT_USD
            FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
            LEFT JOIN SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE g
                ON t.SYMBOL = g.PRIMARY_TICKER AND g.ASSET_CLASS = 'Equity'
        """
        
        bond_query = """
            SELECT 
                t.ORDER_ID,
                'Bond' as ASSET_CLASS,
                t.TRADE_DATE,
                TO_CHAR(t.CREATED_AT, 'HH24:MI:SS') as TRADE_TIME,
                DATEADD('day', 1, t.TRADE_DATE) as SETTLEMENT_DATE,
                CASE WHEN CURRENT_DATE() >= DATEADD('day', 1, t.TRADE_DATE) THEN 'Settled' ELSE 'Pending' END as SETTLEMENT_STATUS,
                t.SIDE,
                g.PRIMARY_TICKER as TICKER,
                g.ISSUER,
                g.PRIMARY_EXCHANGE as EXCHANGE,
                t.FACE_VALUE as QUANTITY,
                t.PRICE,
                t.TOTAL_VALUE as AMOUNT_USD
            FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES t
            LEFT JOIN SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE g
                ON t.CUSIP = g.CUSIP AND g.ASSET_CLASS = 'Fixed Income'
        """
        
        if security_type == "Equity":
            full_query = equity_query
        elif security_type == "Bond":
            full_query = bond_query
        elif security_type == "ETF":
            full_query = equity_query
        else:
            full_query = f"({equity_query}) UNION ALL ({bond_query})"
        
        full_query = f"SELECT * FROM ({full_query}) trades WHERE 1=1"
        
        if trade_date and trade_date != "<ALL>":
            from datetime import datetime
            parsed_date = datetime.strptime(trade_date, '%d-%b-%Y')
            full_query += f" AND TRADE_DATE = '{parsed_date.strftime('%Y-%m-%d')}'"
        if exchange_filter and exchange_filter != "<ALL>":
            full_query += f" AND EXCHANGE = '{exchange_filter}'"
        
        full_query += " ORDER BY ORDER_ID DESC LIMIT 1000"
        
        return session.sql(full_query).to_pandas()
    
    @st.cache_data(ttl=300)
    def get_trade_dates():
        return session.sql("""
            SELECT DISTINCT TRADE_DATE 
            FROM (
                SELECT TRADE_DATE FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES
                UNION
                SELECT TRADE_DATE FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES
            )
            ORDER BY TRADE_DATE DESC
        """).to_pandas()['TRADE_DATE'].tolist()
    
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns([0.4, 0.4, 0.4, 0.4, 0.375])
    
    with filter_col1:
        settle_type_filter = st.selectbox(
            "Type",
            options=["<ALL>", "Equity", "Bond", "ETF"],
            index=1,
            key="settle_type"
        )
    
    with filter_col2:
        trade_dates = ["<ALL>"] + [pd.to_datetime(d).strftime('%d-%b-%Y').upper() for d in get_trade_dates()]
        settle_date_filter = st.selectbox(
            "Trade Date",
            options=trade_dates,
            index=0,
            key="settle_date"
        )
    
    with filter_col3:
        settle_exchange_filter = st.selectbox(
            "Exchange",
            options=["<ALL>", "NYSE", "NASDAQ", "OTC"],
            index=0,
            key="settle_exchange"
        )
    
    with filter_col4:
        settle_status_filter = st.selectbox(
            "Status",
            options=["<ALL>", "Pending", "Settled", "Cancelled"],
            index=0,
            key="settle_status"
        )
    
    with filter_col5:
        st.markdown('<label style="font-size: 0.875rem; color: transparent; display: block; margin-bottom: 0.5rem;">&nbsp;</label>', unsafe_allow_html=True)
        if st.button("🔄 Refresh", key="refresh_settlement", use_container_width=True):
            st.cache_data.clear()
            st.experimental_rerun()
    
    settlement_trades = load_settlement_trades(
        security_type=settle_type_filter if settle_type_filter != "<ALL>" else None,
        trade_date=settle_date_filter if settle_date_filter != "<ALL>" else None,
        exchange_filter=settle_exchange_filter if settle_exchange_filter != "<ALL>" else None
    )
    
    if settle_status_filter != "<ALL>" and not settlement_trades.empty:
        settlement_trades = settlement_trades[settlement_trades['SETTLEMENT_STATUS'] == settle_status_filter]
    
    if not settlement_trades.empty:
        display_settle = settlement_trades[['ORDER_ID', 'ASSET_CLASS', 'TRADE_DATE', 'TRADE_TIME', 
                                            'SETTLEMENT_DATE', 'SETTLEMENT_STATUS', 'SIDE', 'TICKER',
                                            'ISSUER', 'EXCHANGE', 'QUANTITY', 'PRICE', 'AMOUNT_USD']].copy()
        display_settle['ORDER_ID'] = display_settle['ORDER_ID'].apply(lambda x: f"ORD{int(x)}" if pd.notna(x) else "N/A")
        display_settle['TRADE_DATE'] = pd.to_datetime(display_settle['TRADE_DATE']).dt.strftime('%d-%b-%Y').str.upper()
        display_settle['SETTLEMENT_DATE'] = pd.to_datetime(display_settle['SETTLEMENT_DATE']).dt.strftime('%d-%b-%Y').str.upper()
        display_settle['AMOUNT_USD'] = display_settle['AMOUNT_USD'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        display_settle['PRICE'] = display_settle['PRICE'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        display_settle['QUANTITY'] = display_settle['QUANTITY'].apply(lambda x: f"{int(x)}" if pd.notna(x) else "N/A")
        display_settle = display_settle.rename(columns={
            'ORDER_ID': 'Order ID',
            'ASSET_CLASS': 'Type',
            'TRADE_DATE': 'Trade Date',
            'TRADE_TIME': 'Time',
            'SETTLEMENT_DATE': 'Settle Date',
            'SETTLEMENT_STATUS': 'Status',
            'SIDE': 'Side',
            'TICKER': 'Ticker',
            'ISSUER': 'Issuer',
            'EXCHANGE': 'Exchange',
            'QUANTITY': 'Qty',
            'PRICE': 'Price',
            'AMOUNT_USD': 'Amount USD'
        })
        
        st.markdown(f'<p style="color: #64748b; font-size: 0.85rem;">Showing <strong>{len(settlement_trades):,}</strong> trades</p>', unsafe_allow_html=True)
        st.dataframe(display_settle, use_container_width=True, height=500)
    else:
        st.info("No trades found matching the selected filters.")

# ============================================
# TAB 9: STOCK/ETF ORDER
# ============================================
with tab9:
    import json
    
    st.markdown("""
    <div style="background: linear-gradient(135deg, #a5b4fc 0%, #93c5fd 50%, #bfdbfe 100%);
                border-radius: 8px; padding: 0.5rem 1rem; margin-bottom: 0.75rem;
                box-shadow: 0 1px 5px rgba(147, 197, 253, 0.2);">
        <h4 style="color: #1e3a5f; margin: 0; font-family: 'Outfit', sans-serif; font-weight: 700; font-size: 1.4rem;">
            📝 Stock / ETF Order
        </h4>
        <p style="color: #334155; margin: 0.1rem 0 0 0; font-size: 0.75rem;">
            Place buy and sell orders for stocks and ETFs
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    order_col1, order_col2 = st.columns([2, 1])
    
    with order_col1:
        st.markdown("""
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
                    padding: 0.75rem; margin-bottom: 0.75rem;">
            <h4 style="margin: 0; color: #0f172a; font-family: 'Outfit', sans-serif;">
                Order
            </h4>
        </div>
        """, unsafe_allow_html=True)
        
        if 'order_symbol' not in st.session_state:
            st.session_state.order_symbol = ''
        if 'order_quantity' not in st.session_state:
            st.session_state.order_quantity = 100
        if 'order_limit_price' not in st.session_state:
            st.session_state.order_limit_price = 0.0
        if 'order_stop_price' not in st.session_state:
            st.session_state.order_stop_price = 0.0
        if 'show_preview' not in st.session_state:
            st.session_state.show_preview = False
        if 'order_confirmed' not in st.session_state:
            st.session_state.order_confirmed = None
        
        tradeable = get_tradeable_securities()
        
        symbol_options = ["-- Select a symbol --"] + [f"{row['SYMBOL']} - {row['SECURITY_NAME']}" for _, row in tradeable.iterrows()]
        
        st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">SYMBOL</p>', unsafe_allow_html=True)
        selected_symbol_display = st.selectbox(
            "Symbol",
            options=symbol_options,
            key="order_symbol_select",
            label_visibility="collapsed"
        )
        
        selected_order_symbol = selected_symbol_display.split(" - ")[0] if selected_symbol_display and selected_symbol_display != "-- Select a symbol --" else ""
        
        live_price = None
        if selected_order_symbol:
            live_price_data = get_live_stock_price(selected_order_symbol)
            if live_price_data and not live_price_data.get('error'):
                live_price = live_price_data.get('price')
                market_state = live_price_data.get('market_state', 'Unknown')
                quote_time = live_price_data.get('quote_time', '')
                prev_close = live_price_data.get('previous_close')
                change = live_price - prev_close if live_price and prev_close else 0
                change_pct = (change / prev_close * 100) if prev_close else 0
                change_color = '#10b981' if change >= 0 else '#ef4444'
                change_symbol = '+' if change >= 0 else ''
                
                security_info = tradeable[tradeable['SYMBOL'] == selected_order_symbol]
                security_name = security_info.iloc[0]['SECURITY_NAME'] if not security_info.empty else selected_order_symbol
                
                st.markdown(f"""
                <div style="background: linear-gradient(90deg, #ecfdf5 0%, #d1fae5 100%);
                            border: 1px solid #10b981; border-radius: 8px; padding: 0.75rem 1rem; margin: 0.5rem 0 1rem 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-family: 'JetBrains Mono', monospace; font-size: 1.5rem; font-weight: 700; color: #0f172a;">
                                {selected_order_symbol}
                            </span>
                            <span style="color: #64748b; font-size: 0.85rem; margin-left: 0.5rem;">
                                {security_name[:40]}
                            </span>
                        </div>
                        <div style="text-align: right;">
                            <span style="font-family: 'JetBrains Mono', monospace; font-size: 1.25rem; font-weight: 600; color: #10b981;">
                                ${live_price:.2f}
                            </span>
                            <span style="font-family: 'JetBrains Mono', monospace; font-size: 0.9rem; color: {change_color}; margin-left: 0.5rem;">
                                {change_symbol}{change:.2f} ({change_symbol}{change_pct:.2f}%)
                            </span>
                            <p style="margin: 0; font-size: 0.75rem; color: #64748b;">Live Price • {market_state} • {quote_time}</p>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                quote_data = get_security_quote(selected_order_symbol)
                if quote_data is not None and not quote_data.empty:
                    q = quote_data.iloc[0]
                    live_price = q['LAST_PRICE']
                    st.markdown(f"""
                    <div style="background: linear-gradient(90deg, #ecfdf5 0%, #d1fae5 100%);
                                border: 1px solid #10b981; border-radius: 8px; padding: 0.75rem 1rem; margin: 0.5rem 0 1rem 0;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <span style="font-family: 'JetBrains Mono', monospace; font-size: 1.5rem; font-weight: 700; color: #0f172a;">
                                    {selected_order_symbol}
                                </span>
                                <span style="color: #64748b; font-size: 0.85rem; margin-left: 0.5rem;">
                                    {q['SECURITY_NAME'][:40]}
                                </span>
                            </div>
                            <div style="text-align: right;">
                                <span style="font-family: 'JetBrains Mono', monospace; font-size: 1.25rem; font-weight: 600; color: #10b981;">
                                    ${q['LAST_PRICE']:.2f}
                                </span>
                                <p style="margin: 0; font-size: 0.75rem; color: #64748b;">Last Trade Price</p>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        action_col, qty_col = st.columns(2)
        
        with action_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">ACTION</p>', unsafe_allow_html=True)
            order_action = st.selectbox(
                "Action",
                options=["Buy", "Sell", "Buy to Cover", "Sell Short"],
                key="order_action",
                label_visibility="collapsed"
            )
        
        with qty_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">QUANTITY</p>', unsafe_allow_html=True)
            order_quantity = st.number_input(
                "Quantity",
                min_value=1,
                max_value=1000000,
                value=100,
                step=1,
                key="order_qty",
                label_visibility="collapsed"
            )
        
        st.markdown("---")
        
        price_col, duration_col = st.columns(2)
        
        with price_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">PRICE TYPE</p>', unsafe_allow_html=True)
            price_type = st.selectbox(
                "Price Type",
                options=["Market", "Limit", "Stop", "Stop Limit", "Trailing Stop $", "Trailing Stop %"],
                key="order_price_type",
                label_visibility="collapsed"
            )
        
        with duration_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">DURATION</p>', unsafe_allow_html=True)
            order_duration = st.selectbox(
                "Duration",
                options=["Good for Day", "Good till Canceled (GTC)", "Fill or Kill", "Immediate or Cancel", "On the Open", "On the Close"],
                key="order_duration",
                label_visibility="collapsed"
            )
        
        if price_type in ["Limit", "Stop Limit"]:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">LIMIT PRICE</p>', unsafe_allow_html=True)
            default_limit = live_price if live_price else 100.00
            limit_price = st.number_input(
                "Limit Price",
                min_value=0.01,
                max_value=100000.00,
                value=default_limit,
                step=0.01,
                format="%.2f",
                key="order_limit",
                label_visibility="collapsed"
            )
        
        if price_type in ["Stop", "Stop Limit"]:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">STOP PRICE</p>', unsafe_allow_html=True)
            default_stop = (live_price * 0.95) if live_price else 95.00
            stop_price = st.number_input(
                "Stop Price",
                min_value=0.01,
                max_value=100000.00,
                value=default_stop,
                step=0.01,
                format="%.2f",
                key="order_stop",
                label_visibility="collapsed"
            )
        
        if price_type == "Trailing Stop $":
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">TRAIL AMOUNT ($)</p>', unsafe_allow_html=True)
            trail_amount = st.number_input(
                "Trail Amount",
                min_value=0.01,
                max_value=1000.00,
                value=5.00,
                step=0.01,
                format="%.2f",
                key="order_trail_amt",
                label_visibility="collapsed"
            )
        
        if price_type == "Trailing Stop %":
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">TRAIL PERCENTAGE (%)</p>', unsafe_allow_html=True)
            trail_percent = st.number_input(
                "Trail Percentage",
                min_value=0.1,
                max_value=50.0,
                value=5.0,
                step=0.1,
                format="%.1f",
                key="order_trail_pct",
                label_visibility="collapsed"
            )
        
        st.markdown("---")
        
        preview_col1, preview_col2 = st.columns(2)
        
        with preview_col1:
            preview_clicked = st.button("👁️ Preview Order", key="preview_order_btn", use_container_width=True, type="primary")
        
        with preview_col2:
            clear_clicked = st.button("🔄 Clear Form", key="clear_order_btn", use_container_width=True)
        
        order_message = st.empty()
        
        if clear_clicked:
            st.session_state.order_symbol = ''
            st.session_state.show_preview = False
            st.session_state.order_confirmed = None
        
        if preview_clicked:
            if not selected_order_symbol:
                st.error("⚠️ Please select a symbol to trade.")
            else:
                if price_type in ["Limit", "Stop Limit"]:
                    execution_price = limit_price
                elif price_type == "Market":
                    execution_price = live_price if live_price else 100.00
                else:
                    execution_price = live_price if live_price else 100.00
                
                security_info = tradeable[tradeable['SYMBOL'] == selected_order_symbol]
                security_name = security_info.iloc[0]['SECURITY_NAME'] if not security_info.empty else selected_order_symbol
                
                st.session_state.preview_data = {
                    'symbol': selected_order_symbol,
                    'security_name': security_name,
                    'action': order_action,
                    'quantity': order_quantity,
                    'price_type': price_type,
                    'duration': order_duration,
                    'execution_price': execution_price,
                    'est_value': order_quantity * execution_price
                }
                st.session_state.show_preview = True
                st.session_state.order_confirmed = None
        
        if st.session_state.order_confirmed:
            if st.session_state.order_confirmed.get('success'):
                st.markdown(f"""
                <div style="background: #dcfce7; border: 2px solid #22c55e; border-radius: 10px; padding: 1rem; margin: 0.75rem 0;">
                    <p style="color: #000000; margin: 0; font-size: 1rem; font-weight: 600;">
                        ✅ Confirmed {st.session_state.order_confirmed['side']} {st.session_state.order_confirmed['security_name']} ${st.session_state.order_confirmed['price']:,.2f} Quantity {st.session_state.order_confirmed['quantity']:,}
                    </p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div style="background: #fee2e2; border: 2px solid #ef4444; border-radius: 10px; padding: 1rem; margin: 0.75rem 0;">
                    <p style="color: #000000; margin: 0; font-size: 1rem; font-weight: 600;">
                        ❌ Error: {st.session_state.order_confirmed.get('error', 'Unknown error')}
                    </p>
                </div>
                """, unsafe_allow_html=True)
        
        if st.session_state.show_preview and 'preview_data' in st.session_state:
            pd_data = st.session_state.preview_data
            
            st.markdown(f"""
            <div style="background: #f3f4f6; 
                        border: 1px solid #d1d5db; border-radius: 10px; padding: 1rem; margin-top: 0.75rem;">
                <h4 style="color: #1f2937; margin: 0 0 0.75rem 0; font-size: 1rem;">📋 Order Preview</h4>
                <table style="width: 100%; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem;">
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Symbol:</td>
                        <td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{pd_data['symbol']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Action:</td>
                        <td style="padding: 0.35rem 0; color: {'#059669' if 'Buy' in pd_data['action'] else '#dc2626'}; font-weight: 700;">{pd_data['action']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Quantity:</td>
                        <td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{pd_data['quantity']:,} shares</td>
                    </tr>
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Price Type:</td>
                        <td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{pd_data['price_type']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Duration:</td>
                        <td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{pd_data['duration']}</td>
                    </tr>
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Execution Price:</td>
                        <td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">${pd_data['execution_price']:,.2f}</td>
                    </tr>
                    <tr>
                        <td style="padding: 0.35rem 0; color: #6b7280;">Est. Value:</td>
                        <td style="padding: 0.35rem 0; color: #000000; font-weight: 800;">${pd_data['est_value']:,.2f}</td>
                    </tr>
                </table>
                <p style="color: #6b7280; margin: 0.75rem 0 0 0; font-size: 0.75rem;">
                    ⚠️ Review all details before placing your order.
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            confirm_col1, confirm_col2 = st.columns(2)
            with confirm_col1:
                place_order_clicked = st.button("✅ Place Order", key="place_order_btn", use_container_width=True, type="primary")
            with confirm_col2:
                cancel_clicked = st.button("❌ Cancel", key="cancel_order_btn", use_container_width=True)
            
            if place_order_clicked:
                import uuid
                from datetime import datetime
                
                order_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
                trade_id = f"TRD-{str(uuid.uuid4())[:8].upper()}"
                now = datetime.now()
                trade_date = now.strftime('%Y-%m-%d')
                settlement_date = (now + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
                side = 'BUY' if 'Buy' in pd_data['action'] else 'SELL'
                total_value = pd_data['quantity'] * pd_data['execution_price']
                
                try:
                    session.sql(f"""
                        INSERT INTO SECURITY_MASTER_DB.TRADES.EQUITY_TRADES (
                            TRADE_ID, TRADE_DATE, SETTLEMENT_DATE, SYMBOL, SECURITY_NAME,
                            SIDE, QUANTITY, PRICE, TOTAL_VALUE, CURRENCY, EXCHANGE,
                            COUNTERPARTY, TRADER, STATUS
                        ) VALUES (
                            '{trade_id}',
                            '{trade_date}',
                            '{settlement_date}',
                            '{pd_data['symbol']}',
                            '{pd_data['security_name'].replace("'", "''")}',
                            '{side}',
                            {pd_data['quantity']},
                            {pd_data['execution_price']},
                            {total_value},
                            'USD',
                            'NYSE',
                            'INTERNAL',
                            'CURRENT_USER',
                            'CONFIRMED'
                        )
                    """).collect()
                    
                    side_code = '1' if 'Buy' in pd_data['action'] else '2'
                    
                    fixml_msg = f'''<?xml version="1.0" encoding="UTF-8"?>
<FIXML xmlns="http://www.fixprotocol.org/FIXML-5-0-SP2" v="5.0SP2">
    <ExecRpt ExecID="{trade_id}" ExecTyp="F" OrdStat="2" Side="{side_code}" LeavesQty="0" CumQty="{pd_data['quantity']}" AvgPx="{pd_data['execution_price']}" TrdDt="{trade_date}" TxnTm="{now.strftime('%Y-%m-%dT%H:%M:%S')}Z" SettlDt="{settlement_date}">
        <Hdr SID="SECMASTER" TID="EXCHANGE" Snt="{now.strftime('%Y-%m-%dT%H:%M:%S')}Z"/>
        <OrdID ID="{order_id}"/>
        <Instrmt Sym="{pd_data['symbol']}" SecTyp="CS" Exch="XNYS" ID="{pd_data['symbol']}" Src="M"/>
        <OrdQty Qty="{pd_data['quantity']}"/>
        <Px Px="{pd_data['execution_price']}"/>
        <TrdCapRpt LastQty="{pd_data['quantity']}" LastPx="{pd_data['execution_price']}"/>
        <Amt Typ="SMTL" Amt="{total_value}" Ccy="USD"/>
        <Comm Typ="3" Comm="0.00" Ccy="USD"/>
        <Pty ID="SECMASTER" R="1"/>
        <Pty ID="EXCHANGE" R="17"/>
    </ExecRpt>
</FIXML>'''
                    
                    fixml_filename = f"FIX_{now.strftime('%d-%b-%Y').upper()}_{now.strftime('%H-%M-%S')}_{pd_data['symbol']}_{side}.xml"
                    
                    session.sql(f"""
                        COPY INTO @SECURITY_MASTER_DB.TRADES.FIX_STAGE/{fixml_filename}
                        FROM (SELECT '{fixml_msg.replace("'", "''")}')
                        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE)
                        OVERWRITE = TRUE
                        SINGLE = TRUE
                    """).collect()
                    
                    st.session_state.order_confirmed = {
                        'success': True,
                        'side': side,
                        'security_name': pd_data['security_name'],
                        'price': pd_data['execution_price'],
                        'quantity': pd_data['quantity']
                    }
                    st.session_state.show_preview = False
                    st.cache_data.clear()
                    st.success(f"✅ Confirmed {side} {pd_data['security_name']} ${pd_data['execution_price']:,.2f} Quantity {pd_data['quantity']:,} - Portfolio updated!")
                    st.experimental_rerun()
                    
                except Exception as e:
                    st.session_state.order_confirmed = {
                        'success': False,
                        'error': str(e)
                    }
                    st.session_state.show_preview = False
            
            if cancel_clicked:
                st.session_state.show_preview = False
                st.session_state.order_confirmed = None
    
    with order_col2:
        st.markdown("""
        <div style="background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px;
                    padding: 0.5rem; margin-bottom: 0.5rem;">
            <h5 style="margin: 0; color: #0f172a; font-family: 'Outfit', sans-serif; font-size: 0.85rem;">
                💡 Order Types
            </h5>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div style="font-size: 0.8rem;">
        
        **Market Order**  
        Executes immediately at current market price.
        
        ---
        
        **Limit Order**  
        Executes only at your specified price or better.
        
        ---
        
        **Stop Order**  
        Triggers a market order when stop price is reached.
        
        ---
        
        **Stop Limit**  
        Triggers a limit order when stop price is reached.
        
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown("#### 📊 Quick Stats")
        
        if selected_order_symbol:
            live_price_data = get_live_stock_price(selected_order_symbol)
            if live_price_data and not live_price_data.get('error'):
                st.metric("Live Price", f"${live_price_data.get('price', 0):.2f}")
                st.metric("Previous Close", f"${live_price_data.get('previous_close', 0):.2f}")
                st.metric("Market State", live_price_data.get('market_state', 'Unknown'))
                st.metric("Exchange", live_price_data.get('exchange', 'N/A'))
            else:
                quote_data = get_security_quote(selected_order_symbol)
                if quote_data is not None and not quote_data.empty:
                    q = quote_data.iloc[0]
                    st.metric("Last Price", f"${q['LAST_PRICE']:.2f}")
                    st.metric("Avg Price (All Trades)", f"${q['AVG_PRICE']:.2f}")
                    st.metric("Total Trades", f"{int(q['TRADE_COUNT']):,}")
        else:
            st.info("Select a symbol to view stats")

# ============================================
# TAB 10: BOND ORDER
# ============================================
with tab10:
    import json
    
    st.markdown("""
    <div style="background: linear-gradient(135deg, #c4b5fd 0%, #a78bfa 50%, #8b5cf6 100%);
                border-radius: 8px; padding: 0.5rem 1rem; margin-bottom: 0.75rem;
                box-shadow: 0 1px 5px rgba(139, 92, 246, 0.2);">
        <h4 style="color: white; margin: 0; font-family: 'Outfit', sans-serif; font-weight: 700; font-size: 1.4rem;">
            🏦 Bond Order
        </h4>
        <p style="color: #e9d5ff; margin: 0.1rem 0 0 0; font-size: 0.75rem;">
            Place buy and sell orders for fixed income securities
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    bond_order_col1, bond_order_col2 = st.columns([2, 1])
    
    with bond_order_col1:
        st.markdown("""
        <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px;
                    padding: 0.75rem; margin-bottom: 0.75rem;">
            <h4 style="margin: 0; color: #0f172a; font-family: 'Outfit', sans-serif;">
                Bond Order
            </h4>
        </div>
        """, unsafe_allow_html=True)
        
        if 'bond_show_preview' not in st.session_state:
            st.session_state.bond_show_preview = False
        if 'bond_order_confirmed' not in st.session_state:
            st.session_state.bond_order_confirmed = None
        
        tradeable_bonds = get_tradeable_bonds()
        bond_holdings = get_bond_holdings()
        
        bond_input_col1, bond_input_col2, bond_input_col3 = st.columns([2, 0.3, 2])
        
        with bond_input_col1:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">CUSIP, symbol or company name</p>', unsafe_allow_html=True)
            bond_options = ["-- Select a bond --"] + [f"{row['CUSIP']} - {row['ISSUER_NAME']}" for _, row in tradeable_bonds.iterrows()]
            selected_bond_display = st.selectbox(
                "Bond",
                options=bond_options,
                key="bond_search_select",
                label_visibility="collapsed"
            )
        
        with bond_input_col2:
            st.markdown('<p style="text-align: center; padding-top: 1.5rem; color: #64748b;">or</p>', unsafe_allow_html=True)
        
        with bond_input_col3:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">Choose from existing holdings</p>', unsafe_allow_html=True)
            holding_options = ["-- Select --"] + [f"{row['CUSIP']} - {row['ISSUER_NAME']}" for _, row in bond_holdings.iterrows()]
            selected_holding = st.selectbox(
                "Holdings",
                options=holding_options,
                key="bond_holding_select",
                label_visibility="collapsed"
            )
        
        selected_bond_cusip = None
        selected_bond_info = None
        
        if selected_bond_display and selected_bond_display != "-- Select a bond --":
            selected_bond_cusip = selected_bond_display.split(" - ")[0]
            bond_match = tradeable_bonds[tradeable_bonds['CUSIP'] == selected_bond_cusip]
            if not bond_match.empty:
                selected_bond_info = bond_match.iloc[0]
        elif selected_holding and selected_holding != "-- Select --":
            selected_bond_cusip = selected_holding.split(" - ")[0]
            bond_match = tradeable_bonds[tradeable_bonds['CUSIP'] == selected_bond_cusip]
            if not bond_match.empty:
                selected_bond_info = bond_match.iloc[0]
        
        if selected_bond_info is not None:
            st.markdown(f"""
            <div style="background: linear-gradient(90deg, #faf5ff 0%, #f3e8ff 100%);
                        border: 1px solid #8b5cf6; border-radius: 8px; padding: 0.75rem 1rem; margin: 0.5rem 0 1rem 0;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <span style="font-family: 'JetBrains Mono', monospace; font-size: 1.3rem; font-weight: 700; color: #0f172a;">
                            {selected_bond_info['ISSUER_NAME']}
                        </span>
                        <p style="margin: 0.25rem 0 0 0; font-size: 0.8rem; color: #64748b;">
                            CUSIP: {selected_bond_info['CUSIP']} | Rating: {selected_bond_info['CREDIT_RATING']}
                        </p>
                    </div>
                    <div style="text-align: right;">
                        <span style="font-family: 'JetBrains Mono', monospace; font-size: 1.1rem; font-weight: 600; color: #8b5cf6;">
                            {selected_bond_info['CURRENT_YIELD']:.2f}% Yield
                        </span>
                        <p style="margin: 0; font-size: 0.75rem; color: #64748b;">Coupon: {selected_bond_info['COUPON_RATE']:.2f}%</p>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        bond_action_col, bond_qty_col = st.columns(2)
        
        with bond_action_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">Action</p>', unsafe_allow_html=True)
            bond_action = st.selectbox(
                "Action",
                options=["Select", "Buy", "Sell"],
                key="bond_action",
                label_visibility="collapsed"
            )
        
        with bond_qty_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">Quantity (Face Value in $1,000s)</p>', unsafe_allow_html=True)
            bond_quantity = st.number_input(
                "Quantity",
                min_value=1,
                max_value=10000,
                value=10,
                step=1,
                key="bond_qty",
                label_visibility="collapsed"
            )
        
        st.markdown("---")
        
        st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">Execution Type</p>', unsafe_allow_html=True)
        bond_exec_type = st.selectbox(
            "Execution Type",
            options=["--", "Market", "Limit"],
            key="bond_exec_type",
            label_visibility="collapsed"
        )
        
        price_mode = st.radio(
            "Price Mode",
            options=["Price", "Yield"],
            key="bond_price_mode",
            horizontal=True
        )
        
        if price_mode == "Price":
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">Price (% of par)</p>', unsafe_allow_html=True)
            bond_price = st.number_input(
                "Price",
                min_value=0.01,
                max_value=200.00,
                value=100.00,
                step=0.01,
                format="%.4f",
                key="bond_price",
                label_visibility="collapsed"
            )
            bond_yield_input = None
        else:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">Yield (%)</p>', unsafe_allow_html=True)
            bond_yield_input = st.number_input(
                "Yield",
                min_value=0.01,
                max_value=50.00,
                value=selected_bond_info['CURRENT_YIELD'] if selected_bond_info is not None else 5.00,
                step=0.01,
                format="%.2f",
                key="bond_yield_input",
                label_visibility="collapsed"
            )
            bond_price = 100.00
        
        st.markdown("---")
        
        comm_col, total_col, calc_col = st.columns([1, 1, 1])
        
        with comm_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.1rem;">Comm.</p>', unsafe_allow_html=True)
            st.markdown('<p style="font-size: 1.1rem; font-weight: 700; color: #0f172a;">$0.00</p>', unsafe_allow_html=True)
        
        face_value = bond_quantity * 1000
        est_total = face_value * (bond_price / 100)
        
        with total_col:
            st.markdown('<p style="font-size: 0.85rem; color: #64748b; margin-bottom: 0.1rem;">Est. Total</p>', unsafe_allow_html=True)
            st.markdown(f'<p style="font-size: 1.1rem; font-weight: 700; color: #0f172a;">${est_total:,.2f}</p>', unsafe_allow_html=True)
        
        with calc_col:
            if st.button("Calculate totals", key="calc_bond_totals"):
                st.info(f"Face Value: ${face_value:,.0f} | Est. Total: ${est_total:,.2f}")
        
        st.markdown("---")
        
        bond_preview = st.button("👁️ Preview order", key="bond_preview_btn", use_container_width=True, type="primary")
        
        if bond_preview:
            if not selected_bond_cusip:
                st.error("⚠️ Please select a bond to trade.")
            elif bond_action == "Select":
                st.error("⚠️ Please select an action (Buy or Sell).")
            else:
                st.session_state.bond_preview_data = {
                    'cusip': selected_bond_cusip,
                    'bond_id': selected_bond_info['BOND_ID'],
                    'issuer_name': selected_bond_info['ISSUER_NAME'],
                    'action': bond_action,
                    'quantity': bond_quantity,
                    'face_value': face_value,
                    'price': bond_price,
                    'yield_value': bond_yield_input if bond_yield_input else selected_bond_info['CURRENT_YIELD'],
                    'exec_type': bond_exec_type,
                    'est_total': est_total,
                    'credit_rating': selected_bond_info['CREDIT_RATING'],
                    'coupon_rate': selected_bond_info['COUPON_RATE']
                }
                st.session_state.bond_show_preview = True
                st.session_state.bond_order_confirmed = None
        
        if st.session_state.bond_order_confirmed and st.session_state.bond_order_confirmed.get('success'):
            conf = st.session_state.bond_order_confirmed
            st.markdown(f"""
            <div style="background: #dcfce7; border: 2px solid #22c55e; border-radius: 10px; padding: 1rem; margin: 0.75rem 0;">
                <p style="color: #166534; margin: 0; font-size: 1rem; font-weight: 600;">
                    ✅ Bond {conf['issuer_name']} confirmed at ${conf['price']:.2f} with Quantity {conf['quantity']:,}
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        if st.session_state.bond_show_preview and 'bond_preview_data' in st.session_state:
            bpd = st.session_state.bond_preview_data
            
            st.markdown(f"""
            <div style="background: #f3f4f6; border: 1px solid #d1d5db; border-radius: 10px; padding: 1rem; margin-top: 0.75rem;">
                <h4 style="color: #1f2937; margin: 0 0 0.75rem 0; font-size: 1rem;">📋 Bond Order Preview</h4>
                <table style="width: 100%; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem;">
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">Issuer:</td><td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{bpd['issuer_name']}</td></tr>
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">CUSIP:</td><td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{bpd['cusip']}</td></tr>
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">Action:</td><td style="padding: 0.35rem 0; color: {'#059669' if bpd['action'] == 'Buy' else '#dc2626'}; font-weight: 700;">{bpd['action']}</td></tr>
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">Face Value:</td><td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">${bpd['face_value']:,}</td></tr>
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">Price:</td><td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{bpd['price']:.4f}%</td></tr>
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">Yield:</td><td style="padding: 0.35rem 0; color: #000000; font-weight: 700;">{bpd['yield_value']:.2f}%</td></tr>
                    <tr><td style="padding: 0.35rem 0; color: #6b7280;">Est. Total:</td><td style="padding: 0.35rem 0; color: #000000; font-weight: 800;">${bpd['est_total']:,.2f}</td></tr>
                </table>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            bond_confirm_col1, bond_confirm_col2 = st.columns(2)
            with bond_confirm_col1:
                place_bond_order = st.button("✅ Place Order", key="place_bond_order_btn", use_container_width=True, type="primary")
            with bond_confirm_col2:
                cancel_bond_order = st.button("❌ Cancel", key="cancel_bond_order_btn", use_container_width=True)
            
            if place_bond_order:
                import uuid
                from datetime import datetime
                
                order_id = f"BND-{str(uuid.uuid4())[:8].upper()}"
                trade_id = f"TRD-{str(uuid.uuid4())[:8].upper()}"
                now = datetime.now()
                trade_date = now.strftime('%Y-%m-%d')
                settlement_date = (now + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
                side = bpd['action'].upper()
                side_code = '1' if side == 'BUY' else '2'
                
                try:
                    session.sql(f"""
                        INSERT INTO SECURITY_MASTER_DB.TRADES.BOND_TRADES (
                            TRADE_ID, TRADE_DATE, SETTLEMENT_DATE, BOND_ID, CUSIP, ISSUER,
                            SIDE, FACE_VALUE, PRICE, YIELD, TOTAL_VALUE, CURRENCY,
                            COUNTERPARTY, TRADER, STATUS
                        ) VALUES (
                            '{trade_id}',
                            '{trade_date}',
                            '{settlement_date}',
                            '{bpd['bond_id']}',
                            '{bpd['cusip']}',
                            '{bpd['issuer_name'].replace("'", "''")}',
                            '{side}',
                            {bpd['face_value']},
                            {bpd['price']},
                            {bpd['yield_value']},
                            {bpd['est_total']},
                            'USD',
                            'INTERNAL',
                            'CURRENT_USER',
                            'CONFIRMED'
                        )
                    """).collect()
                    
                    fixml_bond_msg = f'''<?xml version="1.0" encoding="UTF-8"?>
<FIXML xmlns="http://www.fixprotocol.org/FIXML-5-0-SP2" v="5.0SP2">
    <ExecRpt ExecID="{trade_id}" ExecTyp="F" OrdStat="2" Side="{side_code}" LeavesQty="0" CumQty="{bpd['face_value']}" AvgPx="{bpd['price']}" TrdDt="{trade_date}" TxnTm="{now.strftime('%Y-%m-%dT%H:%M:%S')}Z" SettlDt="{settlement_date}">
        <Hdr SID="SECMASTER" TID="EXCHANGE" Snt="{now.strftime('%Y-%m-%dT%H:%M:%S')}Z"/>
        <OrdID ID="{order_id}"/>
        <Instrmt CUSIP="{bpd['cusip']}" SecTyp="CORP" ID="{bpd['cusip']}" Src="1" Issr="{bpd['issuer_name'].replace('"', '&quot;')}"/>
        <Yield Typ="CURRENT" Yld="{bpd['yield_value']}"/>
        <OrdQty Qty="{bpd['face_value']}"/>
        <Px Px="{bpd['price']}"/>
        <TrdCapRpt LastQty="{bpd['face_value']}" LastPx="{bpd['price']}"/>
        <Amt Typ="SMTL" Amt="{bpd['est_total']}" Ccy="USD"/>
        <Comm Typ="3" Comm="0.00" Ccy="USD"/>
        <Pty ID="SECMASTER" R="1"/>
        <Pty ID="EXCHANGE" R="17"/>
    </ExecRpt>
</FIXML>'''
                    
                    bond_symbol = bpd['cusip']
                    fixml_bond_filename = f"FIXML_BOND_{side}_{bond_symbol}_{now.strftime('%d-%b-%Y').upper()}_{now.strftime('%H-%M-%S')}.xml"
                    
                    session.sql(f"""
                        COPY INTO @SECURITY_MASTER_DB.TRADES.BOND_ORDERS/{fixml_bond_filename}
                        FROM (SELECT '{fixml_bond_msg.replace("'", "''")}')
                        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = NONE)
                        OVERWRITE = TRUE
                        SINGLE = TRUE
                    """).collect()
                    
                    st.session_state.bond_order_confirmed = {
                        'success': True,
                        'side': side,
                        'issuer_name': bpd['issuer_name'],
                        'price': bpd['price'],
                        'quantity': bpd['quantity'],
                        'total': bpd['est_total']
                    }
                    st.session_state.bond_show_preview = False
                    st.cache_data.clear()
                    st.markdown(f"""
                    <div style="background: #dcfce7; border: 2px solid #22c55e; border-radius: 10px; padding: 1rem; margin: 0.75rem 0;">
                        <p style="color: #166534; margin: 0; font-size: 1rem; font-weight: 600;">
                            ✅ Bond {bpd['issuer_name']} confirmed at ${bpd['price']:.2f} with Quantity {bpd['quantity']:,}
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
                    st.experimental_rerun()
                    
                except Exception as e:
                    st.session_state.bond_order_confirmed = {
                        'success': False,
                        'error': str(e)
                    }
                    st.session_state.bond_show_preview = False
            
            if cancel_bond_order:
                st.session_state.bond_show_preview = False
                st.session_state.bond_order_confirmed = None
    
    with bond_order_col2:
        st.markdown("""
        <div style="background: #f1f5f9; border: 1px solid #e2e8f0; border-radius: 6px;
                    padding: 0.5rem; margin-bottom: 0.5rem;">
            <h5 style="margin: 0; color: #0f172a; font-family: 'Outfit', sans-serif; font-size: 0.85rem;">
                💡 Bond Order Types
            </h5>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div style="font-size: 0.8rem;">
        
        **Market Order**  
        Executes at current market price.
        
        ---
        
        **Limit Order**  
        Executes at your specified price or better.
        
        ---
        
        **Price vs Yield**  
        Price: % of par value (100 = par)  
        Yield: Annual return as percentage
        
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown("#### 📊 Bond Quick Stats")
        
        if selected_bond_info is not None:
            st.metric("Current Yield", f"{selected_bond_info['CURRENT_YIELD']:.2f}%")
            st.metric("Coupon Rate", f"{selected_bond_info['COUPON_RATE']:.2f}%")
            st.metric("Credit Rating", selected_bond_info['CREDIT_RATING'])
            st.metric("Maturity", str(selected_bond_info['MATURITY_DATE'])[:10])
        else:
            st.info("Select a bond to view stats")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #94a3b8; font-family: 'JetBrains Mono', monospace; font-size: 0.75rem;">
    Data Source: Snowflake Marketplace | Built with Streamlit in Snowflake
</div>
""", unsafe_allow_html=True)
