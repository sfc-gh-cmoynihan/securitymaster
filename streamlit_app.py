# ============================================
# SECURITY MASTER STREAMLIT APP
# Portfolio Analysis & Trade Viewer
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
    
    .stApp {
        background-color: var(--bg-white) !important;
    }
    
    .main .block-container {
        padding-top: 2rem;
        max-width: 1400px;
        background-color: var(--bg-white);
    }
    
    h1, h2, h3 {
        font-family: 'Outfit', sans-serif !important;
        color: var(--text-primary) !important;
    }
    
    .stMarkdown p, .stMarkdown li {
        font-family: 'Outfit', sans-serif;
        color: var(--text-secondary);
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1rem 1.5rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    [data-testid="stMetricLabel"] {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: var(--primary) !important;
    }
    
    [data-testid="stMetricValue"] {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 700 !important;
        font-size: 1.8rem !important;
        color: var(--text-primary) !important;
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 8px;
        font-family: 'JetBrains Mono', monospace;
    }
    
    /* DataFrame styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border);
    }
    
    /* Custom header */
    .hero-header {
        background: linear-gradient(90deg, var(--primary), var(--primary-light));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 3rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    .hero-sub {
        color: var(--text-secondary);
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    
    /* Card containers */
    .info-card {
        background: var(--bg-light);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    
    .gain-text { color: var(--gain) !important; font-weight: 600; }
    .loss-text { color: var(--loss) !important; font-weight: 600; }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--bg-light);
        border-radius: 12px;
        padding: 0.5rem;
        border: 1px solid var(--border);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
        border-radius: 8px;
        color: var(--text-secondary);
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, var(--primary), var(--primary-light)) !important;
        color: white !important;
    }
    
    /* Make sure all backgrounds are white */
    section[data-testid="stSidebar"] {
        background-color: var(--bg-light) !important;
    }
    
    div[data-testid="stToolbar"] {
        background-color: var(--bg-white) !important;
    }
</style>
""", unsafe_allow_html=True)

# Get Snowflake session
session = get_active_session()

# Header with Snowflake logo
st.markdown('''
<div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;">
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" style="height: 50px; width: 50px;">
        <polygon points="50,5 61,27 85,27 66,42 73,65 50,52 27,65 34,42 15,27 39,27" fill="#29B5E8"/>
        <polygon points="50,25 56,37 70,37 59,45 63,58 50,50 37,58 41,45 30,37 44,37" fill="white"/>
    </svg>
    <span style="font-family: 'Outfit', sans-serif; font-size: 2.5rem; font-weight: 700; 
                 background: linear-gradient(135deg, #29b5e8 0%, #0d9488 100%); 
                 -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
        Security Master EDM
    </span>
</div>
<p style="color: #475569; font-size: 1rem; margin-bottom: 1.5rem;">Powered by Snowflake | Portfolio Analysis & Trade Tracking</p>
''', unsafe_allow_html=True)

# Load S&P 500 data
@st.cache_data(ttl=600)
def load_securities():
    return session.sql("""
        SELECT 
            SYMBOL,
            SECURITY_NAME,
            GICS_SECTOR,
            GICS_SUB_INDUSTRY,
            HEADQUARTERS
        FROM SECURITY_MASTER_DB.SECURITIES.SP500 
        ORDER BY SYMBOL
    """).to_pandas()

@st.cache_data(ttl=60)
def load_trades(symbol=None):
    if symbol and symbol != "All Securities":
        query = f"""
            SELECT 
                t.TRADE_ID,
                t.SYMBOL,
                s.SECURITY_NAME,
                t.TRADE_DATE,
                t.TRADE_TYPE,
                t.QUANTITY,
                t.PRICE,
                t.TOTAL_VALUE
            FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
            JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
            WHERE t.SYMBOL = '{symbol}'
            ORDER BY t.TRADE_DATE DESC
        """
    else:
        query = """
            SELECT 
                t.TRADE_ID,
                t.SYMBOL,
                s.SECURITY_NAME,
                t.TRADE_DATE,
                t.TRADE_TYPE,
                t.QUANTITY,
                t.PRICE,
                t.TOTAL_VALUE
            FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
            JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
            ORDER BY t.TRADE_DATE DESC
            LIMIT 1000
        """
    return session.sql(query).to_pandas()

@st.cache_data(ttl=60)
def get_portfolio_summary():
    return session.sql("""
        SELECT 
            SYMBOL,
            COUNT(*) as TRADE_COUNT,
            SUM(CASE WHEN TRADE_TYPE = 'BUY' THEN QUANTITY ELSE 0 END) as TOTAL_BOUGHT,
            SUM(CASE WHEN TRADE_TYPE = 'SELL' THEN QUANTITY ELSE 0 END) as TOTAL_SOLD,
            SUM(CASE WHEN TRADE_TYPE = 'BUY' THEN TOTAL_VALUE ELSE 0 END) as BUY_VALUE,
            SUM(CASE WHEN TRADE_TYPE = 'SELL' THEN TOTAL_VALUE ELSE 0 END) as SELL_VALUE,
            SUM(CASE WHEN TRADE_TYPE = 'BUY' THEN QUANTITY ELSE 0 END) - 
                SUM(CASE WHEN TRADE_TYPE = 'SELL' THEN QUANTITY ELSE 0 END) as NET_POSITION,
            AVG(PRICE) as AVG_PRICE
        FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES
        GROUP BY SYMBOL
        ORDER BY ABS(BUY_VALUE - SELL_VALUE) DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_sector_breakdown():
    return session.sql("""
        SELECT 
            s.GICS_SECTOR,
            COUNT(DISTINCT t.SYMBOL) as SECURITIES_TRADED,
            SUM(t.TOTAL_VALUE) as TOTAL_VALUE,
            SUM(CASE WHEN t.TRADE_TYPE = 'BUY' THEN t.TOTAL_VALUE ELSE 0 END) as BUY_VALUE,
            SUM(CASE WHEN t.TRADE_TYPE = 'SELL' THEN t.TOTAL_VALUE ELSE 0 END) as SELL_VALUE
        FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
        JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
        GROUP BY s.GICS_SECTOR
        ORDER BY TOTAL_VALUE DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_trades_with_nyse_master():
    return session.sql("""
        SELECT 
            t.TRADE_ID,
            t.SYMBOL,
            n.COMPANY_NAME as NYSE_COMPANY_NAME,
            n.FIGI,
            t.TRADE_DATE,
            t.TRADE_TYPE,
            t.QUANTITY,
            t.PRICE,
            t.TOTAL_VALUE,
            CASE WHEN n.SYMBOL IS NOT NULL THEN 'Matched' ELSE 'Unmatched' END as MATCH_STATUS
        FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
        LEFT JOIN SECURITY_MASTER_DB.EQUITY.NYSE_SECURITIES n ON t.SYMBOL = n.SYMBOL
        ORDER BY t.TRADE_DATE DESC
        LIMIT 1000
    """).to_pandas()

@st.cache_data(ttl=60)
def get_trade_match_summary():
    return session.sql("""
        SELECT 
            CASE WHEN n.SYMBOL IS NOT NULL THEN 'Matched' ELSE 'Unmatched' END as MATCH_STATUS,
            COUNT(*) as TRADE_COUNT,
            COUNT(DISTINCT t.SYMBOL) as UNIQUE_SYMBOLS,
            SUM(t.TOTAL_VALUE) as TOTAL_VALUE
        FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
        LEFT JOIN SECURITY_MASTER_DB.EQUITY.NYSE_SECURITIES n ON t.SYMBOL = n.SYMBOL
        GROUP BY CASE WHEN n.SYMBOL IS NOT NULL THEN 'Matched' ELSE 'Unmatched' END
    """).to_pandas()

@st.cache_data(ttl=60)
def load_bond_trades():
    return session.sql("""
        SELECT 
            t.TRADE_ID,
            t.CUSIP,
            b.ISSUER_NAME,
            b.TICKER,
            b.CREDIT_RATING,
            t.TRADE_DATE,
            t.TRADE_TYPE,
            t.QUANTITY,
            t.PRICE,
            t.YIELD_AT_TRADE,
            t.TOTAL_VALUE,
            t.COUNTERPARTY,
            t.SETTLEMENT_DATE,
            b.SECTOR
        FROM SECURITY_TRADES_DB.TRADES.BOND_TRADES t
        JOIN SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS b ON t.CUSIP = b.CUSIP
        ORDER BY t.TRADE_DATE DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_bond_trade_summary():
    return session.sql("""
        SELECT 
            b.ISSUER_NAME,
            b.TICKER,
            COUNT(*) as TRADE_COUNT,
            SUM(CASE WHEN t.TRADE_TYPE = 'BUY' THEN t.QUANTITY ELSE 0 END) as TOTAL_BOUGHT,
            SUM(CASE WHEN t.TRADE_TYPE = 'SELL' THEN t.QUANTITY ELSE 0 END) as TOTAL_SOLD,
            SUM(CASE WHEN t.TRADE_TYPE = 'BUY' THEN t.TOTAL_VALUE ELSE 0 END) as BUY_VALUE,
            SUM(CASE WHEN t.TRADE_TYPE = 'SELL' THEN t.TOTAL_VALUE ELSE 0 END) as SELL_VALUE,
            AVG(t.PRICE) as AVG_PRICE,
            AVG(t.YIELD_AT_TRADE) as AVG_YIELD
        FROM SECURITY_TRADES_DB.TRADES.BOND_TRADES t
        JOIN SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS b ON t.CUSIP = b.CUSIP
        GROUP BY b.ISSUER_NAME, b.TICKER
        ORDER BY SUM(t.TOTAL_VALUE) DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_bond_trades_by_counterparty():
    return session.sql("""
        SELECT 
            COUNTERPARTY,
            COUNT(*) as TRADE_COUNT,
            SUM(TOTAL_VALUE) as TOTAL_VALUE,
            AVG(PRICE) as AVG_PRICE
        FROM SECURITY_TRADES_DB.TRADES.BOND_TRADES
        GROUP BY COUNTERPARTY
        ORDER BY TOTAL_VALUE DESC
    """).to_pandas()

# Load data
securities = load_securities()
portfolio_summary = get_portfolio_summary()
sector_data = get_sector_breakdown()

# Top metrics
col1, col2, col3, col4, col5 = st.columns(5)

total_trades = portfolio_summary['TRADE_COUNT'].sum()
total_aum = 100_000_000_000  # $100 Billion AUM
cash_balance = 50_000_000  # $50 Million Cash
us_treasury_bonds = 10_000_000_000  # $10 Billion in US Treasury at 4.25%
net_pnl = 2_450_000_000  # $2.45 Billion profit

with col1:
    st.metric("Total AUM", f"${total_aum/1e9:.0f}B")
with col2:
    st.metric("Cash Balance", f"${cash_balance/1e6:.0f}M")
with col3:
    st.metric("US Treasury (4.25%)", f"${us_treasury_bonds/1e9:.0f}B")
with col4:
    st.metric("Total Trades", f"{int(total_trades):,}")
with col5:
    pnl_color = "🟢" if net_pnl >= 0 else "🔴"
    st.metric("Realized P&L", f"{pnl_color} ${net_pnl:,.0f}")

st.markdown("---")

# Tabs for different views
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs(["📊 Portfolio Overview", "🔍 Trade History", "📈 Sector Analysis", "🔗 Equity Trades", "📉 Bond Trades", "✏️ Master Data Reference", "📜 Security Master History", "📋 Trades Mapped to Master Data"])

with tab1:
    st.subheader("📊 Portfolio Summary by Security")
    
    portfolio_with_pnl = portfolio_summary.copy()
    portfolio_with_pnl['REALIZED_PNL'] = portfolio_with_pnl['SELL_VALUE'] - portfolio_with_pnl['BUY_VALUE']
    
    st.subheader("📈 Top 10 Equity Performers")
    
    portfolio_with_names = portfolio_with_pnl.merge(
        securities[['SYMBOL', 'SECURITY_NAME']], on='SYMBOL', how='left'
    )
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("🟢 **Top 10 Gainers**")
        gainers_chart = portfolio_with_names.nlargest(10, 'REALIZED_PNL')[['SECURITY_NAME', 'REALIZED_PNL']].copy()
        gainers_chart = gainers_chart.sort_values('REALIZED_PNL', ascending=True)
        gainers_chart = gainers_chart.set_index('SECURITY_NAME')
        st.bar_chart(gainers_chart, use_container_width=True, height=300)
    
    with chart_col2:
        st.markdown("🔴 **Top 10 Losers**")
        losers_chart = portfolio_with_names.nsmallest(10, 'REALIZED_PNL')[['SECURITY_NAME', 'REALIZED_PNL']].copy()
        losers_chart['REALIZED_PNL'] = losers_chart['REALIZED_PNL'].abs()
        losers_chart = losers_chart.sort_values('REALIZED_PNL', ascending=True)
        losers_chart = losers_chart.set_index('SECURITY_NAME')
        st.bar_chart(losers_chart, use_container_width=True, height=300)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🟢 Top Gainers")
        gainers = portfolio_with_pnl.nlargest(10, 'REALIZED_PNL')[['SYMBOL', 'TRADE_COUNT', 'REALIZED_PNL']].copy()
        gainers['REALIZED_PNL'] = gainers['REALIZED_PNL'].apply(lambda x: f"${x:,.0f}")
        gainers = gainers.rename(columns={
            'SYMBOL': 'Symbol',
            'TRADE_COUNT': 'Trades',
            'REALIZED_PNL': 'P&L'
        })
        st.dataframe(gainers, use_container_width=True)
    
    with col2:
        st.markdown("#### 🔴 Top Losers")
        losers = portfolio_with_pnl.nsmallest(10, 'REALIZED_PNL')[['SYMBOL', 'TRADE_COUNT', 'REALIZED_PNL']].copy()
        losers['REALIZED_PNL'] = losers['REALIZED_PNL'].apply(lambda x: f"${x:,.0f}")
        losers = losers.rename(columns={
            'SYMBOL': 'Symbol',
            'TRADE_COUNT': 'Trades',
            'REALIZED_PNL': 'P&L'
        })
        st.dataframe(losers, use_container_width=True)
    
    # Full portfolio table
    st.markdown("#### 📈 Full Equity Portfolio")
    display_portfolio = portfolio_summary.copy()
    display_portfolio['BUY_VALUE'] = display_portfolio['BUY_VALUE'].apply(lambda x: f"${x:,.0f}")
    display_portfolio['SELL_VALUE'] = display_portfolio['SELL_VALUE'].apply(lambda x: f"${x:,.0f}")
    display_portfolio['AVG_PRICE'] = display_portfolio['AVG_PRICE'].apply(lambda x: f"${x:.2f}")
    display_portfolio = display_portfolio.rename(columns={
        'SYMBOL': 'Symbol',
        'TRADE_COUNT': 'Trades',
        'TOTAL_BOUGHT': 'Bought',
        'TOTAL_SOLD': 'Sold',
        'BUY_VALUE': 'Buy Value',
        'SELL_VALUE': 'Sell Value',
        'NET_POSITION': 'Net Pos',
        'AVG_PRICE': 'Avg Price'
    })
    
    st.dataframe(display_portfolio, use_container_width=True, height=300)
    
    st.markdown("---")
    st.subheader("💵 Bond Portfolio - Yield Analysis")
    
    @st.cache_data(ttl=600)
    def get_active_bonds_by_yield():
        return session.sql("""
            SELECT 
                BOND_ID,
                TICKER,
                ISSUER_NAME,
                COUPON_RATE,
                CURRENT_YIELD,
                MATURITY_DATE,
                CREDIT_RATING,
                PAR_VALUE,
                CURRENCY
            FROM SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS
            WHERE MATURITY_DATE > CURRENT_DATE()
            ORDER BY CURRENT_YIELD DESC
        """).to_pandas()
    
    active_bonds = get_active_bonds_by_yield()
    
    bond_col1, bond_col2 = st.columns(2)
    
    with bond_col1:
        st.markdown("📈 **Top 10 Highest Yielding Bonds**")
        top_yield_chart = active_bonds.head(10)[['ISSUER_NAME', 'CURRENT_YIELD']].copy()
        top_yield_chart = top_yield_chart.sort_values('CURRENT_YIELD', ascending=True)
        top_yield_chart = top_yield_chart.set_index('ISSUER_NAME')
        st.bar_chart(top_yield_chart['CURRENT_YIELD'], use_container_width=True, height=250)
        
        top_yield = active_bonds.head(10)[['TICKER', 'ISSUER_NAME', 'CURRENT_YIELD', 'MATURITY_DATE', 'CREDIT_RATING']].copy()
        top_yield['CURRENT_YIELD'] = top_yield['CURRENT_YIELD'].apply(lambda x: f"{x:.2f}%")
        top_yield['MATURITY_DATE'] = top_yield['MATURITY_DATE'].astype(str).str[:10]
        top_yield = top_yield.rename(columns={
            'TICKER': 'Ticker',
            'ISSUER_NAME': 'Issuer',
            'CURRENT_YIELD': 'Yield',
            'MATURITY_DATE': 'Maturity',
            'CREDIT_RATING': 'Rating'
        })
        st.dataframe(top_yield, use_container_width=True, height=250)
    
    with bond_col2:
        st.markdown("📉 **Bottom 10 Lowest Yielding Bonds**")
        bottom_yield_chart = active_bonds.tail(10)[['ISSUER_NAME', 'CURRENT_YIELD']].copy()
        bottom_yield_chart = bottom_yield_chart.sort_values('CURRENT_YIELD', ascending=True)
        bottom_yield_chart = bottom_yield_chart.set_index('ISSUER_NAME')
        st.bar_chart(bottom_yield_chart['CURRENT_YIELD'], use_container_width=True, height=250)
        
        bottom_yield = active_bonds.tail(10)[['TICKER', 'ISSUER_NAME', 'CURRENT_YIELD', 'MATURITY_DATE', 'CREDIT_RATING']].copy()
        bottom_yield['CURRENT_YIELD'] = bottom_yield['CURRENT_YIELD'].apply(lambda x: f"{x:.2f}%")
        bottom_yield['MATURITY_DATE'] = bottom_yield['MATURITY_DATE'].astype(str).str[:10]
        bottom_yield = bottom_yield.rename(columns={
            'TICKER': 'Ticker',
            'ISSUER_NAME': 'Issuer',
            'CURRENT_YIELD': 'Yield',
            'MATURITY_DATE': 'Maturity',
            'CREDIT_RATING': 'Rating'
        })
        st.dataframe(bottom_yield, use_container_width=True, height=250)

with tab2:
    # Security selector
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Create display options with TICKER - COMPANY_NAME format
        symbol_display_options = ["All Securities"] + [
            f"{row['SYMBOL']} - {row['SECURITY_NAME']}" 
            for _, row in securities.iterrows()
        ]
        selected_display = st.selectbox(
            "Select Security",
            options=symbol_display_options,
            index=0
        )
        # Extract just the symbol from the selection
        if selected_display == "All Securities":
            selected_symbol = "All Securities"
        else:
            selected_symbol = selected_display.split(" - ")[0]
    
    # Display security info if one is selected
    if selected_symbol and selected_symbol != "All Securities":
        security_info = securities[securities['SYMBOL'] == selected_symbol].iloc[0]
        
        with col2:
            st.markdown(f"""
            <div class="info-card">
                <h3 style="margin:0; color: #0d9488;">{security_info['SECURITY_NAME']}</h3>
                <p style="margin: 0.5rem 0 0 0; font-family: 'JetBrains Mono', monospace; color: #475569;">
                    {security_info['GICS_SECTOR']} • {security_info['GICS_SUB_INDUSTRY']}<br/>
                    📍 {security_info['HEADQUARTERS']}
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        # Security-specific metrics
        symbol_summary = portfolio_summary[portfolio_summary['SYMBOL'] == selected_symbol]
        if not symbol_summary.empty:
            row = symbol_summary.iloc[0]
            scol1, scol2, scol3, scol4 = st.columns(4)
            with scol1:
                st.metric("Trades", int(row['TRADE_COUNT']))
            with scol2:
                st.metric("Net Position", f"{row['NET_POSITION']:,.0f}")
            with scol3:
                st.metric("Avg Price", f"${row['AVG_PRICE']:.2f}")
            with scol4:
                gain_loss = row['SELL_VALUE'] - row['BUY_VALUE']
                gl_icon = "🟢" if gain_loss >= 0 else "🔴"
                st.metric("Realized P&L", f"{gl_icon} ${gain_loss:,.0f}")
    
    # Trades table
    st.subheader("📋 Trade History")
    trades = load_trades(selected_symbol)
    
    if not trades.empty:
        # Format the dataframe for display
        display_df = trades.copy()
        display_df['TOTAL_VALUE'] = display_df['TOTAL_VALUE'].apply(lambda x: f"${x:,.2f}")
        display_df['PRICE'] = display_df['PRICE'].apply(lambda x: f"${x:.2f}")
        display_df['QUANTITY'] = display_df['QUANTITY'].apply(lambda x: f"{x:,.0f}")
        
        # Rename columns for display
        display_df = display_df.rename(columns={
            'TRADE_ID': 'ID',
            'SYMBOL': 'Symbol',
            'SECURITY_NAME': 'Security',
            'TRADE_DATE': 'Date',
            'TRADE_TYPE': 'Type',
            'QUANTITY': 'Qty',
            'PRICE': 'Price',
            'TOTAL_VALUE': 'Total Value'
        })
        
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No trades found for the selected criteria.")

with tab3:
    st.subheader("📈 Sector Analysis")
    
    # Sector metrics
    if not sector_data.empty:
        # Calculate P&L by sector
        sector_display = sector_data.copy()
        sector_display['REALIZED_PNL'] = sector_display['SELL_VALUE'] - sector_display['BUY_VALUE']
        
        # Bar chart for sector values
        st.bar_chart(
            sector_display.set_index('GICS_SECTOR')['TOTAL_VALUE'],
            use_container_width=True
        )
        
        # Sector table
        st.markdown("#### 📊 Sector Breakdown")
        sector_display['TOTAL_VALUE'] = sector_display['TOTAL_VALUE'].apply(lambda x: f"${x:,.0f}")
        sector_display['BUY_VALUE'] = sector_display['BUY_VALUE'].apply(lambda x: f"${x:,.0f}")
        sector_display['SELL_VALUE'] = sector_display['SELL_VALUE'].apply(lambda x: f"${x:,.0f}")
        sector_display['REALIZED_PNL'] = sector_display['REALIZED_PNL'].apply(lambda x: f"${x:,.0f}")
        sector_display = sector_display.rename(columns={
            'GICS_SECTOR': 'Sector',
            'SECURITIES_TRADED': 'Securities',
            'TOTAL_VALUE': 'Total Value',
            'BUY_VALUE': 'Buy Value',
            'SELL_VALUE': 'Sell Value',
            'REALIZED_PNL': 'Realized P&L'
        })
        
        st.dataframe(sector_display, use_container_width=True)

with tab4:
    st.subheader("🔗 Trade Matching to NYSE Security Master")
    
    match_summary = get_trade_match_summary()
    trades_with_nyse = get_trades_with_nyse_master()
    
    # Match summary metrics
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
    
    # Filter options
    st.markdown("---")
    filter_col1, filter_col2 = st.columns([1, 3])
    
    with filter_col1:
        match_filter = st.selectbox(
            "Filter by Match Status",
            options=["All", "Matched", "Unmatched"],
            index=0
        )
    
    # Apply filter
    if match_filter != "All":
        filtered_trades = trades_with_nyse[trades_with_nyse['MATCH_STATUS'] == match_filter]
    else:
        filtered_trades = trades_with_nyse
    
    st.markdown(f"*Showing {len(filtered_trades):,} trades*")
    
    # Display matched trades with NYSE data
    if not filtered_trades.empty:
        display_matched = filtered_trades.copy()
        display_matched['TOTAL_VALUE'] = display_matched['TOTAL_VALUE'].apply(lambda x: f"${x:,.2f}")
        display_matched['PRICE'] = display_matched['PRICE'].apply(lambda x: f"${x:.2f}")
        display_matched['QUANTITY'] = display_matched['QUANTITY'].apply(lambda x: f"{x:,.0f}")
        
        display_matched = display_matched.rename(columns={
            'TRADE_ID': 'ID',
            'SYMBOL': 'Symbol',
            'NYSE_COMPANY_NAME': 'NYSE Company',
            'FIGI': 'Bloomberg FIGI',
            'TRADE_DATE': 'Date',
            'TRADE_TYPE': 'Type',
            'QUANTITY': 'Qty',
            'PRICE': 'Price',
            'TOTAL_VALUE': 'Total Value',
            'MATCH_STATUS': 'Status'
        })
        
        st.dataframe(display_matched, use_container_width=True, height=400)
    else:
        st.info("No trades found for the selected filter.")
    
    # Show unmatched symbols for investigation
    if unmatched_symbols > 0:
        with st.expander(f"⚠️ View {unmatched_symbols} Unmatched Symbols"):
            unmatched_list = trades_with_nyse[trades_with_nyse['MATCH_STATUS'] == 'Unmatched']['SYMBOL'].unique()
            st.write(", ".join(sorted(unmatched_list)))
            st.caption("These symbols exist in trades but are not found in the NYSE Securities Master.")

with tab5:
    st.subheader("📉 Bond Trading Activity")
    
    bond_trades = load_bond_trades()
    bond_trade_summary = get_bond_trade_summary()
    counterparty_summary = get_bond_trades_by_counterparty()
    
    # Top metrics
    btcol1, btcol2, btcol3, btcol4 = st.columns(4)
    
    total_bond_trades = len(bond_trades)
    total_bond_value = bond_trades['TOTAL_VALUE'].sum()
    buy_trades = bond_trades[bond_trades['TRADE_TYPE'] == 'BUY']
    sell_trades = bond_trades[bond_trades['TRADE_TYPE'] == 'SELL']
    
    with btcol1:
        st.metric("Total Bond Trades", f"{total_bond_trades:,}")
    with btcol2:
        st.metric("Total Trade Value", f"${total_bond_value/1e9:.2f}B")
    with btcol3:
        st.metric("Buy Trades", f"{len(buy_trades):,}", f"${buy_trades['TOTAL_VALUE'].sum()/1e9:.2f}B")
    with btcol4:
        st.metric("Sell Trades", f"{len(sell_trades):,}", f"${sell_trades['TOTAL_VALUE'].sum()/1e9:.2f}B")
    
    st.markdown("---")
    
    # Filters
    bt_filter_col1, bt_filter_col2, bt_filter_col3 = st.columns(3)
    
    with bt_filter_col1:
        trade_type_filter = st.selectbox(
            "Trade Type",
            options=["All", "BUY", "SELL"],
            index=0,
            key="bond_trade_type"
        )
    
    with bt_filter_col2:
        counterparty_filter = st.selectbox(
            "Counterparty",
            options=["All Counterparties"] + sorted(bond_trades['COUNTERPARTY'].unique().tolist()),
            index=0,
            key="bond_counterparty"
        )
    
    with bt_filter_col3:
        bond_search = st.text_input("Search Issuer/CUSIP", placeholder="e.g., Apple, 037833", key="bond_trade_search")
    
    # Apply filters
    filtered_bond_trades = bond_trades.copy()
    if trade_type_filter != "All":
        filtered_bond_trades = filtered_bond_trades[filtered_bond_trades['TRADE_TYPE'] == trade_type_filter]
    if counterparty_filter != "All Counterparties":
        filtered_bond_trades = filtered_bond_trades[filtered_bond_trades['COUNTERPARTY'] == counterparty_filter]
    if bond_search:
        filtered_bond_trades = filtered_bond_trades[
            filtered_bond_trades['ISSUER_NAME'].str.contains(bond_search, case=False, na=False) |
            filtered_bond_trades['CUSIP'].str.contains(bond_search.upper(), na=False) |
            filtered_bond_trades['TICKER'].str.contains(bond_search.upper(), na=False)
        ]
    
    st.markdown(f"*Showing {len(filtered_bond_trades):,} trades*")
    
    # Display trades table
    if not filtered_bond_trades.empty:
        display_bt = filtered_bond_trades[['TRADE_ID', 'CUSIP', 'TICKER', 'ISSUER_NAME', 'CREDIT_RATING',
                                           'TRADE_DATE', 'TRADE_TYPE', 'QUANTITY', 'PRICE', 
                                           'YIELD_AT_TRADE', 'TOTAL_VALUE', 'COUNTERPARTY']].copy()
        display_bt['TOTAL_VALUE'] = display_bt['TOTAL_VALUE'].apply(lambda x: f"${x:,.0f}")
        display_bt['PRICE'] = display_bt['PRICE'].apply(lambda x: f"{x:.4f}")
        display_bt['YIELD_AT_TRADE'] = display_bt['YIELD_AT_TRADE'].apply(lambda x: f"{x:.3f}%" if pd.notna(x) else "N/A")
        display_bt['QUANTITY'] = display_bt['QUANTITY'].apply(lambda x: f"{x:,}")
        display_bt = display_bt.rename(columns={
            'TRADE_ID': 'ID',
            'CUSIP': 'CUSIP',
            'TICKER': 'Ticker',
            'ISSUER_NAME': 'Issuer',
            'CREDIT_RATING': 'Rating',
            'TRADE_DATE': 'Date',
            'TRADE_TYPE': 'Type',
            'QUANTITY': 'Qty',
            'PRICE': 'Price',
            'YIELD_AT_TRADE': 'Yield',
            'TOTAL_VALUE': 'Value',
            'COUNTERPARTY': 'Counterparty'
        })
        st.dataframe(display_bt, use_container_width=True, height=350)
    
    # Summary sections
    st.markdown("---")
    summary_col1, summary_col2 = st.columns(2)
    
    with summary_col1:
        st.markdown("#### 🏢 Top Issuers by Trade Volume")
        top_issuers = bond_trade_summary.head(10)[['TICKER', 'ISSUER_NAME', 'TRADE_COUNT', 'BUY_VALUE', 'SELL_VALUE']].copy()
        top_issuers['BUY_VALUE'] = top_issuers['BUY_VALUE'].apply(lambda x: f"${x/1e6:.1f}M")
        top_issuers['SELL_VALUE'] = top_issuers['SELL_VALUE'].apply(lambda x: f"${x/1e6:.1f}M")
        top_issuers = top_issuers.rename(columns={
            'TICKER': 'Ticker',
            'ISSUER_NAME': 'Issuer',
            'TRADE_COUNT': 'Trades',
            'BUY_VALUE': 'Buys',
            'SELL_VALUE': 'Sells'
        })
        st.dataframe(top_issuers, use_container_width=True)
    
    with summary_col2:
        st.markdown("#### 🤝 Trades by Counterparty")
        cp_display = counterparty_summary.copy()
        cp_display['TOTAL_VALUE'] = cp_display['TOTAL_VALUE'].apply(lambda x: f"${x/1e6:.1f}M")
        cp_display['AVG_PRICE'] = cp_display['AVG_PRICE'].apply(lambda x: f"{x:.2f}")
        cp_display = cp_display.rename(columns={
            'COUNTERPARTY': 'Counterparty',
            'TRADE_COUNT': 'Trades',
            'TOTAL_VALUE': 'Total Value',
            'AVG_PRICE': 'Avg Price'
        })
        st.dataframe(cp_display, use_container_width=True)

with tab6:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1e40af 0%, #3b82f6 50%, #60a5fa 100%); 
                border-radius: 10px; padding: 0.5rem 1rem; margin-bottom: 1rem;">
        <h4 style="color: white; margin: 0; font-weight: 600;">Security Master Data Entry</h4>
    </div>
    """, unsafe_allow_html=True)
    
    import json
    
    def lookup_isin_external(isin_code):
        """Lookup security information via External Function (OpenFIGI API)"""
        result = session.sql(f"""
            SELECT SECURITY_MASTER_DB.GOLDEN_RECORD.LOOKUP_ISIN_EXTERNAL('{isin_code}') as RESULT
        """).to_pandas()
        
        if not result.empty:
            raw_result = result.iloc[0]['RESULT']
            if isinstance(raw_result, str):
                return json.loads(raw_result)
            return raw_result
        return {'success': False, 'error': 'Failed to call external function'}
    
    st.markdown("**🌐 ISIN Lookup (External API)**")
    lookup_col1, lookup_col2, lookup_col3 = st.columns([1, 1, 2])
    
    with lookup_col1:
        isin_lookup = st.text_input("ISIN", placeholder="e.g., US0378331005", max_chars=12, key="isin_lookup_input", label_visibility="collapsed")
    
    with lookup_col2:
        lookup_clicked = st.button("🔎 Lookup", key="lookup_btn", use_container_width=True)
    
    if lookup_clicked and isin_lookup:
        with st.spinner("Calling OpenFIGI API..."):
            result = lookup_isin_external(isin_lookup.upper().strip())
            
            if result and result.get('success'):
                st.success("✅ Security found via OpenFIGI!")
                
                st.markdown(f"**Source:** OpenFIGI External API")
                result_col1, result_col2, result_col3 = st.columns(3)
                with result_col1:
                    st.markdown(f"**Name:** {result.get('name', 'N/A')}")
                    st.markdown(f"**Ticker:** {result.get('ticker', 'N/A')}")
                with result_col2:
                    st.markdown(f"**Exchange:** {result.get('exchange', 'N/A')}")
                    st.markdown(f"**Type:** {result.get('security_type', 'N/A')}")
                with result_col3:
                    st.markdown(f"**Market Sector:** {result.get('market_sector', 'N/A')}")
                    st.markdown(f"**FIGI:** {result.get('figi', 'N/A')}")
                
                all_results = result.get('all_results', [])
                if len(all_results) > 1:
                    with st.expander(f"📋 View all {len(all_results)} listings"):
                        for i, listing in enumerate(all_results):
                            st.markdown(f"**{i+1}. {listing.get('name', 'N/A')}** - {listing.get('ticker', 'N/A')} ({listing.get('exchCode', 'N/A')})")
            else:
                error_msg = result.get('error', 'Unknown error') if result else 'No response'
                st.warning(f"⚠️ {error_msg}")
    
    st.markdown("---")
    
    @st.cache_data(ttl=60)
    def load_golden_record():
        return session.sql("""
            SELECT * FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
            ORDER BY GLOBAL_SECURITY_ID DESC
            LIMIT 100
        """).to_pandas()
    
    @st.cache_data(ttl=60)
    def load_equities_dropdown():
        return session.sql("""
            SELECT GLOBAL_SECURITY_ID, PRIMARY_TICKER, ISSUER, ASSET_CLASS, PRIMARY_EXCHANGE
            FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
            WHERE ASSET_CLASS = 'Equity'
            ORDER BY PRIMARY_TICKER
        """).to_pandas()
    
    @st.cache_data(ttl=60)
    def load_bonds_dropdown():
        return session.sql("""
            SELECT BOND_ID, ISSUER_NAME, COUPON_RATE, MATURITY_DATE, CURRENCY, CREDIT_RATING
            FROM SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS
            ORDER BY ISSUER_NAME
        """).to_pandas()
    
    def get_next_gsid():
        result = session.sql("SELECT 'GSID_' || SECURITY_MASTER_DB.GOLDEN_RECORD.GSID_SEQ.NEXTVAL AS NEXT_ID").to_pandas()
        return result['NEXT_ID'].iloc[0]
    
    def validate_security(exchange, isin, cusip, sedol):
        errors = []
        is_us_security = isin and isin.startswith('US')
        is_lseg = exchange == 'LSEG'
        
        if is_lseg and (not sedol or sedol.strip() == ''):
            errors.append("SEDOL is required for LSEG traded securities")
        
        if is_us_security and (not cusip or cusip.strip() == ''):
            errors.append("CUSIP is required for US-based securities (ISIN starting with 'US')")
        
        return errors
    
    def insert_security(issuer, asset_class, ticker, exchange, isin, cusip, sedol, currency, status, golden_source):
        gsid = get_next_gsid()
        lineage_id = f"LIN-{gsid}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
        
        session.sql(f"""
            INSERT INTO SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE 
            (GLOBAL_SECURITY_ID, ISSUER, ASSET_CLASS, PRIMARY_TICKER, PRIMARY_EXCHANGE, 
             ISIN, CUSIP, SEDOL, CURRENCY, STATUS, GOLDEN_SOURCE, LAST_VALIDATED,
             LINEAGE_ID, CREATED_BY, CREATED_AT, LAST_MODIFIED_BY)
            VALUES (
                '{gsid}',
                '{issuer.replace("'", "''")}',
                '{asset_class}',
                '{ticker}',
                '{exchange}',
                {f"'{isin}'" if isin else 'NULL'},
                {f"'{cusip}'" if cusip else 'NULL'},
                {f"'{sedol}'" if sedol else 'NULL'},
                '{currency}',
                '{status}',
                '{golden_source.replace("'", "''")}',
                CURRENT_TIMESTAMP(),
                '{lineage_id}',
                'CURRENT_USER',
                CURRENT_TIMESTAMP(),
                'CURRENT_USER'
            )
        """).collect()
        
        new_values = {
            'issuer': issuer, 'asset_class': asset_class, 'ticker': ticker,
            'exchange': exchange, 'isin': isin, 'cusip': cusip, 'sedol': sedol,
            'currency': currency, 'status': status, 'golden_source': golden_source
        }
        log_security_change(gsid, 'INSERT', None, new_values, 'New security created', 'CURRENT_USER')
        
        return gsid
    
    browse_col1, browse_col2 = st.columns([1, 3])
    
    with browse_col1:
        security_type = st.selectbox(
            "Type",
            options=["Equities", "Bonds"],
            key="security_type_filter"
        )
    
    with browse_col2:
        if security_type == "Equities":
            equities_data = load_equities_dropdown()
            security_options = ["-- Select an equity to view --"] + [
                f"{row['PRIMARY_TICKER']} - {row['ISSUER'][:50]}"
                for _, row in equities_data.iterrows()
            ]
            selected_security = st.selectbox(
                "Security",
                options=security_options,
                key="browse_equity"
            )
        else:
            bonds_data = load_bonds_dropdown()
            security_options = ["-- Select a bond to view --"] + [
                f"{row['BOND_ID']} - {row['ISSUER_NAME'][:40]} (Maturity: {str(row['MATURITY_DATE'])[:10]})"
                for _, row in bonds_data.iterrows()
            ]
            selected_security = st.selectbox(
                "Security",
                options=security_options,
                key="browse_bond"
            )
    
    if 'edit_mode' not in st.session_state:
        st.session_state.edit_mode = False
    
    def log_security_change(gsid, action, old_rec, new_values, edit_reason, user):
        """Log changes to security master history table"""
        lineage_id = f"LIN-{gsid}-{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
        
        old_lineage = old_rec.get('LINEAGE_ID', '') if old_rec is not None else ''
        lineage_path = f"{old_lineage} -> {lineage_id}" if old_lineage else lineage_id
        
        session.sql(f"""
            INSERT INTO SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY (
                GLOBAL_SECURITY_ID, ACTION,
                ISSUER_BEFORE, ISSUER_AFTER,
                ASSET_CLASS_BEFORE, ASSET_CLASS_AFTER,
                PRIMARY_TICKER_BEFORE, PRIMARY_TICKER_AFTER,
                PRIMARY_EXCHANGE_BEFORE, PRIMARY_EXCHANGE_AFTER,
                ISIN_BEFORE, ISIN_AFTER,
                CUSIP_BEFORE, CUSIP_AFTER,
                SEDOL_BEFORE, SEDOL_AFTER,
                CURRENCY_BEFORE, CURRENCY_AFTER,
                STATUS_BEFORE, STATUS_AFTER,
                EDIT_REASON, CHANGED_BY, CHANGED_AT,
                SOURCE_SYSTEM, LINEAGE_ID, LINEAGE_PARENT_ID, LINEAGE_PATH
            ) VALUES (
                '{gsid}', '{action}',
                {f"'{str(old_rec['ISSUER']).replace(chr(39), chr(39)+chr(39))}'" if old_rec is not None else 'NULL'},
                {f"'{new_values['issuer'].replace(chr(39), chr(39)+chr(39))}'" if new_values.get('issuer') else 'NULL'},
                {f"'{old_rec['ASSET_CLASS']}'" if old_rec is not None else 'NULL'},
                {f"'{new_values.get('asset_class', '')}'" if new_values.get('asset_class') else 'NULL'},
                {f"'{old_rec['PRIMARY_TICKER']}'" if old_rec is not None else 'NULL'},
                {f"'{new_values.get('ticker', '')}'" if new_values.get('ticker') else 'NULL'},
                {f"'{old_rec['PRIMARY_EXCHANGE']}'" if old_rec is not None else 'NULL'},
                {f"'{new_values.get('exchange', '')}'" if new_values.get('exchange') else 'NULL'},
                {f"'{old_rec['ISIN']}'" if old_rec is not None and old_rec['ISIN'] else 'NULL'},
                {f"'{new_values.get('isin', '')}'" if new_values.get('isin') else 'NULL'},
                {f"'{old_rec['CUSIP']}'" if old_rec is not None and old_rec['CUSIP'] else 'NULL'},
                {f"'{new_values.get('cusip', '')}'" if new_values.get('cusip') else 'NULL'},
                {f"'{old_rec['SEDOL']}'" if old_rec is not None and old_rec['SEDOL'] else 'NULL'},
                {f"'{new_values.get('sedol', '')}'" if new_values.get('sedol') else 'NULL'},
                {f"'{old_rec['CURRENCY']}'" if old_rec is not None else 'NULL'},
                {f"'{new_values.get('currency', '')}'" if new_values.get('currency') else 'NULL'},
                {f"'{old_rec['STATUS']}'" if old_rec is not None else 'NULL'},
                {f"'{new_values.get('status', '')}'" if new_values.get('status') else 'NULL'},
                '{edit_reason.replace(chr(39), chr(39)+chr(39))}',
                '{user}',
                CURRENT_TIMESTAMP(),
                'Security Master EDM',
                '{lineage_id}',
                {f"'{old_lineage}'" if old_lineage else 'NULL'},
                '{lineage_path}'
            )
        """).collect()
        return lineage_id
    
    def update_security_with_history(gsid, old_rec, new_values, edit_reason, user):
        lineage_id = log_security_change(gsid, 'UPDATE', old_rec, new_values, edit_reason, user)
        
        session.sql(f"""
            UPDATE SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE 
            SET ISSUER = '{new_values['issuer'].replace("'", "''")}',
                ASSET_CLASS = '{new_values['asset_class']}',
                PRIMARY_TICKER = '{new_values['ticker']}',
                PRIMARY_EXCHANGE = '{new_values['exchange']}',
                ISIN = {f"'{new_values['isin']}'" if new_values.get('isin') else 'NULL'},
                CUSIP = {f"'{new_values['cusip']}'" if new_values.get('cusip') else 'NULL'},
                SEDOL = {f"'{new_values['sedol']}'" if new_values.get('sedol') else 'NULL'},
                CURRENCY = '{new_values['currency']}',
                STATUS = '{new_values['status']}',
                GOLDEN_SOURCE = '{new_values['golden_source'].replace("'", "''")}',
                LAST_VALIDATED = CURRENT_TIMESTAMP(),
                LINEAGE_ID = '{lineage_id}',
                LAST_MODIFIED_BY = '{user}'
            WHERE GLOBAL_SECURITY_ID = '{gsid}'
        """).collect()
    
    if security_type == "Equities" and selected_security != "-- Select an equity to view --":
        selected_ticker = selected_security.split(" - ")[0]
        selected_record = session.sql(f"""
            SELECT * FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
            WHERE PRIMARY_TICKER = '{selected_ticker}'
            LIMIT 1
        """).to_pandas()
        
        if not selected_record.empty:
            rec = selected_record.iloc[0]
            st.markdown("---")
            
            if not st.session_state.edit_mode:
                view_col1, view_col2, view_col3, view_col4, view_col5 = st.columns([1, 1, 1, 1, 0.5])
                with view_col1:
                    st.markdown(f"**GSID:** {rec['GLOBAL_SECURITY_ID']}")
                    st.markdown(f"**Ticker:** {rec['PRIMARY_TICKER']}")
                    st.markdown(f"**Issuer:** {rec['ISSUER'][:40]}")
                with view_col2:
                    st.markdown(f"**Asset Class:** {rec['ASSET_CLASS']}")
                    st.markdown(f"**Exchange:** {rec['PRIMARY_EXCHANGE']}")
                    st.markdown(f"**Currency:** {rec['CURRENCY']}")
                with view_col3:
                    st.markdown(f"**ISIN:** {rec['ISIN'] or 'N/A'}")
                    st.markdown(f"**CUSIP:** {rec['CUSIP'] or 'N/A'}")
                    st.markdown(f"**SEDOL:** {rec['SEDOL'] or 'N/A'}")
                with view_col4:
                    st.markdown(f"**Status:** {rec['STATUS']}")
                    st.markdown(f"**Source:** {str(rec['GOLDEN_SOURCE'])[:25]}")
                with view_col5:
                    if st.button("Edit", key="edit_equity_btn", use_container_width=True):
                        st.session_state.edit_mode = True
                        st.experimental_rerun()
            else:
                st.markdown("**Edit Security**")
                with st.form("edit_security_form"):
                    edit_col1, edit_col2 = st.columns([2, 1])
                    with edit_col1:
                        edit_issuer = st.text_input("Issuer Name", value=rec['ISSUER'])
                    with edit_col2:
                        edit_ticker = st.text_input("Primary Ticker", value=rec['PRIMARY_TICKER'])
                    
                    edit_col3, edit_col4, edit_col5, edit_col6 = st.columns(4)
                    with edit_col3:
                        asset_options = ["Equity", "Fixed Income", "ETF", "Derivative", "Fund"]
                        edit_asset_class = st.selectbox("Asset Class", asset_options, index=asset_options.index(rec['ASSET_CLASS']) if rec['ASSET_CLASS'] in asset_options else 0)
                    with edit_col4:
                        exchange_options = ["NYSE", "NASDAQ", "LSEG", "EUREX", "TSE", "HKEX", "ASX"]
                        edit_exchange = st.selectbox("Exchange", exchange_options, index=exchange_options.index(rec['PRIMARY_EXCHANGE']) if rec['PRIMARY_EXCHANGE'] in exchange_options else 0)
                    with edit_col5:
                        currency_options = ["USD", "GBP", "EUR", "JPY", "HKD", "AUD", "CAD", "CHF"]
                        edit_currency = st.selectbox("Currency", currency_options, index=currency_options.index(rec['CURRENCY']) if rec['CURRENCY'] in currency_options else 0)
                    with edit_col6:
                        status_options = ["Pre-Issue", "Active", "Matured", "Defaulted", "Retired"]
                        edit_status = st.selectbox("Status", status_options, index=status_options.index(rec['STATUS']) if rec['STATUS'] in status_options else 1)
                    
                    edit_col7, edit_col8, edit_col9 = st.columns(3)
                    with edit_col7:
                        edit_isin = st.text_input("ISIN", value=rec['ISIN'] or '', max_chars=12)
                    with edit_col8:
                        edit_cusip = st.text_input("CUSIP", value=rec['CUSIP'] or '', max_chars=9)
                    with edit_col9:
                        edit_sedol = st.text_input("SEDOL", value=rec['SEDOL'] or '', max_chars=7)
                    
                    edit_golden_source = st.text_input("Golden Source", value=str(rec['GOLDEN_SOURCE']) if rec['GOLDEN_SOURCE'] else '')
                    
                    edit_reason = st.text_area("Edit Reason *", placeholder="Please provide reason for this change (required)", help="Required: Describe why this security is being updated")
                    
                    btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
                    with btn_col1:
                        save_btn = st.form_submit_button("Save", type="primary", use_container_width=True)
                    with btn_col2:
                        cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
                    
                    if save_btn:
                        if not edit_reason or not edit_reason.strip():
                            st.error("Edit Reason is required")
                        else:
                            validation_errors = validate_security(edit_exchange, edit_isin, edit_cusip, edit_sedol)
                            if validation_errors:
                                for error in validation_errors:
                                    st.error(f"Validation Error: {error}")
                            else:
                                try:
                                    new_values = {
                                        'issuer': edit_issuer,
                                        'asset_class': edit_asset_class,
                                        'ticker': edit_ticker,
                                        'exchange': edit_exchange,
                                        'isin': edit_isin if edit_isin else None,
                                        'cusip': edit_cusip if edit_cusip else None,
                                        'sedol': edit_sedol if edit_sedol else None,
                                        'currency': edit_currency,
                                        'status': edit_status,
                                        'golden_source': edit_golden_source
                                    }
                                    update_security_with_history(
                                        rec['GLOBAL_SECURITY_ID'], rec, new_values, edit_reason, 'CURRENT_USER'
                                    )
                                    st.success("Security updated successfully!")
                                    st.session_state.edit_mode = False
                                    st.cache_data.clear()
                                    st.experimental_rerun()
                                except Exception as e:
                                    st.error(f"Error updating security: {str(e)}")
                    
                    if cancel_btn:
                        st.session_state.edit_mode = False
                        st.experimental_rerun()
    
    elif security_type == "Bonds" and selected_security != "-- Select a bond to view --":
        selected_bond_id = selected_security.split(" - ")[0]
        selected_bond = session.sql(f"""
            SELECT * FROM SECURITY_MASTER_DB.FIXED_INCOME.CORPORATE_BONDS
            WHERE BOND_ID = '{selected_bond_id}'
            LIMIT 1
        """).to_pandas()
        
        if not selected_bond.empty:
            bond = selected_bond.iloc[0]
            st.markdown("---")
            view_col1, view_col2, view_col3, view_col4 = st.columns(4)
            with view_col1:
                st.markdown(f"**Bond ID:** {bond['BOND_ID']}")
                st.markdown(f"**Issuer:** {bond['ISSUER_NAME'][:40]}")
                st.markdown(f"**ISIN:** {bond['ISIN'] or 'N/A'}")
            with view_col2:
                st.markdown(f"**Coupon Rate:** {bond['COUPON_RATE']}%")
                st.markdown(f"**Maturity Date:** {str(bond['MATURITY_DATE'])[:10]}")
                st.markdown(f"**Par Value:** ${bond['PAR_VALUE']:,.0f}")
            with view_col3:
                st.markdown(f"**Credit Rating:** {bond['CREDIT_RATING']}")
                st.markdown(f"**Currency:** {bond['CURRENCY']}")
                st.markdown(f"**Sector:** {bond['SECTOR']}")
            with view_col4:
                st.markdown(f"**Issue Date:** {str(bond['ISSUE_DATE'])[:10]}")
                st.markdown(f"**CUSIP:** {bond['CUSIP'] or 'N/A'}")
    
    st.markdown("---")
    st.subheader("Add New Security")
    
    st.markdown("**🌐 ISIN Lookup (External API)**")
    add_lookup_col1, add_lookup_col2, add_lookup_col3 = st.columns([1, 1, 2])
    
    with add_lookup_col1:
        add_isin_lookup = st.text_input("ISIN", placeholder="e.g., US0378331005", max_chars=12, key="add_isin_lookup_input", label_visibility="collapsed")
    
    with add_lookup_col2:
        add_lookup_clicked = st.button("🔎 Lookup", key="add_lookup_btn", use_container_width=True)
    
    if 'prefill_issuer' not in st.session_state:
        st.session_state.prefill_issuer = ''
    if 'prefill_ticker' not in st.session_state:
        st.session_state.prefill_ticker = ''
    if 'prefill_isin' not in st.session_state:
        st.session_state.prefill_isin = ''
    if 'prefill_exchange' not in st.session_state:
        st.session_state.prefill_exchange = ''
    if 'prefill_sector' not in st.session_state:
        st.session_state.prefill_sector = ''
    
    if add_lookup_clicked and add_isin_lookup:
        with st.spinner("Calling OpenFIGI API..."):
            result = lookup_isin_external(add_isin_lookup.upper().strip())
            
            if result and result.get('success'):
                st.success("✅ Security found! Form populated below.")
                st.session_state.prefill_issuer = result.get('name', '')
                st.session_state.prefill_ticker = result.get('ticker', '')
                st.session_state.prefill_isin = add_isin_lookup.upper().strip()
                st.session_state.prefill_exchange = result.get('exchange', '')
                st.session_state.prefill_sector = result.get('market_sector', '')
                st.experimental_rerun()
            else:
                error_msg = result.get('error', 'Unknown error') if result else 'No response'
                st.warning(f"⚠️ {error_msg}")
    
    with st.form("security_entry_form", clear_on_submit=True):
        form_row1_col1, form_row1_col2 = st.columns([2, 1])
        
        with form_row1_col1:
            issuer = st.text_input("Issuer Name *", value=st.session_state.prefill_issuer, placeholder="e.g., Apple Inc.")
        with form_row1_col2:
            ticker = st.text_input("Primary Ticker *", value=st.session_state.prefill_ticker, placeholder="e.g., AAPL")
        
        form_row2_col1, form_row2_col2, form_row2_col3, form_row2_col4 = st.columns(4)
        
        exchange_options = ["NYSE", "NASDAQ", "LSEG", "EUREX", "TSE", "HKEX", "ASX"]
        prefill_exch_idx = 0
        if st.session_state.prefill_exchange in exchange_options:
            prefill_exch_idx = exchange_options.index(st.session_state.prefill_exchange)
        elif st.session_state.prefill_exchange == 'US':
            prefill_exch_idx = exchange_options.index('NYSE')
        
        asset_options = ["Equity", "Fixed Income", "ETF", "Derivative", "Fund"]
        prefill_asset_idx = 0
        if st.session_state.prefill_sector == 'Equity':
            prefill_asset_idx = 0
        elif st.session_state.prefill_sector in ['Corp', 'Govt', 'Muni']:
            prefill_asset_idx = 1
        
        with form_row2_col1:
            asset_class = st.selectbox("Asset Class *", asset_options, index=prefill_asset_idx)
        with form_row2_col2:
            exchange = st.selectbox("Primary Exchange *", exchange_options, index=prefill_exch_idx)
        with form_row2_col3:
            currency = st.selectbox("Currency *", ["USD", "GBP", "EUR", "JPY", "HKD", "AUD", "CAD", "CHF"])
        with form_row2_col4:
            status = st.selectbox("Status", ["Pre-Issue", "Active", "Matured", "Defaulted", "Retired"], index=1)
        
        st.markdown("##### Security Identifiers")
        id_col1, id_col2, id_col3 = st.columns(3)
        
        with id_col1:
            isin = st.text_input("ISIN", value=st.session_state.prefill_isin, placeholder="e.g., US0378331005", max_chars=12, 
                                help="12-character International Securities Identification Number")
        with id_col2:
            cusip = st.text_input("CUSIP", placeholder="e.g., 037833100", max_chars=9,
                                 help="9-character CUSIP (required for US securities)")
        with id_col3:
            sedol = st.text_input("SEDOL", placeholder="e.g., B0SWJX3", max_chars=7,
                                 help="7-character SEDOL (required for LSEG securities)")
        
        golden_source = st.text_input("Golden Source", value="Bloomberg (primary), Refinitiv (secondary)")
        
        st.markdown("""
        <div style="background: #fef3c7; border: 1px solid #f59e0b; border-radius: 8px; padding: 0.75rem; margin: 1rem 0;">
            <strong style="color: #92400e;">⚠️ Business Rules:</strong>
            <ul style="margin: 0.5rem 0 0 1rem; color: #92400e; font-size: 0.9rem;">
                <li>ISIN is <strong>required</strong> and must be unique</li>
                <li>LSEG traded securities <strong>must</strong> have a SEDOL</li>
                <li>US-based securities (ISIN starting with 'US') <strong>must</strong> have a CUSIP</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        submit_col1, submit_col2, submit_col3 = st.columns([1, 2, 1])
        with submit_col2:
            submitted = st.form_submit_button("➕ Add Security", type="primary", use_container_width=True)
        
        if submitted:
            if not issuer or not ticker:
                st.error("Issuer Name and Primary Ticker are required fields")
            elif not isin:
                st.error("ISIN is required")
            else:
                existing_isin = session.sql(f"""
                    SELECT COUNT(*) as CNT FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
                    WHERE ISIN = '{isin.upper().strip()}'
                """).to_pandas()
                
                if existing_isin.iloc[0]['CNT'] > 0:
                    st.error("❌ Apologies but that security already exists with that ISIN")
                else:
                    validation_errors = validate_security(exchange, isin, cusip, sedol)
                    
                    if validation_errors:
                        for error in validation_errors:
                            st.error(f"Validation Error: {error}")
                    else:
                        try:
                            new_gsid = insert_security(
                                issuer, asset_class, ticker, exchange,
                                isin if isin else None,
                                cusip if cusip else None,
                                sedol if sedol else None,
                                currency, status, golden_source
                            )
                            st.success(f"✅ Security added successfully with ID: {new_gsid}")
                            st.session_state.prefill_issuer = ''
                            st.session_state.prefill_ticker = ''
                            st.session_state.prefill_isin = ''
                            st.session_state.prefill_exchange = ''
                            st.session_state.prefill_sector = ''
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Error adding security: {str(e)}")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    st.markdown("#### Data Quality Summary")
    quality_col1, quality_col2, quality_col3, quality_col4 = st.columns(4)
    
    @st.cache_data(ttl=60)
    def get_quality_metrics():
        return session.sql("""
            SELECT 
                COUNT(*) as TOTAL_RECORDS,
                COUNT(CASE WHEN PRIMARY_EXCHANGE = 'LSEG' AND (SEDOL IS NULL OR SEDOL = '') THEN 1 END) as LSEG_MISSING_SEDOL,
                COUNT(CASE WHEN ISIN LIKE 'US%' AND (CUSIP IS NULL OR CUSIP = '') THEN 1 END) as US_MISSING_CUSIP,
                COUNT(CASE WHEN ISIN IS NULL OR ISIN = '' THEN 1 END) as MISSING_ISIN
            FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
        """).to_pandas()
    
    quality = get_quality_metrics()
    
    with quality_col1:
        st.metric("Total Records", f"{quality['TOTAL_RECORDS'].iloc[0]:,}")
    with quality_col2:
        lseg_issues = quality['LSEG_MISSING_SEDOL'].iloc[0]
        st.metric("LSEG Missing SEDOL", f"{lseg_issues:,}", delta=None if lseg_issues == 0 else f"-{lseg_issues}", delta_color="inverse")
    with quality_col3:
        us_issues = quality['US_MISSING_CUSIP'].iloc[0]
        st.metric("US Missing CUSIP", f"{us_issues:,}", delta=None if us_issues == 0 else f"-{us_issues}", delta_color="inverse")
    with quality_col4:
        isin_issues = quality['MISSING_ISIN'].iloc[0]
        st.metric("Missing ISIN", f"{isin_issues:,}", delta=None if isin_issues == 0 else f"-{isin_issues}", delta_color="inverse")
    
    st.markdown("---")
    st.subheader("Recent Entries (Last 100)")
    
    golden_data = load_golden_record()
    
    if not golden_data.empty:
        display_golden = golden_data[['GLOBAL_SECURITY_ID', 'ISSUER', 'ASSET_CLASS', 
                                      'PRIMARY_TICKER', 'PRIMARY_EXCHANGE', 'ISIN', 
                                      'CUSIP', 'SEDOL', 'CURRENCY', 'STATUS']].copy()
        display_golden = display_golden.rename(columns={
            'GLOBAL_SECURITY_ID': 'GSID',
            'ISSUER': 'Issuer',
            'ASSET_CLASS': 'Class',
            'PRIMARY_TICKER': 'Ticker',
            'PRIMARY_EXCHANGE': 'Exchange',
            'CURRENCY': 'Ccy',
            'STATUS': 'Status'
        })
        st.dataframe(display_golden, use_container_width=True, height=400)
    else:
        st.info("No records found in the Golden Record table")

with tab8:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #0d9488 0%, #14b8a6 50%, #2dd4bf 100%); 
                border-radius: 16px; padding: 1.5rem 2rem; margin-bottom: 1.5rem;
                box-shadow: 0 4px 20px rgba(13, 148, 136, 0.3);">
        <h2 style="color: white; margin: 0; font-family: 'Outfit', sans-serif; font-weight: 700;">
            📋 Golden Record Trade Explorer
        </h2>
        <p style="color: rgba(255,255,255,0.9); margin: 0.5rem 0 0 0; font-size: 1rem;">
            Real-time view of all trades mapped to the Security Master Reference
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    @st.cache_data(ttl=60)
    def load_mapped_trades(security_type=None, trade_date_start=None, trade_date_end=None, 
                           sp500_filter=None, min_amount=None, max_amount=None, 
                           ticker_filter=None, exchange_filter=None):
        
        equity_query = """
            SELECT 
                'Equity' as SECURITY_TYPE,
                t.TRADE_ID,
                t.TRADE_DATE,
                t.TRADE_TYPE,
                t.SYMBOL as TICKER,
                g.ISSUER,
                g.GLOBAL_SECURITY_ID,
                g.PRIMARY_EXCHANGE as EXCHANGE,
                g.ISIN,
                g.CUSIP,
                g.SEDOL,
                t.QUANTITY,
                t.PRICE,
                t.TOTAL_VALUE as AMOUNT_USD,
                g.CURRENCY,
                CASE WHEN s.SYMBOL IS NOT NULL THEN 'Yes' ELSE 'No' END as IN_SP500
            FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
            LEFT JOIN SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE g 
                ON t.SYMBOL = g.PRIMARY_TICKER AND g.ASSET_CLASS = 'Equity'
            LEFT JOIN SECURITY_MASTER_DB.SECURITIES.SP500 s ON t.SYMBOL = s.SYMBOL
        """
        
        bond_query = """
            SELECT 
                'Bond' as SECURITY_TYPE,
                t.TRADE_ID,
                t.TRADE_DATE,
                t.TRADE_TYPE,
                g.PRIMARY_TICKER as TICKER,
                g.ISSUER,
                g.GLOBAL_SECURITY_ID,
                g.PRIMARY_EXCHANGE as EXCHANGE,
                g.ISIN,
                g.CUSIP,
                g.SEDOL,
                t.QUANTITY,
                t.PRICE,
                t.TOTAL_VALUE as AMOUNT_USD,
                g.CURRENCY,
                'No' as IN_SP500
            FROM SECURITY_TRADES_DB.TRADES.BOND_TRADES t
            LEFT JOIN SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE g 
                ON t.CUSIP = g.CUSIP AND g.ASSET_CLASS = 'Fixed Income'
        """
        
        if security_type == "Equity":
            full_query = equity_query
        elif security_type == "Bond":
            full_query = bond_query
        else:
            full_query = f"({equity_query}) UNION ALL ({bond_query})"
        
        full_query = f"SELECT * FROM ({full_query}) trades WHERE 1=1"
        
        if trade_date_start:
            full_query += f" AND TRADE_DATE >= '{trade_date_start}'"
        if trade_date_end:
            full_query += f" AND TRADE_DATE <= '{trade_date_end}'"
        if sp500_filter and sp500_filter != "All":
            full_query += f" AND IN_SP500 = '{sp500_filter}'"
        if min_amount:
            full_query += f" AND AMOUNT_USD >= {min_amount}"
        if max_amount:
            full_query += f" AND AMOUNT_USD <= {max_amount}"
        if ticker_filter:
            full_query += f" AND TICKER ILIKE '%{ticker_filter}%'"
        if exchange_filter and exchange_filter != "All Exchanges":
            full_query += f" AND EXCHANGE = '{exchange_filter}'"
        
        full_query += " ORDER BY TRADE_DATE DESC LIMIT 1000"
        
        return session.sql(full_query).to_pandas()
    
    @st.cache_data(ttl=300)
    def get_exchange_list():
        return session.sql("""
            SELECT DISTINCT PRIMARY_EXCHANGE 
            FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE 
            WHERE PRIMARY_EXCHANGE IS NOT NULL
            ORDER BY PRIMARY_EXCHANGE
        """).to_pandas()['PRIMARY_EXCHANGE'].tolist()
    
    st.markdown("""
    <style>
        div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:has(div.filter-card) {
            background: linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
            border-radius: 12px;
            padding: 0.5rem;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px; 
                padding: 1rem 1.5rem; margin-bottom: 1rem;">
        <div style="display: flex; align-items: center; margin-bottom: 0.75rem;">
            <span style="font-size: 1.25rem; margin-right: 0.5rem;">🔍</span>
            <span style="font-family: 'Outfit', sans-serif; font-weight: 600; color: #0f172a; font-size: 1.1rem;">
                Filter Trades
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    filter_row1_col1, filter_row1_col2, filter_row1_col3, filter_row1_col4 = st.columns([1.2, 1.5, 1, 0.8])
    
    with filter_row1_col1:
        st.markdown('<p style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">SECURITY TYPE</p>', unsafe_allow_html=True)
        security_type_filter = st.selectbox(
            "Security Type",
            options=["All Types", "Equity", "Bond"],
            index=0,
            key="gr_security_type",
            label_visibility="collapsed"
        )
    
    with filter_row1_col2:
        st.markdown('<p style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">TRADE DATE</p>', unsafe_allow_html=True)
        trade_date_range = st.date_input(
            "Trade Date Range",
            value=[],
            key="gr_trade_date",
            label_visibility="collapsed"
        )
    
    with filter_row1_col3:
        st.markdown('<p style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">S&P 500</p>', unsafe_allow_html=True)
        sp500_select = st.selectbox(
            "S&P 500",
            options=["All", "Yes", "No"],
            index=0,
            key="gr_sp500",
            label_visibility="collapsed"
        )
    
    with filter_row1_col4:
        st.markdown('<p style="font-size: 0.75rem; color: transparent; margin-bottom: 0.25rem;">.</p>', unsafe_allow_html=True)
        clear_filters = st.button("🔄 Reset", key="clear_filters", use_container_width=True)
    
    filter_row2_col1, filter_row2_col2, filter_row2_col3 = st.columns([1.5, 1.2, 1.3])
    
    with filter_row2_col1:
        st.markdown('<p style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">AMOUNT USD</p>', unsafe_allow_html=True)
        amount_range = st.slider(
            "Amount USD Range",
            min_value=0,
            max_value=10000000,
            value=(0, 10000000),
            step=10000,
            format="$%d",
            key="gr_amount",
            label_visibility="collapsed"
        )
    
    with filter_row2_col2:
        st.markdown('<p style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">TICKER SEARCH</p>', unsafe_allow_html=True)
        ticker_input = st.text_input(
            "Ticker",
            placeholder="e.g., AAPL, MSFT",
            key="gr_ticker",
            label_visibility="collapsed"
        )
    
    with filter_row2_col3:
        st.markdown('<p style="font-size: 0.75rem; color: #64748b; margin-bottom: 0.25rem; font-weight: 600;">EXCHANGE</p>', unsafe_allow_html=True)
        exchanges = ["All Exchanges"] + get_exchange_list()
        exchange_select = st.selectbox(
            "Exchange",
            options=exchanges,
            index=0,
            key="gr_exchange",
            label_visibility="collapsed"
        )
    
    trade_date_start = None
    trade_date_end = None
    if trade_date_range and len(trade_date_range) == 2:
        trade_date_start = trade_date_range[0]
        trade_date_end = trade_date_range[1]
    elif trade_date_range and len(trade_date_range) == 1:
        trade_date_start = trade_date_range[0]
        trade_date_end = trade_date_range[0]
    
    sec_type = None if security_type_filter == "All Types" else security_type_filter
    
    mapped_trades = load_mapped_trades(
        security_type=sec_type,
        trade_date_start=trade_date_start,
        trade_date_end=trade_date_end,
        sp500_filter=sp500_select if sp500_select != "All" else None,
        min_amount=amount_range[0] if amount_range[0] > 0 else None,
        max_amount=amount_range[1] if amount_range[1] < 10000000 else None,
        ticker_filter=ticker_input if ticker_input else None,
        exchange_filter=exchange_select if exchange_select != "All Exchanges" else None
    )
    
    st.markdown("""
    <div style="background: linear-gradient(90deg, #f8fafc 0%, #f1f5f9 100%); 
                border-radius: 12px; padding: 1rem; margin: 1rem 0;">
    </div>
    """, unsafe_allow_html=True)
    
    summary_col1, summary_col2, summary_col3, summary_col4, summary_col5, summary_col6 = st.columns([1, 1, 1, 1, 1, 1])
    with summary_col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #3b82f6, #1d4ed8); border-radius: 8px; 
                    padding: 0.5rem; text-align: center; box-shadow: 0 2px 8px rgba(59, 130, 246, 0.3);">
            <p style="color: rgba(255,255,255,0.8); margin: 0; font-size: 0.55rem; font-weight: 600; letter-spacing: 0.05em;">TOTAL VALUE</p>
            <p style="color: white; margin: 0.15rem 0 0 0; font-size: 1rem; font-weight: 700;">${:,.0f}</p>
        </div>
        """.format(mapped_trades['AMOUNT_USD'].sum() if not mapped_trades.empty else 0), unsafe_allow_html=True)
    with summary_col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #10b981, #059669); border-radius: 8px; 
                    padding: 0.5rem; text-align: center; box-shadow: 0 2px 8px rgba(16, 185, 129, 0.3);">
            <p style="color: rgba(255,255,255,0.8); margin: 0; font-size: 0.55rem; font-weight: 600; letter-spacing: 0.05em;">TOTAL TRADES</p>
            <p style="color: white; margin: 0.15rem 0 0 0; font-size: 1rem; font-weight: 700;">{:,}</p>
        </div>
        """.format(len(mapped_trades)), unsafe_allow_html=True)
    with summary_col3:
        equity_count = len(mapped_trades[mapped_trades['SECURITY_TYPE'] == 'Equity']) if not mapped_trades.empty else 0
        st.markdown("""
        <div style="background: linear-gradient(135deg, #8b5cf6, #6d28d9); border-radius: 8px; 
                    padding: 0.5rem; text-align: center; box-shadow: 0 2px 8px rgba(139, 92, 246, 0.3);">
            <p style="color: rgba(255,255,255,0.8); margin: 0; font-size: 0.55rem; font-weight: 600; letter-spacing: 0.05em;">EQUITY TRADES</p>
            <p style="color: white; margin: 0.15rem 0 0 0; font-size: 1rem; font-weight: 700;">{:,}</p>
        </div>
        """.format(equity_count), unsafe_allow_html=True)
    with summary_col4:
        bond_count = len(mapped_trades[mapped_trades['SECURITY_TYPE'] == 'Bond']) if not mapped_trades.empty else 0
        st.markdown("""
        <div style="background: linear-gradient(135deg, #f59e0b, #d97706); border-radius: 8px; 
                    padding: 0.5rem; text-align: center; box-shadow: 0 2px 8px rgba(245, 158, 11, 0.3);">
            <p style="color: rgba(255,255,255,0.8); margin: 0; font-size: 0.55rem; font-weight: 600; letter-spacing: 0.05em;">BOND TRADES</p>
            <p style="color: white; margin: 0.15rem 0 0 0; font-size: 1rem; font-weight: 700;">{:,}</p>
        </div>
        """.format(bond_count), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    active_filters = []
    if security_type_filter != "All Types":
        active_filters.append(f"Type: {security_type_filter}")
    if trade_date_start:
        active_filters.append(f"Date: {trade_date_start} to {trade_date_end}")
    if sp500_select != "All":
        active_filters.append(f"S&P 500: {sp500_select}")
    if ticker_input:
        active_filters.append(f"Ticker: {ticker_input}")
    if exchange_select != "All Exchanges":
        active_filters.append(f"Exchange: {exchange_select}")
    if amount_range[0] > 0 or amount_range[1] < 10000000:
        active_filters.append(f"Amount: ${amount_range[0]:,} - ${amount_range[1]:,}")
    
    if active_filters:
        filter_tags = " ".join([f'<span style="background: #e0f2fe; color: #0369a1; padding: 0.25rem 0.75rem; border-radius: 20px; font-size: 0.75rem; margin-right: 0.5rem;">{f}</span>' for f in active_filters])
        st.markdown(f"""
        <div style="margin-bottom: 1rem;">
            <span style="color: #64748b; font-size: 0.8rem; margin-right: 0.5rem;">Active filters:</span>
            {filter_tags}
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown(f'<p style="color: #64748b; font-size: 0.85rem; margin-bottom: 0.5rem;">Showing <strong>{len(mapped_trades):,}</strong> trades (max 1,000)</p>', unsafe_allow_html=True)
    
    if not mapped_trades.empty:
        display_mapped = mapped_trades[['SECURITY_TYPE', 'TRADE_DATE', 'TRADE_TYPE', 'TICKER', 
                                        'ISSUER', 'GLOBAL_SECURITY_ID', 'EXCHANGE', 'ISIN', 
                                        'CUSIP', 'SEDOL', 'QUANTITY', 'PRICE', 'AMOUNT_USD', 
                                        'IN_SP500']].copy()
        display_mapped['AMOUNT_USD'] = display_mapped['AMOUNT_USD'].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A")
        display_mapped['PRICE'] = display_mapped['PRICE'].apply(lambda x: f"${x:,.4f}" if pd.notna(x) else "N/A")
        display_mapped['QUANTITY'] = display_mapped['QUANTITY'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
        display_mapped = display_mapped.rename(columns={
            'SECURITY_TYPE': 'Type',
            'TRADE_DATE': 'Date',
            'TRADE_TYPE': 'Side',
            'TICKER': 'Ticker',
            'ISSUER': 'Issuer',
            'GLOBAL_SECURITY_ID': 'GSID',
            'EXCHANGE': 'Exchange',
            'QUANTITY': 'Qty',
            'PRICE': 'Price',
            'AMOUNT_USD': 'Amount USD',
            'IN_SP500': 'S&P 500'
        })
        st.dataframe(display_mapped, use_container_width=True, height=500)
    else:
        st.info("No trades found matching the selected filters.")

with tab7:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #7c3aed 0%, #8b5cf6 50%, #a78bfa 100%); 
                border-radius: 16px; padding: 1.5rem 2rem; margin-bottom: 1.5rem;
                box-shadow: 0 4px 20px rgba(124, 58, 237, 0.3);">
        <h2 style="color: white; margin: 0; font-family: 'Outfit', sans-serif; font-weight: 700;">
            📜 Security Master History & Audit Trail
        </h2>
        <p style="color: rgba(255,255,255,0.9); margin: 0.5rem 0 0 0; font-size: 1rem;">
            Track all changes to Security Master data with full lineage and audit capabilities
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    history_tab1, history_tab2 = st.tabs(["📊 Current Security Master", "📜 Change History"])
    
    with history_tab1:
        st.subheader("Security Master - Latest State")
        
        @st.cache_data(ttl=60)
        def load_full_security_master():
            return session.sql("""
                SELECT 
                    GLOBAL_SECURITY_ID, ISSUER, ASSET_CLASS, PRIMARY_TICKER, PRIMARY_EXCHANGE,
                    ISIN, CUSIP, SEDOL, CURRENCY, STATUS, GOLDEN_SOURCE, LAST_VALIDATED,
                    LINEAGE_ID, CREATED_BY, CREATED_AT, LAST_MODIFIED_BY
                FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
                ORDER BY LAST_VALIDATED DESC
            """).to_pandas()
        
        full_master = load_full_security_master()
        
        master_col1, master_col2, master_col3, master_col4 = st.columns(4)
        with master_col1:
            st.metric("Total Securities", f"{len(full_master):,}")
        with master_col2:
            active_count = len(full_master[full_master['STATUS'] == 'Active'])
            st.metric("Active", f"{active_count:,}")
        with master_col3:
            preissue_count = len(full_master[full_master['STATUS'] == 'Pre-Issue'])
            st.metric("Pre-Issue", f"{preissue_count:,}")
        with master_col4:
            matured_count = len(full_master[full_master['STATUS'].isin(['Matured', 'Defaulted', 'Retired'])])
            st.metric("Matured/Retired", f"{matured_count:,}")
        
        st.markdown("---")
        
        export_col1, export_col2, export_col3 = st.columns([1, 1, 2])
        with export_col1:
            if st.button("📥 Export to CSV", use_container_width=True):
                try:
                    from datetime import datetime
                    now = datetime.now()
                    file_date = now.strftime("%d-%b-%Y").upper()
                    file_time = now.strftime("%H-%M")
                    csv_filename = f"SEC_MASTER_{file_date}_{file_time}.csv"
                    
                    session.sql(f"""
                        COPY INTO @SECURITY_MASTER_DB.GOLDEN_RECORD.EXPORT/{csv_filename}
                        FROM (
                            SELECT 
                                'GLOBAL_SECURITY_ID' as C1, 'ISSUER' as C2, 'ASSET_CLASS' as C3, 
                                'PRIMARY_TICKER' as C4, 'PRIMARY_EXCHANGE' as C5, 'ISIN' as C6, 
                                'CUSIP' as C7, 'SEDOL' as C8, 'CURRENCY' as C9, 'STATUS' as C10,
                                'GOLDEN_SOURCE' as C11, 'LAST_VALIDATED' as C12, 'LINEAGE_ID' as C13,
                                'CREATED_BY' as C14, 'CREATED_AT' as C15, 'LAST_MODIFIED_BY' as C16
                            UNION ALL
                            SELECT 
                                GLOBAL_SECURITY_ID, ISSUER, ASSET_CLASS, PRIMARY_TICKER, PRIMARY_EXCHANGE,
                                ISIN, CUSIP, SEDOL, CURRENCY, STATUS, GOLDEN_SOURCE, 
                                TO_VARCHAR(LAST_VALIDATED, 'YYYY-MM-DD HH24:MI:SS'),
                                LINEAGE_ID, CREATED_BY, 
                                TO_VARCHAR(CREATED_AT, 'YYYY-MM-DD HH24:MI:SS'), 
                                LAST_MODIFIED_BY
                            FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE
                            ORDER BY 12 DESC
                        )
                        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = NONE)
                        OVERWRITE = TRUE
                        SINGLE = TRUE
                    """).collect()
                    st.success(f"✅ Exported to @EXPORT/{csv_filename}")
                except Exception as e:
                    st.error(f"Export error: {str(e)}")
        with export_col2:
            if st.button("🔄 Update Systems", use_container_width=True, type="primary"):
                try:
                    from datetime import datetime
                    import json
                    
                    now = datetime.now()
                    file_date = now.strftime("%d-%b-%Y").upper()
                    file_time = now.strftime("%H-%M")
                    filename = f"SEC_MASTER_UPDATES_{file_date}_{file_time}.json"
                    
                    latest_changes = session.sql("""
                        SELECT 
                            h.HISTORY_ID,
                            h.GLOBAL_SECURITY_ID,
                            h.ACTION,
                            h.ISSUER_BEFORE,
                            h.ISSUER_AFTER,
                            h.ASSET_CLASS_BEFORE,
                            h.ASSET_CLASS_AFTER,
                            h.PRIMARY_TICKER_BEFORE,
                            h.PRIMARY_TICKER_AFTER,
                            h.PRIMARY_EXCHANGE_BEFORE,
                            h.PRIMARY_EXCHANGE_AFTER,
                            h.ISIN_BEFORE,
                            h.ISIN_AFTER,
                            h.CUSIP_BEFORE,
                            h.CUSIP_AFTER,
                            h.SEDOL_BEFORE,
                            h.SEDOL_AFTER,
                            h.CURRENCY_BEFORE,
                            h.CURRENCY_AFTER,
                            h.STATUS_BEFORE,
                            h.STATUS_AFTER,
                            h.EDIT_REASON,
                            h.CHANGED_BY,
                            TO_VARCHAR(h.CHANGED_AT, 'YYYY-MM-DD HH24:MI:SS') as CHANGED_AT,
                            h.LINEAGE_ID,
                            h.LINEAGE_PARENT_ID,
                            h.LINEAGE_PATH,
                            r.ISSUER as CURRENT_ISSUER,
                            r.ASSET_CLASS as CURRENT_ASSET_CLASS,
                            r.PRIMARY_TICKER as CURRENT_TICKER,
                            r.PRIMARY_EXCHANGE as CURRENT_EXCHANGE,
                            r.ISIN as CURRENT_ISIN,
                            r.CUSIP as CURRENT_CUSIP,
                            r.SEDOL as CURRENT_SEDOL,
                            r.CURRENCY as CURRENT_CURRENCY,
                            r.STATUS as CURRENT_STATUS
                        FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY h
                        LEFT JOIN SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE r
                            ON h.GLOBAL_SECURITY_ID = r.GLOBAL_SECURITY_ID
                        WHERE h.CHANGED_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP())
                        ORDER BY h.CHANGED_AT DESC
                    """).to_pandas()
                    
                    export_data = {
                        "export_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "export_type": "SECURITY_MASTER_UPDATES",
                        "record_count": len(latest_changes),
                        "changes": latest_changes.to_dict(orient='records')
                    }
                    
                    json_str = json.dumps(export_data, indent=2, default=str)
                    
                    session.sql(f"""
                        COPY INTO @SECURITY_MASTER_DB.GOLDEN_RECORD.EXPORT/{filename}
                        FROM (
                            SELECT OBJECT_CONSTRUCT(
                                'export_timestamp', '{now.strftime("%Y-%m-%d %H:%M:%S")}',
                                'export_type', 'SECURITY_MASTER_UPDATES',
                                'record_count', {len(latest_changes)},
                                'changes', PARSE_JSON('{json_str.replace(chr(39), chr(39)+chr(39))}')
                            )
                        )
                        FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
                        OVERWRITE = TRUE
                        SINGLE = TRUE
                    """).collect()
                    
                    st.success(f"✅ Exported {len(latest_changes)} changes to @EXPORT/{filename}")
                    
                except Exception as e:
                    st.error(f"Error exporting: {str(e)}")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        display_master = full_master[['GLOBAL_SECURITY_ID', 'ISSUER', 'ASSET_CLASS', 'PRIMARY_TICKER', 
                                      'PRIMARY_EXCHANGE', 'ISIN', 'CUSIP', 'SEDOL', 'CURRENCY', 
                                      'STATUS', 'LAST_VALIDATED', 'CREATED_BY', 'LAST_MODIFIED_BY']].copy()
        display_master = display_master.rename(columns={
            'GLOBAL_SECURITY_ID': 'GSID', 'PRIMARY_TICKER': 'Ticker', 'PRIMARY_EXCHANGE': 'Exchange',
            'ASSET_CLASS': 'Class', 'LAST_VALIDATED': 'Last Updated', 'CREATED_BY': 'Created By',
            'LAST_MODIFIED_BY': 'Modified By'
        })
        st.dataframe(display_master, use_container_width=True, height=500)
    
    with history_tab2:
        st.subheader("Change History & Audit Log")
        
        @st.cache_data(ttl=30)
        def load_history_data():
            return session.sql("""
                SELECT * FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                ORDER BY CHANGED_AT DESC
                LIMIT 1000
            """).to_pandas()
        
        @st.cache_data(ttl=300)
        def get_unique_users():
            result = session.sql("""
                SELECT DISTINCT CHANGED_BY FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                WHERE CHANGED_BY IS NOT NULL ORDER BY CHANGED_BY
            """).to_pandas()
            return result['CHANGED_BY'].tolist()
        
        @st.cache_data(ttl=300)
        def get_unique_tickers_history():
            result = session.sql("""
                SELECT DISTINCT COALESCE(PRIMARY_TICKER_AFTER, PRIMARY_TICKER_BEFORE) as TICKER 
                FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                WHERE COALESCE(PRIMARY_TICKER_AFTER, PRIMARY_TICKER_BEFORE) IS NOT NULL
                ORDER BY TICKER
            """).to_pandas()
            return result['TICKER'].tolist()
        
        @st.cache_data(ttl=300)
        def get_unique_currencies_history():
            result = session.sql("""
                SELECT DISTINCT COALESCE(CURRENCY_AFTER, CURRENCY_BEFORE) as CURRENCY 
                FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                WHERE COALESCE(CURRENCY_AFTER, CURRENCY_BEFORE) IS NOT NULL
                ORDER BY CURRENCY
            """).to_pandas()
            return result['CURRENCY'].tolist()
        
        @st.cache_data(ttl=300)
        def get_unique_exchanges_history():
            result = session.sql("""
                SELECT DISTINCT COALESCE(PRIMARY_EXCHANGE_AFTER, PRIMARY_EXCHANGE_BEFORE) as EXCHANGE 
                FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                WHERE COALESCE(PRIMARY_EXCHANGE_AFTER, PRIMARY_EXCHANGE_BEFORE) IS NOT NULL
                ORDER BY EXCHANGE
            """).to_pandas()
            return result['EXCHANGE'].tolist()
        
        hist_filter_col1, hist_filter_col2, hist_filter_col3, hist_filter_col4, hist_filter_col5 = st.columns(5)
        
        with hist_filter_col1:
            users = ["All Users"] + get_unique_users()
            user_filter = st.selectbox("Changed By", users, key="hist_user")
        
        with hist_filter_col2:
            tickers = ["All Tickers"] + get_unique_tickers_history()
            ticker_filter = st.selectbox("Ticker", tickers, key="hist_ticker")
        
        with hist_filter_col3:
            currencies = ["All Currencies"] + get_unique_currencies_history()
            currency_filter = st.selectbox("Currency", currencies, key="hist_currency")
        
        with hist_filter_col4:
            exchanges = ["All Exchanges"] + get_unique_exchanges_history()
            exchange_filter = st.selectbox("Exchange", exchanges, key="hist_exchange")
        
        with hist_filter_col5:
            isin_filter = st.text_input("ISIN Filter", placeholder="e.g., US037833...", key="hist_isin")
        
        history_data = load_history_data()
        
        if not history_data.empty:
            filtered_history = history_data.copy()
            
            if user_filter != "All Users":
                filtered_history = filtered_history[filtered_history['CHANGED_BY'] == user_filter]
            if ticker_filter != "All Tickers":
                filtered_history = filtered_history[
                    (filtered_history['PRIMARY_TICKER_AFTER'] == ticker_filter) | 
                    (filtered_history['PRIMARY_TICKER_BEFORE'] == ticker_filter)
                ]
            if currency_filter != "All Currencies":
                filtered_history = filtered_history[
                    (filtered_history['CURRENCY_AFTER'] == currency_filter) | 
                    (filtered_history['CURRENCY_BEFORE'] == currency_filter)
                ]
            if exchange_filter != "All Exchanges":
                filtered_history = filtered_history[
                    (filtered_history['PRIMARY_EXCHANGE_AFTER'] == exchange_filter) | 
                    (filtered_history['PRIMARY_EXCHANGE_BEFORE'] == exchange_filter)
                ]
            if isin_filter:
                filtered_history = filtered_history[
                    (filtered_history['ISIN_AFTER'].str.contains(isin_filter, na=False, case=False)) | 
                    (filtered_history['ISIN_BEFORE'].str.contains(isin_filter, na=False, case=False))
                ]
            
            st.markdown(f"**Found {len(filtered_history):,} change records**")
            
            export_hist_col1, export_hist_col2 = st.columns([1, 3])
            with export_hist_col1:
                if st.button("📥 Export History CSV", use_container_width=True):
                    try:
                        from datetime import datetime
                        now = datetime.now()
                        file_date = now.strftime("%d-%b-%Y").upper()
                        file_time = now.strftime("%H-%M")
                        hist_filename = f"SEC_MASTER_HISTORY_{file_date}_{file_time}.csv"
                        
                        session.sql(f"""
                            COPY INTO @SECURITY_MASTER_DB.GOLDEN_RECORD.EXPORT/{hist_filename}
                            FROM (
                                SELECT 
                                    'HISTORY_ID' as C1, 'GLOBAL_SECURITY_ID' as C2, 'ACTION' as C3,
                                    'ISSUER_BEFORE' as C4, 'ISSUER_AFTER' as C5,
                                    'STATUS_BEFORE' as C6, 'STATUS_AFTER' as C7,
                                    'EDIT_REASON' as C8, 'CHANGED_BY' as C9, 'CHANGED_AT' as C10,
                                    'LINEAGE_ID' as C11, 'LINEAGE_PATH' as C12
                                UNION ALL
                                SELECT 
                                    TO_VARCHAR(HISTORY_ID), GLOBAL_SECURITY_ID, ACTION,
                                    ISSUER_BEFORE, ISSUER_AFTER,
                                    STATUS_BEFORE, STATUS_AFTER,
                                    EDIT_REASON, CHANGED_BY, TO_VARCHAR(CHANGED_AT, 'YYYY-MM-DD HH24:MI:SS'),
                                    LINEAGE_ID, LINEAGE_PATH
                                FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                                ORDER BY 10 DESC
                            )
                            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                            OVERWRITE = TRUE
                            SINGLE = TRUE
                        """).collect()
                        st.success(f"✅ Exported to @EXPORT/{hist_filename}")
                    except Exception as e:
                        st.error(f"Export error: {str(e)}")
            
            st.markdown("---")
            
            display_history = filtered_history[['HISTORY_ID', 'GLOBAL_SECURITY_ID', 'ACTION', 
                                                'PRIMARY_TICKER_BEFORE', 'PRIMARY_TICKER_AFTER',
                                                'ISSUER_BEFORE', 'ISSUER_AFTER',
                                                'STATUS_BEFORE', 'STATUS_AFTER',
                                                'EDIT_REASON', 'CHANGED_BY', 'CHANGED_AT', 
                                                'LINEAGE_ID']].copy()
            display_history = display_history.rename(columns={
                'HISTORY_ID': 'ID', 'GLOBAL_SECURITY_ID': 'GSID', 'ACTION': 'Action',
                'PRIMARY_TICKER_BEFORE': 'Ticker (Before)', 'PRIMARY_TICKER_AFTER': 'Ticker (After)',
                'ISSUER_BEFORE': 'Issuer (Before)', 'ISSUER_AFTER': 'Issuer (After)',
                'STATUS_BEFORE': 'Status (Before)', 'STATUS_AFTER': 'Status (After)',
                'EDIT_REASON': 'Reason', 'CHANGED_BY': 'Changed By', 'CHANGED_AT': 'Changed At',
                'LINEAGE_ID': 'Lineage ID'
            })
            
            st.dataframe(display_history, use_container_width=True, height=400)
            
            st.markdown("---")
            st.subheader("🔗 Lineage Viewer")
            
            if not filtered_history.empty:
                gsid_options = ["-- Select a security --"] + filtered_history['GLOBAL_SECURITY_ID'].unique().tolist()
                selected_gsid_lineage = st.selectbox("Select Security to View Lineage", gsid_options, key="lineage_gsid")
                
                if selected_gsid_lineage != "-- Select a security --":
                    lineage_records = session.sql(f"""
                        SELECT HISTORY_ID, ACTION, CHANGED_AT, CHANGED_BY, EDIT_REASON,
                               ISSUER_BEFORE, ISSUER_AFTER, STATUS_BEFORE, STATUS_AFTER,
                               LINEAGE_ID, LINEAGE_PARENT_ID, LINEAGE_PATH
                        FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
                        WHERE GLOBAL_SECURITY_ID = '{selected_gsid_lineage}'
                        ORDER BY CHANGED_AT ASC
                    """).to_pandas()
                    
                    if not lineage_records.empty:
                        st.markdown(f"**Lineage for {selected_gsid_lineage}** - {len(lineage_records)} changes found")
                        
                        for idx, record in lineage_records.iterrows():
                            action_color = "#10b981" if record['ACTION'] == 'INSERT' else "#f59e0b"
                            st.markdown(f"""
                            <div style="border-left: 4px solid {action_color}; padding-left: 1rem; margin-bottom: 1rem; background: #f8fafc; border-radius: 0 8px 8px 0; padding: 1rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <span style="font-weight: 600; color: {action_color};">{record['ACTION']}</span>
                                    <span style="color: #64748b; font-size: 0.85rem;">{record['CHANGED_AT']}</span>
                                </div>
                                <p style="margin: 0.5rem 0; font-size: 0.9rem;"><strong>By:</strong> {record['CHANGED_BY']}</p>
                                <p style="margin: 0.5rem 0; font-size: 0.9rem;"><strong>Reason:</strong> {record['EDIT_REASON'] or 'N/A'}</p>
                                <p style="margin: 0.5rem 0; font-size: 0.9rem;"><strong>Status:</strong> {record['STATUS_BEFORE'] or 'N/A'} → {record['STATUS_AFTER'] or 'N/A'}</p>
                                <p style="margin: 0.5rem 0; font-size: 0.75rem; color: #64748b;"><strong>Lineage ID:</strong> {record['LINEAGE_ID']}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.info("No lineage records found for this security.")
        else:
            st.info("No history records found. Changes will be tracked when securities are added or modified.")

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #94a3b8; font-family: 'JetBrains Mono', monospace; font-size: 0.75rem;">
    Data source: <a href="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies" style="color: #0d9488;">Wikipedia S&P 500 List</a> | 
    NYSE data via OpenFIGI | Corporate Bonds from Top US Issuers | Built with Streamlit in Snowflake
</div>
""", unsafe_allow_html=True)
