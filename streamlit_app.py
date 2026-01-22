# ============================================
# SECURITY MASTER STREAMLIT APP
# Portfolio Analysis & Trade Viewer
# ============================================

import streamlit as st
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Security Master",
    page_icon="📈",
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

# Header
st.markdown('<p class="hero-header">📈 Security Master</p>', unsafe_allow_html=True)
st.markdown('<p class="hero-sub">S&P 500 Portfolio Analysis & Trade Tracking</p>', unsafe_allow_html=True)

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
        FROM SP500 
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
            FROM TRADES t
            JOIN SP500 s ON t.SYMBOL = s.SYMBOL
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
            FROM TRADES t
            JOIN SP500 s ON t.SYMBOL = s.SYMBOL
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
        FROM TRADES
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
        FROM TRADES t
        JOIN SP500 s ON t.SYMBOL = s.SYMBOL
        GROUP BY s.GICS_SECTOR
        ORDER BY TOTAL_VALUE DESC
    """).to_pandas()

# Load data
securities = load_securities()
portfolio_summary = get_portfolio_summary()
sector_data = get_sector_breakdown()

# Top metrics
col1, col2, col3, col4 = st.columns(4)

total_securities = len(securities)
total_trades = portfolio_summary['TRADE_COUNT'].sum()
total_buy_value = portfolio_summary['BUY_VALUE'].sum()
total_sell_value = portfolio_summary['SELL_VALUE'].sum()
net_pnl = total_sell_value - total_buy_value

with col1:
    st.metric("Securities", f"{total_securities:,}")
with col2:
    st.metric("Total Trades", f"{int(total_trades):,}")
with col3:
    st.metric("Total Buy Value", f"${total_buy_value:,.0f}")
with col4:
    pnl_color = "🟢" if net_pnl >= 0 else "🔴"
    st.metric("Realized P&L", f"{pnl_color} ${net_pnl:,.0f}")

st.markdown("---")

# Tabs for different views
tab1, tab2, tab3 = st.tabs(["🔍 Security Lookup", "📊 Portfolio Overview", "📈 Sector Analysis"])

with tab1:
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

with tab2:
    st.subheader("📊 Portfolio Summary by Security")
    
    # Top gainers and losers
    portfolio_with_pnl = portfolio_summary.copy()
    portfolio_with_pnl['REALIZED_PNL'] = portfolio_with_pnl['SELL_VALUE'] - portfolio_with_pnl['BUY_VALUE']
    
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
    st.markdown("#### 📈 Full Portfolio")
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
    
    st.dataframe(display_portfolio, use_container_width=True, height=400)

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

# Footer
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #94a3b8; font-family: 'JetBrains Mono', monospace; font-size: 0.75rem;">
    Data source: <a href="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies" style="color: #0d9488;">Wikipedia S&P 500 List</a> | 
    Built with Streamlit in Snowflake
</div>
""", unsafe_allow_html=True)
