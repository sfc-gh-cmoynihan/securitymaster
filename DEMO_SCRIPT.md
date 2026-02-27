# Snow-Trade Demo Script

## Overview
This demo showcases Snow-Trade, a Snowflake-native application for **Security Trading, Settlement & Master** data management. The app demonstrates real-time trading capabilities, security master management, and settlement workflows—all powered by Snowflake.

---

## Pre-Demo Setup

1. **Ensure Installation Complete**
   ```sql
   -- Run install script if not already done
   @install_snowtrade.sql
   ```

2. **Open the App**
   - Navigate to Snowsight → Streamlit Apps → `SNOWTRADE_APP`
   - Or use direct link: `https://app.snowflake.com/<account>/#/streamlit-apps/SECURITY_MASTER_DB.GOLDEN_RECORD.SNOWTRADE_APP`

---

## Demo Flow (15-20 minutes)

### 1. Introduction (2 min)
> "Welcome to Snow-Trade—a complete securities trading and master data management platform built natively on Snowflake. Today I'll show you how we handle everything from trade execution to settlement, all with real-time data and external API integrations."

**Key Points:**
- Built 100% on Snowflake (Streamlit, Hybrid Tables, External Access)
- Real-time market data via Yahoo Finance API
- Security lookup via OpenFIGI API
- FIX/FIXML protocol support for trade messaging

---

### 2. Portfolio Overview (3 min)
**Navigate to: 📊 Portfolio Tab**

> "Let's start with our portfolio dashboard. Here you can see our current positions across equities and fixed income."

**Demo Actions:**
- Point out the **Top 5 metrics**: Total AUM, Cash Balance, US Treasury holdings, Total Trades, Realized P&L
- Show the **Top 10 Gainers/Losers** charts
- Scroll to **Sector Analysis** - explain how trades are automatically categorized by GICS sector
- Highlight the **Bond Portfolio Yield Analysis** section

**Talking Points:**
- "All data is live from Snowflake—no external BI tools needed"
- "Sector breakdown comes from joining trade data with our S&P 500 reference table"
- "Bond yields are calculated in real-time from our corporate bonds master"

---

### 3. Live Stock Trading (5 min)
**Navigate to: 📝 Stock / ETF Order Tab**

> "Now let's execute a live trade. Watch how we pull real-time market data."

**Demo Actions:**
1. Select a symbol (e.g., **AAPL** or **MSFT**)
2. Point out the **live price quote** appearing with:
   - Current price
   - Previous close
   - Change % (green/red)
   - Market state
3. Configure order:
   - Action: **Buy**
   - Quantity: **100**
   - Price Type: **Market**
   - Duration: **Good for Day**
4. Click **Preview Order** - review the order summary
5. Click **Place Order** - show confirmation message

**Talking Points:**
- "Live prices come from Yahoo Finance via Snowflake External Access Integration"
- "When we place this order, three things happen automatically:
  1. Trade is inserted into our Hybrid Table for ACID compliance
  2. A FIX/FIXML message is generated and stored in our stage
  3. Portfolio metrics update in real-time"

**Show the FIXML:**
```sql
-- Show generated FIXML messages
LIST @SECURITY_MASTER_DB.TRADES.FIX_STAGE;
```

---

### 4. Bond Trading (3 min)
**Navigate to: 🏦 Bond Order Tab**

> "We also support fixed income trading with the same workflow."

**Demo Actions:**
1. Search for a bond (e.g., **Apple** or **Microsoft**)
2. Show bond details: CUSIP, Credit Rating, Yield, Coupon Rate
3. Configure order:
   - Action: **Buy**
   - Quantity: **10** ($10,000 face value)
   - Execution Type: **Market**
4. Toggle between **Price** and **Yield** modes
5. Preview and place the order

**Talking Points:**
- "Bonds are identified by CUSIP and linked to our corporate bonds master"
- "Notice we can trade by either price or yield—common in fixed income"

---

### 5. Security Master Management (4 min)
**Navigate to: ✏️ Master Data Tab**

> "The heart of any trading system is the security master. Let me show you our golden record management."

**Demo Actions:**
1. **ISIN Lookup Demo:**
   - Select "ISIN" from dropdown
   - Enter: `US0378331005` (Apple's ISIN)
   - Click **Lookup**
   - Show how fields auto-populate from OpenFIGI API

2. **Ticker Lookup Demo:**
   - Select "Ticker" from dropdown
   - Enter: `GOOGL`
   - Click **Lookup**
   - Show S&P 500 data populating

3. **Create New Security:**
   - Fill in remaining fields
   - Click **Save Security**

**Talking Points:**
- "We integrate with OpenFIGI—the industry standard for security identification"
- "Every security gets a unique Global Security ID (GSID)"
- "All changes are tracked in our audit history"

**Navigate to: 📜 Master History Tab**
- Show the audit trail of all security master changes
- Point out INSERT vs UPDATE actions
- Demonstrate the CSV export feature

---

### 6. Trade Matching & Settlement (3 min)
**Navigate to: 🔗 Equity Trades Tab**

> "Let's look at how we match trades against our NYSE security master."

**Demo Actions:**
- Show the **Match Rate** metric
- Filter by "Unmatched" to show trades needing attention
- Explain how trades are matched by symbol to NYSE reference data

**Navigate to: 📋 Settlement Details Tab**
- Filter by **Type**: Equity
- Filter by **Status**: Pending
- Show T+1 settlement dates
- Demonstrate filtering by Trade Date and Exchange

**Talking Points:**
- "Settlement follows T+1 standard"
- "Status automatically updates based on current date vs settlement date"
- "All filters work in real-time against Snowflake"

---

### 7. Architecture Deep Dive (Optional - 2 min)

> "Let me quickly show you what's under the hood."

```sql
-- Show the database structure
SHOW SCHEMAS IN DATABASE SECURITY_MASTER_DB;

-- Show hybrid tables for ACID compliance
SHOW TABLES LIKE '%TRADES%' IN SCHEMA SECURITY_MASTER_DB.TRADES;

-- Show external integrations
SHOW INTEGRATIONS LIKE '%YAHOO%';
SHOW INTEGRATIONS LIKE '%OPENFIGI%';

-- Show the UDFs
SHOW USER FUNCTIONS IN SCHEMA SECURITY_MASTER_DB.GOLDEN_RECORD;
SHOW USER FUNCTIONS IN SCHEMA SECURITY_MASTER_DB.TRADES;
```

**Talking Points:**
- "Hybrid Tables give us ACID transactions for trade data"
- "External Access Integrations securely connect to Yahoo Finance and OpenFIGI"
- "Python UDFs handle the API calls—all running in Snowflake's secure sandbox"

---

## Key Differentiators

| Feature | Snow-Trade Advantage |
|---------|---------------------|
| **Data Platform** | 100% Snowflake-native—no external databases |
| **Real-time Pricing** | Yahoo Finance API via External Access |
| **Security Lookup** | OpenFIGI integration for ISIN/FIGI resolution |
| **Trade Messaging** | FIX/FIXML protocol support |
| **Audit Trail** | Complete history with before/after tracking |
| **Settlement** | Automated T+1 calculation and status |
| **UI** | Streamlit in Snowflake—no separate hosting |

---

## Common Questions

**Q: How does this handle high-frequency trading?**
> A: Hybrid Tables provide single-digit millisecond latency for transactional workloads. For true HFT, you'd want to add Snowpark Container Services for co-located processing.

**Q: Can we connect to Bloomberg instead of Yahoo Finance?**
> A: Absolutely. The External Access Integration pattern works with any REST API. You'd just update the network rules and UDF to call Bloomberg's API.

**Q: How do we handle corporate actions?**
> A: The Security Master History table tracks all changes. You could extend this with a dedicated corporate actions table and event-driven processing via Streams and Tasks.

**Q: What about regulatory reporting?**
> A: All data is in Snowflake, so you can easily generate TRACE, CAT, or MiFID II reports using standard SQL. The audit trail provides complete lineage.

---

## Cleanup (if needed)

```sql
-- To remove all Snow-Trade objects
@uninstall_snowtrade.sql
```

---

## Resources

- **GitHub**: https://github.com/sfc-gh-cmoynihan/snowtrade
- **Snowflake Docs**: [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- **OpenFIGI API**: https://www.openfigi.com/api
- **FIX Protocol**: https://www.fixtrading.org/

---

*Demo created for Snow-Trade v1.0 - February 2026*
