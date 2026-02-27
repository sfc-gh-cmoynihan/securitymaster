# SnowTrade Demo Script

This demo script walks through the key features of the SnowTrade application, demonstrating how the system handles trade execution, settlement matching, and master data management.

## Pre-Demo Setup

Ensure you have:
- Access to the Snowflake account with the SnowTrade application
- The SNOWTRADE_APP Streamlit application running
- Test securities available (ideally with some missing from master data for demo purposes)

---

## Demo Flow

### Part 1: Execute New Trades (Missing Master Data)

**Objective:** Show what happens when trades are executed for securities not yet in the master data.

#### Step 1.1: Execute an Equity Trade (AAPL - Not in Master)

1. Navigate to **📝 Stock / ETF Order** tab
2. Select **AAPL - Apple Inc** from the symbol dropdown
3. Notice the live price quote appears (via Yahoo Finance integration)
4. Set:
   - Action: **BUY**
   - Quantity: **100**
   - Execution Type: **Market**
5. Click **Preview Order**
6. Review the order details including:
   - Estimated total value
   - T+1 Settlement date
7. Click **Place Order**
8. Note the green confirmation message

**Talking Point:** *"We've just executed a buy order for Apple stock. The system automatically calculates T+1 settlement and generates a unique trade ID. But notice - Apple isn't in our master data yet..."*

#### Step 1.2: Execute a Bond Trade (New Bond)

1. Navigate to **🏦 Bond Order** tab
2. Select a corporate bond from the dropdown (e.g., a Goldman Sachs or JPMorgan bond)
3. Set:
   - Action: **BUY**
   - Face Value: **10,000**
   - Price Mode: Select **Price** or **Yield**
   - Enter appropriate price/yield
4. Click **Preview Order**
5. Click **Place Order**
6. Note the confirmation with FIXML generation

**Talking Point:** *"For fixed income, we also generate FIXML messages that are stored in a Snowflake stage - this enables downstream integration with settlement systems and regulatory reporting."*

---

### Part 2: View Settlement Issues

**Objective:** Demonstrate how unmatched trades appear in settlement details.

1. Navigate to **📋 Settlement Details** tab
2. Set filters:
   - Type: **Equity** (or **<ALL>**)
   - Status: **Pending**
3. Look for the Apple trade - notice the **ISSUER** and **EXCHANGE** columns show **NULL** or are empty

**Talking Point:** *"Here we can see our pending settlements. Notice the Apple trade has no issuer information - that's because Apple isn't in our SnowTrade Reference data. This is a settlement risk - we can't properly route or match this trade."*

4. Click **🔄 Refresh** to show real-time updates

---

### Part 3: Add Missing Securities to Master Data

**Objective:** Show how adding master data resolves settlement matching.

#### Step 3.1: Add Apple to Master Data

1. Navigate to **✏️ Master Data** tab
2. Click **➕ Add New Security**
3. Fill in the form:
   - **Asset Class:** Equity
   - **Primary Ticker:** AAPL
   - **ISIN:** US0378331005
   - **CUSIP:** 037833100
   - **Issuer:** Apple Inc
   - **Primary Exchange:** NASDAQ
   - **Currency:** USD
   - **Country:** United States
4. Click **Add Security**
5. Verify the success message

**Talking Point:** *"Now we're adding Apple to our golden record. This master data will be used to enrich all our trades and enable proper settlement matching."*

#### Step 3.2: Verify Master History

1. Navigate to **📜 Master History** tab
2. Find the Apple entry showing:
   - OPERATION: INSERT
   - Timestamp of when it was added
   - Full record details

**Talking Point:** *"Every change to our master data is tracked with full audit history using Snowflake Streams and Change Data Capture. This is critical for regulatory compliance and data governance."*

---

### Part 4: Verify Settlement Resolution

**Objective:** Show that settlements now match properly.

1. Navigate back to **📋 Settlement Details** tab
2. Click **🔄 Refresh**
3. Find the Apple trade - now shows:
   - **ISSUER:** Apple Inc
   - **EXCHANGE:** NASDAQ

**Talking Point:** *"After adding Apple to our master data, the settlement details are now enriched. The LEFT JOIN to our SnowTrade Reference now finds a match, and we have complete trade information for settlement."*

---

### Part 5: Portfolio & Analytics

**Objective:** Show the business intelligence capabilities.

1. Navigate to **📊 Portfolio** tab
2. Point out:
   - Top 10 Gainers/Losers charts
   - Sector allocation breakdown
   - Portfolio summary table with P&L

**Talking Point:** *"All this trade data flows into our portfolio analytics in real-time. We can see positions, P&L, and sector exposure - all powered by Snowflake's unified data platform."*

2. Navigate to **🔍 Trade History** tab
3. Use filters to find specific trades
4. Show the trade blotter with full details

---

## Key Value Propositions

Throughout the demo, emphasize:

1. **Real-Time Data:** Live price quotes, instant trade execution, immediate settlement visibility
2. **Data Quality:** Missing master data is immediately visible in settlement details
3. **Audit Trail:** Full change history via Snowflake Streams
4. **Integration Ready:** FIXML generation for downstream systems
5. **Single Platform:** Trading, settlement, master data, and analytics all in Snowflake

---

## Technical Highlights

- **Hybrid Tables:** Used for EQUITY_TRADES and BOND_TRADES for low-latency transactional workloads
- **External Access:** Yahoo Finance integration for live stock prices
- **Streams & CDC:** Automatic change tracking on master data
- **Stages:** FIXML files stored in Snowflake stages for integration
- **Caching:** Optimized Streamlit with `@st.cache_data` for performance

---

## Cleanup (Post-Demo)

If you need to reset for another demo:

```sql
-- Remove test trades
DELETE FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES WHERE SYMBOL = 'AAPL';
DELETE FROM SECURITY_MASTER_DB.TRADES.BOND_TRADES WHERE CUSIP = '<test_cusip>';

-- Remove test master data
DELETE FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE WHERE PRIMARY_TICKER = 'AAPL';
```
