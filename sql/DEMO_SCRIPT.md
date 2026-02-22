# Security Master EDM - Demo Script

## Overview
This demo showcases the Security Master Enterprise Data Management (EDM) application built on Snowflake with Streamlit. The demo highlights trade matching, data quality management, audit trails, and AI-powered analytics.

---

## Demo Setup Summary

| Component | Details |
|-----------|---------|
| **App URL** | [Security Master EDM](https://app.snowflake.com/sfseeurope-colm_uswest/heb26349/#/streamlit-apps/SECURITIES_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP) |
| **Database** | SECURITIES_MASTER_DB |
| **Schema** | GOLDEN_RECORD |
| **Users Created** | SECURITY_ADMIN, CUSTODIAN_ADMIN |

---

## Part 1: Identify Unmatched Trades (NVIDIA)

### Scenario
Five NVIDIA (NVDA) equity trades were executed today totaling **$3,947,375** but they cannot be matched to the Security Master because NVIDIA is missing from the reference data.

### Steps

1. **Open the Streamlit App** and navigate to **"Trades Mapped to Master Data"** tab

2. **Filter for NVIDIA trades:**
   - In the Ticker filter, type: `NVDA`
   - Observe that 5 trades appear with NO matching GSID (Global Security ID)

3. **View the unmatched trades:**

| Trade ID | Symbol | Date | Type | Quantity | Price | Total Value | Status |
|----------|--------|------|------|----------|-------|-------------|--------|
| 2556 | NVDA | 2026-02-10 | BUY | 1,000 | $875.50 | $875,500 | UNMATCHED |
| 2557 | NVDA | 2026-02-10 | BUY | 500 | $878.25 | $439,125 | UNMATCHED |
| 2558 | NVDA | 2026-02-10 | SELL | 250 | $880.00 | $220,000 | UNMATCHED |
| 2559 | NVDA | 2026-02-10 | BUY | 2,000 | $876.75 | $1,753,500 | UNMATCHED |
| 2560 | NVDA | 2026-02-10 | BUY | 750 | $879.00 | $659,250 | UNMATCHED |

**Total Unmatched Value: $3,947,375**

### Key Talking Points
- Unmatched trades represent operational and regulatory risk
- Without a Golden Record match, we cannot properly report positions
- The Security Master acts as the single source of truth for all securities

---

## Part 2: Add NVIDIA to Security Master

### Steps

1. **Navigate to "Master Data Reference"** tab

2. **Scroll to "Add New Security"** section

3. **Use ISIN Lookup (External API):**
   - Enter ISIN: `US67066G1040`
   - Click **"Lookup"** button
   - The OpenFIGI API will return NVIDIA's details

4. **Review pre-filled form:**
   - **Issuer Name:** NVIDIA Corporation
   - **Ticker:** NVDA
   - **Exchange:** NYSE
   - **Asset Class:** Equity (auto-detected from Market Sector)

5. **Complete additional fields:**
   - **Currency:** USD
   - **Status:** Active
   - **CUSIP:** 67066G104 (required for US securities)

6. **Click "Add Security"**
   - New GSID will be assigned (e.g., GSID_23701)
   - Record is logged to Security Master History

7. **Verify the match:**
   - Return to "Trades Mapped to Master Data" tab
   - Filter for NVDA again
   - All 5 trades should now show **MATCHED** status with the new GSID

---

## Part 3: Update Downstream Systems

### The Challenge: Keeping Enterprise Systems in Sync

In a typical financial institution, the Security Master is the **golden source of truth** that feeds multiple downstream systems:

```
                    ┌─────────────────┐
                    │ Security Master │
                    │   (Golden Record)│
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Risk Systems  │ │   Settlement    │ │    Custodian    │
│                 │ │                 │ │                 │
│ • VaR Calcs     │ │ • Trade Matching│ │ • Asset Servicing│
│ • Exposure Mgmt │ │ • SWIFT Messages│ │ • Corporate Actions│
│ • Stress Testing│ │ • Netting       │ │ • Income Collection│
└─────────────────┘ └─────────────────┘ └─────────────────┘
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Back Office    │ │   Compliance    │ │   Accounting    │
│                 │ │                 │ │                 │
│ • P&L Attribution│ │ • Reg Reporting │ │ • GL Posting   │
│ • Position Mgmt │ │ • MiFID II      │ │ • NAV Calc     │
│ • Reconciliation│ │ • EMIR/SFTR    │ │ • Fund Accounting│
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

**When a security is added or modified, ALL these systems need to be updated!**

### The "Update Systems" Button

The **🔄 Update Systems** button generates a JSON file containing all recent Security Master changes, ready for distribution to downstream systems.

### Steps

1. **Navigate to "Security Master History"** tab

2. **Click "🔄 Update Systems"** button

3. **A JSON file is generated:**
   - Filename: `SEC_MASTER_UPDATES_10-FEB-2026_14-30.json`
   - Location: `@SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT`

### JSON Payload Structure

```json
{
  "export_timestamp": "2026-02-10 14:30:00",
  "export_type": "SECURITY_MASTER_UPDATES",
  "record_count": 6,
  "changes": [
    {
      "HISTORY_ID": 6,
      "GLOBAL_SECURITY_ID": "GSID_13143",
      "ACTION": "UPDATE",
      "ISSUER_BEFORE": "Home Depot Inc",
      "ISSUER_AFTER": "The Home Depot, Inc.",
      "STATUS_BEFORE": "Active",
      "STATUS_AFTER": "Matured",
      "EDIT_REASON": "Bond matured on Feb 1, 2026",
      "CHANGED_BY": "CUSTODIAN_ADMIN",
      "CHANGED_AT": "2026-02-10 11:15:00",
      "LINEAGE_ID": "LIN-GSID_13143-20260210111500",
      "CURRENT_STATUS": "Matured"
    }
  ]
}
```

### How Downstream Systems Use This Data

| System | What They Extract | Business Impact |
|--------|-------------------|-----------------|
| **Risk Systems** | Status changes, new securities | Recalculate VaR, update exposure limits |
| **Settlement** | ISIN, CUSIP, ticker changes | Update SSI instructions, SWIFT templates |
| **Custodian** | Corporate action flags, maturity dates | Asset servicing, income processing |
| **Back Office** | All changes | Position reconciliation, P&L attribution |
| **Compliance** | New ISINs, status changes | Regulatory reporting (MiFID II, EMIR) |
| **Accounting** | Matured bonds, new securities | GL posting, NAV calculations |

### Real-World Integration Patterns

**Pattern 1: Event-Driven (Real-time)**
```
Security Master → Snowpipe → Kafka → Risk Engine
                                   → Settlement System
                                   → Custodian Portal
```

**Pattern 2: Batch Processing (End of Day)**
```
Security Master → Scheduled Task → JSON Export → SFTP
                                              → Back Office Import
                                              → Reconciliation Jobs
```

**Pattern 3: API-Based**
```
Downstream System → REST API → Security Master DB
                            → Returns delta changes
                            → System updates local cache
```

### Demo Narrative

> *"When CUSTODIAN_ADMIN marked the Home Depot bond as 'Matured' this morning, that single change needs to ripple across the entire enterprise:*
>
> - *Risk needs to remove it from active exposure calculations*
> - *Settlement needs to process the final redemption*
> - *The custodian needs to collect the principal payment*
> - *Accounting needs to book the maturity event*
> - *Compliance needs to update regulatory positions*
>
> *The **Update Systems** button generates the JSON payload that triggers all of these downstream processes, ensuring the entire organization stays in sync with a single source of truth."*

### Verify Export

```sql
-- List exported files
LIST @SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT;

-- View JSON content
SELECT $1 FROM @SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT/SEC_MASTER_UPDATES_10-FEB-2026_14-30.json;
```

---

## Part 4: Export Security Master Data

### Export Options Available

1. **Navigate to "Security Master History"** tab

2. **Current Security Master Export:**
   - Click **"📥 Export to CSV"** button
   - Creates `SEC_MASTER_DD-MMM-YYYY_HH-MM.csv` in @EXPORT stage
   - Contains all active securities with identifiers

3. **History/Audit Trail Export:**
   - Switch to "Change History" sub-tab
   - Apply filters as needed:
     - User: SECURITY_ADMIN, CUSTODIAN_ADMIN, or All
     - Ticker, Currency, Exchange, ISIN filters available
   - Click **"📥 Export History CSV"**
   - Creates `SEC_MASTER_HISTORY_DD-MMM-YYYY_HH-MM.csv` in @EXPORT stage

### Export File Locations

All exports are stored in Snowflake stage:
```sql
@SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT
```

### Sample Export Contents

**SEC_MASTER_*.csv columns:**
- GSID, Issuer, Asset Class, Ticker, Exchange
- ISIN, CUSIP, SEDOL, Currency, Status
- Last Updated, Created By, Modified By

---

## Part 5: AI Agent Queries (Cortex Analyst)

### Using Natural Language to Query Trades

Ask the Cortex Agent these questions about the trade data:

#### Query 1: Trade Summary
```
"Show me all NVIDIA trades from today with their total value"
```

**Expected Response:**
- 5 trades totaling $3,947,375
- 4 BUY orders, 1 SELL order
- Average price: $877.90

#### Query 2: Unmatched Analysis
```
"Which equity trades are not matched to the security master?"
```

**Expected Response:**
- Prior to adding NVIDIA: 5 unmatched trades
- After adding NVIDIA: 0 unmatched trades

#### Query 3: User Activity
```
"What changes did CUSTODIAN_ADMIN make to the security master?"
```

**Expected Response:**
- 3 bond updates on 2026-02-10
- Changed statuses: Pre-Issue→Active, Defaulted→Active, Active→Matured
- Issuers updated: Coca-Cola, GE Aerospace, Home Depot

#### Query 4: Lineage Tracking
```
"Show me the complete history of changes for the Coca-Cola bond"
```

**Expected Response:**
- Original creation by SYSTEM_MIGRATION
- Update by CUSTODIAN_ADMIN on 2026-02-10 09:00
- Reason: "Bond now trading - updated status to Active, added CUSIP per custodian records"

---

### Trade Analysis Queries

#### Query 5: Today's Trading Volume
```
"What is the total trading volume for today?"
```

**Expected Response:**
- Total trades: X trades
- Total value: $X million
- Buy vs Sell breakdown

#### Query 6: Top Traded Securities
```
"Which securities had the most trades today?"
```

**Expected Response:**
- Ranked list of securities by trade count
- Volume and value per security

#### Query 7: Large Trades
```
"Show me all trades over $500,000 today"
```

**Expected Response:**
- List of high-value trades with details
- Security, quantity, price, total value

#### Query 8: Buy vs Sell Analysis
```
"What is the buy to sell ratio for equity trades today?"
```

**Expected Response:**
- Number of buy orders vs sell orders
- Total buy value vs sell value
- Net position direction

#### Query 9: Trading by Exchange
```
"Break down today's trades by exchange"
```

**Expected Response:**
- NYSE, NASDAQ, LSE breakdown
- Trade count and value per exchange

#### Query 10: Price Analysis
```
"What was the average trade price for Apple stock today?"
```

**Expected Response:**
- Average price, min price, max price
- Price range and volatility indicator

#### Query 11: Counterparty Analysis
```
"Which counterparties have we traded with most today?"
```

**Expected Response:**
- Top counterparties by trade count
- Volume per counterparty

#### Query 12: Settlement Risk
```
"Which trades are settling in the next 2 days?"
```

**Expected Response:**
- Trades with T+1 and T+2 settlement
- Total settlement exposure by date

#### Query 13: Currency Exposure
```
"What is our currency exposure from today's trades?"
```

**Expected Response:**
- Breakdown by currency (USD, EUR, GBP, etc.)
- Total exposure per currency

#### Query 14: Sector Analysis
```
"Show me trades by sector for today"
```

**Expected Response:**
- Technology, Healthcare, Financials breakdown
- Trade count and value per sector

#### Query 15: Failed Trade Check
```
"Are there any trades that failed to match today?"
```

**Expected Response:**
- Count of unmatched trades
- Total value at risk
- Symbols requiring attention

---

## Part 6: Audit Trail & Lineage Demo

### View Change History

1. **Navigate to "Security Master History" → "Change History"** tab

2. **Filter by User:**
   - Select `SECURITY_ADMIN` to see equity updates
   - Select `CUSTODIAN_ADMIN` to see bond updates

3. **Recent Changes by SECURITY_ADMIN (3 equities):**

| GSID | Action | Change | Reason |
|------|--------|--------|--------|
| GSID_1110 | UPDATE | Added ISIN | Updated issuer name to official corporate name |
| GSID_1528 | UPDATE | Added ISIN | Standardized issuer name format |
| GSID_1447 | UPDATE | Status→Pre-Issue | Pending fund restructuring |

4. **Recent Changes by CUSTODIAN_ADMIN (3 bonds):**

| GSID | Action | Change | Reason |
|------|--------|--------|--------|
| GSID_14101 | UPDATE | Pre-Issue→Active | Coca-Cola bond now trading |
| GSID_13993 | UPDATE | Defaulted→Active | GE restructuring complete |
| GSID_13143 | UPDATE | Active→Matured | Home Depot bond matured Feb 1 |

### View Lineage

1. **In the "Lineage Viewer" section:**
   - Select a GSID from the dropdown
   - View the complete chain of changes
   - Each entry shows:
     - Action (INSERT/UPDATE)
     - Timestamp
     - User who made the change
     - Reason for change
     - Before/After values

2. **Example Lineage Path:**
```
LIN-GSID_13993-20260209110034 → LIN-GSID_13993-20260210103000
       ↓                                    ↓
   Original Creation              CUSTODIAN_ADMIN Update
   (SYSTEM_MIGRATION)            (Status: Defaulted → Active)
```

---

## Key Value Propositions

1. **Data Quality** - Business rules ensure ISIN uniqueness, CUSIP for US securities, SEDOL for LSEG
2. **Audit Compliance** - Full before/after tracking with user attribution and edit reasons
3. **Lineage Tracking** - Complete chain of custody for every security change
4. **External Integration** - OpenFIGI API lookup for automated data enrichment
5. **AI-Powered Analytics** - Natural language queries via Cortex Analyst
6. **Real-time Matching** - Instantly identify unmatched trades and their exposure
7. **Enterprise Distribution** - JSON exports for downstream system synchronization (Risk, Settlement, Custodian, Back Office)

---

## Demo Checklist

- [ ] Show 5 unmatched NVIDIA trades in Trades Mapped tab
- [ ] Navigate to Master Data Reference
- [ ] Use ISIN Lookup for NVIDIA (US67066G1040)
- [ ] Add NVIDIA to Security Master
- [ ] Verify trades are now matched
- [ ] **Click "Update Systems" button - explain downstream impact**
- [ ] Export Security Master to CSV
- [ ] Show Change History filtered by SECURITY_ADMIN
- [ ] Show Change History filtered by CUSTODIAN_ADMIN
- [ ] View lineage for a specific security
- [ ] Ask Cortex Agent about trade data
- [ ] Export History to CSV
- [ ] Show exported files in @EXPORT stage

---

## SQL Queries for Demo

### Check Unmatched Trades
```sql
SELECT t.SYMBOL, COUNT(*) as TRADE_COUNT, SUM(t.TOTAL_VALUE) as TOTAL_VALUE,
       CASE WHEN g.GLOBAL_SECURITY_ID IS NOT NULL THEN 'Matched' ELSE 'UNMATCHED' END as STATUS
FROM SECURITY_TRADES_DB.TRADES.EQUITY_TRADES t
LEFT JOIN SECURITIES_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE g 
    ON t.SYMBOL = g.PRIMARY_TICKER AND g.ASSET_CLASS = 'Equity'
GROUP BY t.SYMBOL, CASE WHEN g.GLOBAL_SECURITY_ID IS NOT NULL THEN 'Matched' ELSE 'UNMATCHED' END
HAVING STATUS = 'UNMATCHED'
ORDER BY TOTAL_VALUE DESC;
```

### View User Activity
```sql
SELECT CHANGED_BY, COUNT(*) as CHANGES, 
       MIN(CHANGED_AT) as FIRST_CHANGE, MAX(CHANGED_AT) as LAST_CHANGE
FROM SECURITIES_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
GROUP BY CHANGED_BY
ORDER BY CHANGES DESC;
```

### View Lineage for a Security
```sql
SELECT HISTORY_ID, ACTION, CHANGED_AT, CHANGED_BY, 
       STATUS_BEFORE, STATUS_AFTER, EDIT_REASON,
       LINEAGE_ID, LINEAGE_PATH
FROM SECURITIES_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
WHERE GLOBAL_SECURITY_ID = 'GSID_13993'
ORDER BY CHANGED_AT;
```

### List Exported Files
```sql
LIST @SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT;
```

### View JSON Export Content
```sql
SELECT $1 FROM @SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT 
WHERE METADATA$FILENAME LIKE '%SEC_MASTER_UPDATES%'
LIMIT 1;
```

---

*Demo Script Generated: February 10, 2026*
*Application: Security Master EDM - Powered by Snowflake*
