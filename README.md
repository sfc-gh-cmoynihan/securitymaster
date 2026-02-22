<div align="center">

# ❄️ Security Master EDM

**Enterprise Data Management for Securities**

[![Snowflake](https://img.shields.io/badge/Powered%20by-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com)
[![Streamlit](https://img.shields.io/badge/Built%20with-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FIX Protocol](https://img.shields.io/badge/FIX-5.0%20SP2-00599C?style=for-the-badge)](https://www.fixtrading.org)

---

**Author:** Colm Moynihan | **Version:** 1.2 | **Updated:** February 2026

</div>

> ⚠️ **Disclaimer:** This is a custom demo for Financial Services clients. The code is provided under an open source license with no guarantee of maintenance, security updates, or support.

---

## 🎯 Overview

Security Master EDM serves as the **golden source of truth** for all security reference data within a financial institution.

<table>
<tr>
<td width="50%">

### ✨ Key Features

- 📊 **Portfolio Analytics** - Real-time P&L tracking
- 📝 **Order Entry** - E*Trade-style trading form
- 🔄 **Trade Matching** - Automated GSID assignment
- 📋 **FIXML Messaging** - FIX 5.0 SP2 compliant
- 🔍 **ISIN Lookup** - OpenFIGI API integration
- 💹 **Live Pricing** - Yahoo Finance integration
- 📜 **Full Audit Trail** - Before/after tracking

</td>
<td width="50%">

### 🏗️ Architecture

```
┌─────────────────────────┐
│   Security Master EDM   │
│     (Golden Record)     │
└───────────┬─────────────┘
            │
    ┌───────┼───────┐
    ▼       ▼       ▼
┌───────┐┌───────┐┌───────┐
│ Risk  ││Settle-││Custo- │
│Systems││ ment  ││ dian  │
└───────┘└───────┘└───────┘
```

</td>
</tr>
</table>

---

## 📁 Project Structure

```
securities_master/
├── 📄 README.md              # This file
├── 📄 install.sql            # Complete installation script
├── 📂 streamlit/             # Streamlit application
│   └── streamlit_app.py      # Main app (9 tabs)
├── 📂 sql/                   # SQL scripts
│   ├── setup.sql             # Master setup script
│   ├── setup_security_master.sql
│   ├── setup_golden_record_tables.sql
│   ├── create_trades.sql
│   └── deploy_streamlit.sql
├── 📂 python/                # Python utilities
│   ├── create_nyse_table.py
│   ├── create_corporate_bonds_table.py
│   └── generate_uk_customer_data.py
└── 📂 data/                  # Reference data
    ├── SECURITIES/           # S&P 500 data
    ├── EQUITY/               # NYSE securities
    ├── FIXED_INCOME/         # Bond data
    ├── GOLDEN_RECORD/        # Master records
    └── TRADES/               # Trade data
```

---

## 🖥️ Application Tabs

| Tab | Name | Description |
|:---:|------|-------------|
| 📊 | **Portfolio** | AUM summary, Top gainers/losers, P&L analysis |
| 🔍 | **Trade History** | Real-time trade viewing with filters |
| 📈 | **Sector Analysis** | GICS sector breakdown, Buy/Sell analysis |
| 🔗 | **Equity Trades** | Detailed equity trade view |
| 📉 | **Bond Trades** | Fixed income trades with yield analysis |
| ✏️ | **Master Data** | Add/edit securities, ISIN lookup |
| 📜 | **Master History** | Full audit trail, CSV/JSON export |
| 📋 | **Trades Mapped** | Golden Record trade explorer |
| 📝 | **Stock/ETF Order** | E*Trade-style order entry |

---

## 📝 Stock/ETF Order Entry

<table>
<tr>
<td width="60%">

### Order Features

| Feature | Description |
|---------|-------------|
| **Live Pricing** | Yahoo Finance API (30s cache) |
| **Order Types** | Market, Limit, Stop, Stop Limit |
| **Duration** | Day, GTC, FOK, IOC, Open, Close |
| **Preview** | Grey box with order details |
| **Execution** | Auto-insert to EQUITY_TRADES |
| **FIXML** | FIX 5.0 SP2 execution reports |

</td>
<td width="40%">

### FIXML Output

```xml
<FIXML xmlns="...FIXML-5-0-SP2">
  <ExecRpt ExecID="TRD-001"
           Side="1" OrdStat="2">
    <Instrmt Sym="AAPL"/>
    <OrdQty Qty="100"/>
    <Px Px="185.50"/>
  </ExecRpt>
</FIXML>
```

**Filename Format:**
`FIX_DD-MMM-YYYY_HH:MM:SS_SYMBOL_BUY.xml`

</td>
</tr>
</table>

---

## 🗄️ Database Schema

### Tables

| Schema | Table | Description |
|--------|-------|-------------|
| `SECURITIES` | `SP500` | S&P 500 reference (503 companies) |
| `EQUITY` | `NYSE_SECURITIES` | NYSE listed securities |
| `FIXED_INCOME` | `CORPORATE_BONDS` | Corporate bond data |
| `GOLDEN_RECORD` | `SECURITY_MASTER_REFERENCE` | Golden source of truth |
| `GOLDEN_RECORD` | `SECURITY_MASTER_HISTORY` | Full audit trail |
| `TRADES` | `EQUITY_TRADES` | Equity executions |
| `TRADES` | `BOND_TRADES` | Bond executions |
| `TRADES` | `FIX_STAGE` | FIXML message files |

### Functions

| Function | Description |
|----------|-------------|
| `GET_STOCK_PRICE(symbol)` | Live price from Yahoo Finance |
| `LOOKUP_ISIN_EXTERNAL(isin)` | OpenFIGI ISIN lookup |

---

## 🚀 Quick Start

### 1️⃣ Clone Repository
```bash
git clone https://github.com/your-repo/securities_master.git
cd securities_master
```

### 2️⃣ Run Installation Script
```sql
-- Execute install.sql in Snowsight or SnowSQL
-- This creates all databases, tables, functions, and the Streamlit app
```

### 3️⃣ Deploy Streamlit App
```bash
snow stage copy streamlit/streamlit_app.py @SECURITY_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE --overwrite
```

### 4️⃣ Access Application
```
https://app.snowflake.com/<account>/#/streamlit-apps/SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP
```

---

## 🔌 External Integrations

<table>
<tr>
<td align="center" width="50%">

### 🌐 OpenFIGI API

**Purpose:** ISIN to security details mapping

```
Endpoint: api.openfigi.com/v3/mapping
```

Returns: Name, Ticker, Exchange, FIGI

</td>
<td align="center" width="50%">

### 📈 Yahoo Finance API

**Purpose:** Live stock prices

```
Endpoint: query1.finance.yahoo.com
```

Returns: Price, Previous Close, Market State

</td>
</tr>
</table>

---

## 🔐 Security Roles

| Role | Purpose | Permissions |
|------|---------|-------------|
| `SECURITY_ADMIN` | Equity data management | Add/edit equity securities |
| `CUSTODIAN_ADMIN` | Bond data management | Add/edit bond securities |
| `ACCOUNTADMIN` | Full administration | All operations |

---

## 💡 Key Value Propositions

| Feature | Benefit |
|---------|---------|
| ✅ **Data Quality** | Business rules ensure identifier uniqueness |
| ✅ **Audit Compliance** | Full before/after tracking for regulations |
| ✅ **Lineage Tracking** | Complete chain of custody |
| ✅ **External APIs** | Automated data enrichment |
| ✅ **Real-time Matching** | Instant unmatched trade identification |
| ✅ **FIXML Standard** | Industry-standard trade messaging |

---

## 🎬 Demo Script

### Demo Overview

This demo showcases the Security Master EDM application including trade matching, data quality management, audit trails, and AI-powered analytics.

---

### Part 1: Identify Unmatched Trades (NVIDIA)

**Scenario:** Five NVIDIA (NVDA) equity trades totaling **$3,947,375** cannot be matched because NVIDIA is missing from the Security Master.

| Trade ID | Symbol | Type | Quantity | Price | Total Value | Status |
|----------|--------|------|----------|-------|-------------|--------|
| 2556 | NVDA | BUY | 1,000 | $875.50 | $875,500 | ⚠️ UNMATCHED |
| 2557 | NVDA | BUY | 500 | $878.25 | $439,125 | ⚠️ UNMATCHED |
| 2558 | NVDA | SELL | 250 | $880.00 | $220,000 | ⚠️ UNMATCHED |
| 2559 | NVDA | BUY | 2,000 | $876.75 | $1,753,500 | ⚠️ UNMATCHED |
| 2560 | NVDA | BUY | 750 | $879.00 | $659,250 | ⚠️ UNMATCHED |

**Steps:**
1. Open **"Trades Mapped to Master Data"** tab
2. Filter for `NVDA` - observe 5 unmatched trades
3. Note: Unmatched trades = operational and regulatory risk

---

### Part 2: Add NVIDIA to Security Master

**Steps:**
1. Navigate to **"Master Data Reference"** tab
2. Enter ISIN: `US67066G1040`
3. Click **"Lookup"** - OpenFIGI API returns NVIDIA details
4. Complete form: Currency=USD, Status=Active, CUSIP=67066G104
5. Click **"Add Security"** - new GSID assigned
6. Return to "Trades Mapped" - all trades now show ✅ MATCHED

---

### Part 3: Update Downstream Systems

**The Challenge:** Security Master changes must propagate to all enterprise systems:

```
                    ┌─────────────────┐
                    │ Security Master │
                    │  (Golden Record) │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Risk Systems  │ │   Settlement    │ │    Custodian    │
│ • VaR Calcs     │ │ • SWIFT Messages│ │ • Asset Servicing│
│ • Exposure Mgmt │ │ • Netting       │ │ • Corp Actions  │
└─────────────────┘ └─────────────────┘ └─────────────────┘
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Back Office    │ │   Compliance    │ │   Accounting    │
│ • P&L           │ │ • MiFID II      │ │ • GL Posting    │
│ • Reconciliation│ │ • EMIR/SFTR     │ │ • NAV Calc      │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

**Steps:**
1. Navigate to **"Security Master History"** tab
2. Click **"🔄 Update Systems"** button
3. JSON file generated with all changes for downstream distribution

---

### Part 4: Export Security Master Data

| Export Type | Button | Output |
|-------------|--------|--------|
| Current Master | 📥 Export to CSV | `SEC_MASTER_DD-MMM-YYYY_HH-MM.csv` |
| Change History | 📥 Export History CSV | `SEC_MASTER_HISTORY_DD-MMM-YYYY_HH-MM.csv` |
| System Updates | 🔄 Update Systems | `SEC_MASTER_UPDATES_DD-MMM-YYYY_HH-MM.json` |

---

### Part 5: AI Agent Queries (Cortex Analyst)

Ask natural language questions:

| Query | Expected Response |
|-------|-------------------|
| "Show me all NVIDIA trades with total value" | 5 trades, $3.9M total |
| "Which trades are not matched?" | Count of unmatched trades |
| "What changes did CUSTODIAN_ADMIN make?" | Bond status updates |
| "Show trades over $500,000" | High-value trade list |
| "What is today's buy/sell ratio?" | Order flow analysis |
| "Break down trades by sector" | GICS sector summary |

---

### Part 6: Audit Trail & Lineage

**View Change History:**

| User | Action | Example Change |
|------|--------|----------------|
| SECURITY_ADMIN | UPDATE | Added ISIN to equity record |
| CUSTODIAN_ADMIN | UPDATE | Bond status: Pre-Issue → Active |
| CUSTODIAN_ADMIN | UPDATE | Bond status: Active → Matured |

**Lineage Tracking:**
```
LIN-GSID_13993-20260209 → LIN-GSID_13993-20260210
        ↓                           ↓
   Original Creation       CUSTODIAN_ADMIN Update
   (SYSTEM_MIGRATION)     (Status: Defaulted → Active)
```

---

### Demo Checklist

- [ ] Show 5 unmatched NVIDIA trades in Trades Mapped tab
- [ ] Use ISIN Lookup for NVIDIA (US67066G1040)
- [ ] Add NVIDIA to Security Master
- [ ] Verify trades are now matched
- [ ] Click **"Update Systems"** - explain downstream impact
- [ ] Export Security Master to CSV
- [ ] Show Change History by user
- [ ] View lineage for a specific security
- [ ] Ask Cortex Agent questions about trade data
- [ ] Place a test order using Stock/ETF Order tab
- [ ] Verify FIXML file in FIX_STAGE

---

### SQL Queries for Demo

```sql
-- Check Unmatched Trades
SELECT t.SYMBOL, COUNT(*) as TRADE_COUNT, SUM(t.TOTAL_VALUE) as TOTAL_VALUE
FROM SECURITY_MASTER_DB.TRADES.EQUITY_TRADES t
LEFT JOIN SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_REFERENCE g 
    ON t.SYMBOL = g.PRIMARY_TICKER
WHERE g.GLOBAL_SECURITY_ID IS NULL
GROUP BY t.SYMBOL;

-- View User Activity
SELECT CHANGED_BY, COUNT(*) as CHANGES
FROM SECURITY_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_HISTORY
GROUP BY CHANGED_BY;

-- List Exported Files
LIST @SECURITY_MASTER_DB.GOLDEN_RECORD.EXPORT;

-- List FIXML Files
LIST @SECURITY_MASTER_DB.TRADES.FIX_STAGE;
```

---

## 📜 License

This project is proprietary software for demonstration purposes.

---

<div align="center">

**Built with ❄️ Snowflake and 🎈 Streamlit**

*Data Source: Snowflake Marketplace*

</div>
