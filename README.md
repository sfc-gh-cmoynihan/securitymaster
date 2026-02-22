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

## 📜 License

This project is proprietary software for demonstration purposes.

---

<div align="center">

**Built with ❄️ Snowflake and 🎈 Streamlit**

*Data Source: Snowflake Marketplace*

</div>
