# Security Master EDM

**Author:** Colm Moynihan  
**Date:** 20th Feb 2026  
**Version:** 1.0

> **Disclaimer:** This is a custom demo of Security Master built for use by Financial Services clients. The code here is not supported and is provided under an open source license. It is released with this source code publicly available, but with no guarantee of maintenance, security updates, bug fixes, or customer support from the original developer.

---

Enterprise Data Management application for Securities Master built on Snowflake with Streamlit. This application provides a comprehensive solution for managing security reference data, trade matching, audit trails, and downstream system integration.

![Powered by Snowflake](https://img.shields.io/badge/Powered%20by-Snowflake-29B5E8?style=flat&logo=snowflake)
![Streamlit](https://img.shields.io/badge/Built%20with-Streamlit-FF4B4B?style=flat&logo=streamlit)

## Overview

The Security Master EDM serves as the **golden source of truth** for all security reference data within a financial institution. It enables:

- **Trade Matching** - Match equity and bond trades to master security records
- **Data Quality Management** - Business rules for ISIN uniqueness, CUSIP for US securities, SEDOL for LSEG
- **Full Audit Trail** - Before/after tracking with user attribution and edit reasons
- **Lineage Tracking** - Complete chain of custody for every security change
- **External API Integration** - OpenFIGI API lookup for automated data enrichment
- **AI-Powered Analytics** - Natural language queries via Cortex Analyst
- **Downstream Distribution** - JSON/CSV exports for Risk, Settlement, Custodian, and Back Office systems

## Architecture

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
│                 │ │                 │ │                 │
│ • VaR Calcs     │ │ • Trade Matching│ │ • Asset Servicing│
│ • Exposure Mgmt │ │ • SWIFT Messages│ │ • Corporate Actions│
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

## Features

### 1. Portfolio Overview
- Total AUM, Cash Balance, Treasury Holdings
- Top Gainers and Losers with P&L analysis
- Equity and Bond portfolio summaries

### 2. Trade History
- Real-time trade viewing with filters
- Trade details including quantity, price, and total value

### 3. Sector Analysis
- GICS sector breakdown of trading activity
- Buy/Sell analysis by sector

### 4. Equity & Bond Trades
- Separate views for equity and fixed income trades
- Counterparty analysis for bond trades
- Yield analysis for bond portfolios

### 5. Master Data Reference
- Add, edit, and manage security master records
- ISIN lookup via OpenFIGI API
- Support for multiple identifiers (ISIN, CUSIP, SEDOL, FIGI)

### 6. Security Master History
- Complete audit trail of all changes
- Export to CSV and JSON formats
- "Update Systems" button for downstream distribution

### 7. Trade Matching
- Real-time matching of trades to security master
- Identification of unmatched trades with exposure values
- Global Security ID (GSID) assignment

## Database Schema

### Databases

| Database | Purpose |
|----------|---------|
| `SECURITIES_MASTER_DB` | Master security reference data and history |
| `SECURITY_TRADES_DB` | Trade execution data for equities and bonds |

### Key Tables

| Schema | Table | Description |
|--------|-------|-------------|
| `SECURITIES` | `SP500` | S&P 500 company reference data |
| `EQUITY` | `NYSE_SECURITIES` | NYSE listed securities with ISINs |
| `FIXED_INCOME` | `CORPORATE_BONDS` | Corporate bond reference data |
| `GOLDEN_RECORD` | `SECURITY_MASTER_REFERENCE` | Golden record for all securities |
| `GOLDEN_RECORD` | `SECURITY_MASTER_HISTORY` | Full audit trail with lineage |
| `TRADES` | `EQUITY_TRADES` | Equity trade executions |
| `TRADES` | `BOND_TRADES` | Bond trade executions |

## Installation

### Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Python 3.8+ (for data generation scripts)
- Snowflake CLI (`snow`) or SnowSQL

### Step 1: Create Database Objects

```sql
-- Run setup script
!source setup_security_master.sql
```

### Step 2: Load Reference Data

```bash
# Load S&P 500 companies
# (Already included in setup_security_master.sql)

# Load NYSE securities with ISINs
python create_nyse_table.py

# Load corporate bonds
python create_corporate_bonds_table.py
```

### Step 3: Create Sample Trades

```sql
!source create_trades.sql
```

### Step 4: Deploy Streamlit App

```sql
-- Create stage and upload app
CREATE OR REPLACE STAGE SECURITIES_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Upload the app file
PUT file:///path/to/streamlit_app.py @SECURITIES_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE 
    AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT SECURITIES_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP
    ROOT_LOCATION = '@SECURITIES_MASTER_DB.GOLDEN_RECORD.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH'
    TITLE = 'Security Master EDM';
```

## File Structure

```
securities_master/
├── streamlit_app.py              # Main Streamlit application
├── setup_security_master.sql     # Database and table creation
├── create_trades.sql             # Sample trade data
├── deploy_streamlit.sql          # Streamlit deployment script
├── create_nyse_table.py          # NYSE securities data loader
├── create_corporate_bonds_table.py # Corporate bonds data loader
├── DEMO_SCRIPT.md                # Demo walkthrough guide
├── README.md                     # This file
└── json_data/                    # Sample JSON data files
```

## Usage

### Accessing the App

Navigate to the Streamlit app URL in Snowsight:
```
https://app.snowflake.com/<account>/#/streamlit-apps/SECURITIES_MASTER_DB.GOLDEN_RECORD.SECURITY_MASTER_APP
```

### Adding a New Security

1. Go to **Master Data Reference** tab
2. Enter ISIN in the lookup field (e.g., `US67066G1040` for NVIDIA)
3. Click **Lookup** to fetch data from OpenFIGI API
4. Review and complete the form
5. Click **Add Security**

### Exporting Data

**CSV Export:**
- Navigate to **Security Master History** tab
- Click **📥 Export to CSV**
- File is saved to `@SECURITIES_MASTER_DB.GOLDEN_RECORD.EXPORT`

**JSON Export for Downstream Systems:**
- Click **🔄 Update Systems** button
- Generates JSON with all recent changes
- Ready for Risk, Settlement, Custodian, and Back Office systems

### AI Agent Queries

Ask natural language questions in the Cortex Agent:

- "Show me all NVIDIA trades from today"
- "Which equity trades are not matched to the security master?"
- "What is the total trading volume for today?"
- "Show me all trades over $500,000"
- "What changes did CUSTODIAN_ADMIN make?"

## Demo Scenario

A complete demo script is available in [DEMO_SCRIPT.md](DEMO_SCRIPT.md) that covers:

1. **Identify Unmatched Trades** - Find NVIDIA trades missing from security master
2. **Add Missing Security** - Use ISIN lookup to add NVIDIA
3. **Update Downstream Systems** - Generate JSON for enterprise distribution
4. **Export Data** - Create CSV exports for compliance
5. **AI Queries** - Use natural language to query trade data
6. **Audit Trail** - View complete change history with lineage

## Security Roles

| Role | Purpose | Permissions |
|------|---------|-------------|
| `SECURITY_ADMIN` | Equity data management | Add/edit equity securities |
| `CUSTODIAN_ADMIN` | Bond data management | Add/edit bond securities |
| `ACCOUNTADMIN` | Full administration | All operations |

## Key Value Propositions

1. **Data Quality** - Business rules ensure identifier uniqueness and regional compliance
2. **Audit Compliance** - Full before/after tracking meets regulatory requirements
3. **Lineage Tracking** - Complete chain of custody for every change
4. **External Integration** - OpenFIGI API for automated data enrichment
5. **AI-Powered Analytics** - Natural language queries via Cortex Analyst
6. **Real-time Matching** - Instantly identify unmatched trades and exposure
7. **Enterprise Distribution** - Standardized exports for downstream systems

## Technical Details

### Snowflake Features Used

- **Streamlit in Snowflake** - Native app deployment
- **Snowpark** - Python data processing
- **Cortex Analyst** - Natural language to SQL
- **Stages** - File storage for exports
- **Time Travel** - Historical data access
- **Access Control** - Role-based security

### External APIs

- **OpenFIGI API** - Security identifier lookup
  - Endpoint: `https://api.openfigi.com/v3/mapping`
  - Used for ISIN to security details mapping

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is proprietary software for demonstration purposes.

## Support

For questions or issues, contact the development team or open an issue in this repository.

---

*Built with Snowflake and Streamlit*
